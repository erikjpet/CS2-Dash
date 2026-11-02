"""Pure qualification of same-response Skinport capture scopes.

No I/O occurs on import. The default cash_prices import is deferred until a
qualification call; callers can inject the pinned base module instead. This
module neither admits publication nor reads previous coverage/store state.
"""
import json
import math

FORMAT = 'skinport-items-full-catalog:wire:v1'
COLUMNS = ('market_hash_name', 'currency', 'min_price', 'quantity', 'updated_at', 'version')
VARIANT_COLUMNS = ('version', 'min_price', 'quantity', 'updated_at')
FULL_KEYS = frozenset(('contractVersion', 'format', 'scope', 'complete', 'reason', 'columns',
                       'rowCount', 'seenRows', 'emittedRows', 'absenceSemantics',
                       'providerTimeSemantics', 'rows'))
TIME_SEMANTICS = 'upstream_updated_at_not_claimed_trade_time'
MAX_ROWS = 150000
MAX_REQUESTED = 10000
WIRE_CAP = 8 * 1024 * 1024
ROW_WIRE_CAP = WIRE_CAP - 2048
EVIDENCE_CAP = WIRE_CAP


class CaptureQualificationError(ValueError):
    """The outer selected receipt or its same-response consistency is invalid."""


class _FullRejected(ValueError):
    """The optional catalog cannot qualify; selected scope remains independent."""


def _units(value):
    return len(value.encode('utf-16-le', errors='surrogatepass')) // 2


def _name(value):
    return isinstance(value, str) and 0 < _units(value) <= 512 and '\x00' not in value


def _scalar(value):
    if value is None or type(value) is bool:
        return True
    if isinstance(value, str):
        return _units(value) <= 512
    if type(value) in (int, float):
        try:
            return math.isfinite(value)
        except OverflowError:
            return False
    return False


def _encoded_bytes(value):
    try:
        return len(json.dumps(value, ensure_ascii=False, separators=(',', ':'),
                              allow_nan=False).encode('utf-8'))
    except (ValueError, TypeError, UnicodeError, OverflowError) as error:
        raise CaptureQualificationError('unsupported_wire_encoding') from error


def _wire_row(row):
    if (type(row) is not list or len(row) != len(COLUMNS) or not _name(row[0])
            or row[1] != 'USD' or any(not _scalar(value) for value in row[2:5])
            or not (row[5] is None or isinstance(row[5], str) and _units(row[5]) <= 512)):
        raise _FullRejected('full_catalog_row_schema_invalid')
    return row


def _selected_wire(row):
    wire = [row.get(key) for key in COLUMNS]
    try:
        return _wire_row(wire)
    except _FullRejected as error:
        raise CaptureQualificationError('selected_row_schema_invalid') from error


def _base_contract(cash_contract):
    if cash_contract is None:
        import cash_prices
        cash_contract = cash_prices
    expected = {'PROVIDER': 'skinport_live', 'CURRENCY': 'USD',
                'SOURCE': 'skinport:items:730:USD:tradable0', 'BASIS': 'lowest_listed_price',
                'REQUEST_URL': 'https://api.skinport.com/v1/items?app_id=730&currency=USD&tradable=0'}
    if (any(getattr(cash_contract, key, None) != value for key, value in expected.items())
            or not callable(getattr(cash_contract, '_receipt', None))
            or not callable(getattr(cash_contract, 'normalize_outcome', None))):
        raise CaptureQualificationError('unqualified_base_cash_contract')
    return cash_contract


def _outer(result, requested, base):
    if (type(requested) is not list or len(requested) > MAX_REQUESTED
            or any(not _name(name) for name in requested) or len(set(requested)) != len(requested)):
        raise CaptureQualificationError('bounded_unique_requested_hashes_required')
    if (type(result) is not dict or type(result.get('currencyMismatches')) is not int
            or type(result.get('malformed')) is not int):
        raise CaptureQualificationError('invalid_outer_receipt')
    try:
        receipt, selected = base._receipt(result, requested)
    except (ValueError, TypeError, KeyError, OverflowError) as error:
        raise CaptureQualificationError('invalid_outer_receipt: ' + str(error)) from error
    projected = {}
    for name in requested:
        rows = selected.get(name, [])
        # The existing bridge supplies every selected duplicate. A lower count
        # cannot preserve recoverable variant evidence and is rejected here.
        if len(rows) != result['matchCounts'][name]:
            raise CaptureQualificationError('selected_duplicate_evidence_incomplete')
        projected[name] = [_selected_wire(row) for row in rows]
    return dict(receipt), projected


def _full(result, receipt, selected):
    catalog = result.get('fullCatalog')
    if catalog is None:
        raise _FullRejected('full_catalog_absent')
    if type(catalog) is not dict:
        raise _FullRejected('full_catalog_schema_invalid')
    if catalog.get('complete') is not True:
        # Do not echo an untrusted arbitrary reason or interpret partial rows.
        raise _FullRejected('full_catalog_incomplete')
    if (set(catalog) != FULL_KEYS or type(catalog.get('contractVersion')) is not int
            or catalog['contractVersion'] != 1 or catalog.get('format') != FORMAT
            or catalog.get('scope') != 'all_rows_from_same_items_response'
            or type(catalog.get('columns')) is not list or tuple(catalog['columns']) != COLUMNS
            or catalog.get('reason') is not None
            or catalog.get('absenceSemantics') != 'exact_hash_absence_only_when_complete'
            or catalog.get('providerTimeSemantics') != TIME_SEMANTICS):
        raise _FullRejected('full_catalog_schema_invalid')
    rows = catalog.get('rows')
    if type(rows) is not list or len(rows) > MAX_ROWS:
        raise _FullRejected('full_catalog_rows_invalid')
    counts = [catalog.get(key) for key in ('rowCount', 'seenRows', 'emittedRows')]
    if any(type(count) is not int or count != len(rows) for count in counts):
        raise _FullRejected('full_catalog_count_mismatch')
    if len(rows) != receipt['rowCount']:
        raise CaptureQualificationError('full_catalog_outer_row_count_mismatch')
    groups = {}
    row_bytes = 2
    for index, row in enumerate(rows):
        row = _wire_row(row)
        row_bytes += _encoded_bytes(row) + (1 if index else 0)
        if row_bytes > ROW_WIRE_CAP:
            raise _FullRejected('full_catalog_row_wire_bound')
        groups.setdefault(row[0], []).append(row)
    if len(groups) != receipt['distinctNames']:
        raise CaptureQualificationError('full_catalog_outer_distinct_count_mismatch')
    for name, selected_rows in selected.items():
        full_rows = groups.get(name, [])
        same = len(full_rows) == len(selected_rows) and all(
            all(type(left) is type(right) and left == right for left, right in zip(full_row, selected_row))
            for full_row, selected_row in zip(full_rows, selected_rows))
        if not same:
            raise CaptureQualificationError('selected_full_projection_mismatch')
    return groups


def _outcomes(names, groups, base, scope):
    outcomes, evidence_bytes = [], 0
    for name in names:
        wire_rows = groups.get(name, [])
        # Duplicate exact hashes always remain ambiguous, even equal versions
        # or prices. No phase mapping or lowest duplicate is chosen here.
        rows = [dict(zip(COLUMNS, wire_rows[0]))] if len(wire_rows) == 1 else []
        outcome = dict(base.normalize_outcome(name, len(wire_rows), rows))
        evidence = None
        if wire_rows:
            variants = [[row[5], row[2], row[3], row[4]] for row in wire_rows]
            evidence_bytes += _encoded_bytes(variants)
            if evidence_bytes > EVIDENCE_CAP:
                raise CaptureQualificationError('variant_evidence_wire_bound')
            evidence = {'contractVersion': 1, 'columns': list(VARIANT_COLUMNS),
                        'rowCount': len(variants), 'complete': True, 'rows': variants,
                        'bindingVerified': False}
        outcome.update(providerVersion=wire_rows[0][5] if len(wire_rows) == 1 else None,
                       variantEvidence=evidence, observationScope=scope,
                       providerTimeSemantics=TIME_SEMANTICS)
        outcomes.append(outcome)
    return outcomes


def qualify_capture(result, requested_names, *, cash_contract=None):
    """Return receipt/outcomes/fullCatalog/feedNameCount/reason, without I/O.

    The default base import is lazy; inject cash_contract for a fully explicit
    pure call. Complete transport is not publication admission. The root must
    compare prior full coverage and its attempt/clock fences before writing.
    No result authorizes pruning or advancing freshness of unobserved subjects.
    """
    base = _base_contract(cash_contract)
    receipt, selected = _outer(result, requested_names, base)
    try:
        groups = _full(result, receipt, selected)
        full, reason = True, None
    except _FullRejected as error:
        groups, full, reason = selected, False, str(error)
    names = sorted(set(groups).union(requested_names)) if full else list(requested_names)
    scope = 'complete_catalog_transport' if full else 'selected_only_incomplete_catalog'
    outcomes = _outcomes(names, groups, base, scope)
    empty = full and receipt['rowCount'] == 0
    if empty:
        reason = 'empty_complete_feed_requires_admission'
    receipt.update(captureScope=scope, catalogCompletenessAdmission={
        'required': True, 'admitted': False, 'emptyCompleteFeed': empty,
        'unexpectedSparsityAssessed': False,
        'basis': 'root_previous_full_row_and_distinct_counts_required',
        'globalPublicationAllowed': False, 'globalFreshnessAdvanceAllowed': False,
        'absenceScope': 'requested_hashes_only' if not full else 'complete_feed_union_requested_only'
    }, feedNameCountSemantics='validated_catalog_distinct_hashes' if full else 'outer_reported_distinct_hashes')
    return {'receipt': receipt, 'outcomes': outcomes, 'fullCatalog': full,
            'feedNameCount': len(groups) if full else receipt['distinctNames'], 'reason': reason}
