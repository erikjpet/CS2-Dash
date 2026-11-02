"""Cash acquisition/control over the small authoritative ledger.

The existing action, single-flight, cooldown, uncertainty, cadence and holdings
contracts remain. A successful capture and its job/control revision commit in
one short transaction; analytics never owns this store's writer lock.
"""
import contextlib
import json
import threading
import time

import cash_prices
import compact_cash_store as store
from skinport_cash_capture import qualify_capture


class CompactCash(cash_prices.CashPrices):
    def __init__(self, path, holdings_reader, transport, capacity_check, clock=time.time):
        self.path = path
        super().__init__(lambda *, write=False: store.connection(path, write=write),
                         lambda portfolio_id, ignored=None: holdings_reader(portfolio_id, None),
                         transport, capacity_check, threading.RLock(), clock=clock)
        with contextlib.closing(self.connect()) as conn:
            if conn.execute("SELECT value FROM settings WHERE key='compact_authority_version'").fetchone()[0] != '1':
                raise cash_prices.CashError('Compact authority handoff is not qualified')
            self._control(conn)  # Invalid or absent migrated control fails closed.
            if conn.execute('SELECT value FROM settings WHERE key=?', (cash_prices.CONTROL_KEY,)).fetchone() is None:
                raise cash_prices.CashError('Migrated cash reservation is required')

    def current_rows(self, conn, names):
        return store.legacy_rows(conn, names)

    def _run(self, job_id, frozen):
        try:
            if not frozen['hashes']:
                with self._write() as conn:
                    control = self._owned(conn, job_id)
                    control['job'].update(status='completed', phase='completed', updatedAt=int(self.clock()))
                    self._save_control(conn, control)
                return
            self.capacity_check()
            with self._write() as conn:
                control = self._owned(conn, job_id)
                started = control['job']['startedAt']
                control['job'].update(status='fetching', phase='fetching', updatedAt=int(self.clock()))
                self._save_control(conn, control)
            # Same single provider GET, fixed limits and no transaction held.
            result = self.transport(list(frozen['hashes']))
            if isinstance(result, dict) and result.get('status') == 429:
                deadline, reason = self._retry_deadline(result)
                self._fail(job_id, 'Skinport returned HTTP 429', deadline, reason)
                return
            qualified = qualify_capture(result, frozen['hashes'])
            receipt, outcomes = qualified['receipt'], qualified['outcomes']
            if receipt['receivedAt'] < started - 5 or receipt['receivedAt'] > int(self.clock()) + 60:
                raise ValueError('Transport acquisition clock is outside this request attempt')
            receipt.update(portfolioId=frozen['portfolioId'], holdingsFingerprint=frozen['holdingsFingerprint'])
            if qualified['fullCatalog'] and qualified['feedNameCount'] == 0:
                raise ValueError('Empty full-catalog response is not admitted; prior prices retained')
            # Exact prior successful feed counts provide a conservative floor,
            # not an invented percentage or undocumented provider guarantee.
            # A smaller internally consistent response may be legitimate, but
            # cannot globally freshen/clear unseen subjects. Publish observed
            # subjects and requested absences with their own capture instead.
            with contextlib.closing(self.connect()) as conn:
                prior_full = conn.execute('SELECT c.receipt_json FROM cash_heads h JOIN cash_captures c ON c.id=h.full_capture WHERE h.id=1').fetchone()
                floor = conn.execute("SELECT value FROM settings WHERE key='compact_feed_count_floor'").fetchone()
                previous = json.loads(prior_full[0]) if prior_full else json.loads(floor[0]) if floor else {}
            prior_rows, prior_names = previous.get('rowCount'), previous.get('distinctNames')
            admitted = bool(qualified['fullCatalog'] and type(prior_rows) is int and type(prior_names) is int
                            and prior_rows > 0 and prior_names > 0 and receipt['rowCount'] >= prior_rows
                            and receipt['distinctNames'] >= prior_names)
            receipt['catalogCompletenessAdmission'] = dict(required=True, admitted=admitted,
                globalPublicationAllowed=admitted, globalFreshnessAdvanceAllowed=admitted,
                unexpectedSparsityAssessed=True, priorRows=prior_rows, priorDistinctNames=prior_names,
                basis='complete_nonempty_same_response_and_no_count_decline' if admitted else 'observed_subjects_only_no_global_absence')
            # Validate complete envelope before opening the one publication
            # transaction. An incomplete envelope may freshen selected subjects
            # only. It cannot clear or freshen any unobserved catalog subject.
            selected = set(frozen['hashes'])
            counts = {name: 0 for name in ('pricedRows', 'missingRows', 'noListingsRows', 'ambiguousRows', 'invalidRows')}
            keys = dict(priced='pricedRows', missing='missingRows', no_listings='noListingsRows', ambiguous='ambiguousRows', invalid='invalidRows')
            for outcome in outcomes:
                if outcome['marketHashName'] in selected:
                    counts[keys[outcome['state']]] += 1
            if sum(counts.values()) != len(selected):
                raise ValueError('Every selected holding must have one committed outcome')
            del result
            self.capacity_check()
            before = time.monotonic()
            with self._write() as conn:
                control = self._owned(conn, job_id)
                publication = store.ingest(conn, receipt, outcomes, full_catalog=admitted)
                control['job'].update(status='completed', phase='completed', receivedAt=receipt['receivedAt'],
                                      updatedAt=int(self.clock()), classifiedRows=len(selected),
                                      committedRows=len(selected), **counts)
                control['job']['catalog'] = dict(full=admitted, transportComplete=qualified['fullCatalog'], classifiedRows=len(outcomes),
                                                feedNames=qualified['feedNameCount'], reason=qualified['reason'] or (None if admitted else 'observed_only_no_global_completeness_admission'),
                                                changedRows=publication['changedRows'], historyRows=publication['historyRows'])
                control['nextAllowedAt'] = max(control['nextAllowedAt'], receipt['receivedAt'] + cash_prices.ADMISSION_SECONDS)
                control.update(lastSuccessfulFetch=receipt, lastError=None)
                self._save_control(conn, control)
            self.last_commit_seconds = time.monotonic() - before
        except Exception as error:
            self._fail(job_id, error)

    def value(self, portfolio_id):
        # Legacy explicit cash endpoint now shares canonical semantics and the
        # same revision. No duplicate current store or stale aggregate fallback.
        from canonical_cash import CanonicalCash
        result = CanonicalCash(self.connect, self, clock=self.clock).value(portfolio_id)
        result.update(provider=cash_prices.PROVIDER, valuationLane='cash_lowest_listed', basis=cash_prices.BASIS)
        return result

    def status(self, job_id=None):
        result = super().status(job_id)
        with contextlib.closing(self.connect()) as conn:
            result['storage'] = dict(authority='compact_current_cash_v1', legacyWrites=False,
                                     originalHistoryRetained=True, **store.source_metrics(conn))
        result['lastPublicationSeconds'] = getattr(self, 'last_commit_seconds', None)
        return result
