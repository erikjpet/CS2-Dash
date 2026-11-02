// Pure same-response projection and bounded wire encoding. No I/O on import.
export const OUTPUT_CAP = 8 * 1024 * 1024;
export const MAX_ROWS = 150000;
export const COLUMNS = Object.freeze([
  'market_hash_name', 'currency', 'min_price', 'quantity', 'updated_at', 'version'
]);
const PROPERTY = ',"fullCatalog":';
// Leave room for the bounded catalog metadata; selected rows share OUTPUT_CAP.
const ROW_WIRE_CAP = OUTPUT_CAP - 2048;
const scalar = value => value === null || typeof value === 'boolean' ||
  (typeof value === 'number' && Number.isFinite(value)) ||
  (typeof value === 'string' && value.length <= 512);

export class FullCatalogBuilder {
  constructor(expectedRows) {
    if (!Number.isInteger(expectedRows) || expectedRows < 0 || expectedRows > MAX_ROWS)
      throw new Error('unexpected full-catalog row count');
    this.expectedRows = expectedRows;
    this.seenRows = 0;
    this.rows = [];
    this.wireBytes = 2; // []
    this.reason = null;
  }
  incomplete(reason) {
    if (this.reason === null) this.reason = reason;
    this.rows = []; // An incomplete catalog never carries a usable prefix.
  }
  add(row) {
    this.seenRows++;
    if (this.reason !== null) return;
    if (this.seenRows > this.expectedRows) {
      this.incomplete('row_count_mismatch'); return;
    }
    if (!row || typeof row !== 'object' || Array.isArray(row) ||
        typeof row.market_hash_name !== 'string' || !row.market_hash_name ||
        row.market_hash_name.length > 512) {
      this.incomplete('invalid_row_identity'); return;
    }
    if (row.currency !== 'USD') {
      this.incomplete('currency_mismatch'); return;
    }
    const wire = COLUMNS.map(key => row[key] ?? null);
    // Preserve invalid monetary/quantity scalar values for the strict consumer
    // to classify. Projection completeness does not mean quote eligibility.
    if (wire.slice(2, 5).some(value => !scalar(value)) ||
        !(wire[5] === null || typeof wire[5] === 'string' && wire[5].length <= 512)) {
      this.incomplete('unsupported_field_shape'); return;
    }
    const bytes = Buffer.byteLength(JSON.stringify(wire)) + (this.rows.length ? 1 : 0);
    if (this.wireBytes + bytes > ROW_WIRE_CAP) {
      this.incomplete('catalog_row_wire_bound'); return;
    }
    this.wireBytes += bytes;
    this.rows.push(wire); // Preserve every duplicate and version in feed order.
  }
  finish() {
    if (this.seenRows !== this.expectedRows) this.incomplete('row_count_mismatch');
    return {
      contractVersion: 1, format: 'skinport-items-full-catalog:wire:v1',
      scope: 'all_rows_from_same_items_response', complete: this.reason === null,
      reason: this.reason, columns: COLUMNS,
      rowCount: this.expectedRows, seenRows: this.seenRows,
      emittedRows: this.reason === null ? this.rows.length : 0,
      absenceSemantics: 'exact_hash_absence_only_when_complete',
      providerTimeSemantics: 'upstream_updated_at_not_claimed_trade_time',
      rows: this.reason === null ? this.rows : []
    };
  }
}

export function encodeSelectedWithCatalog(selected, catalog) {
  // Serialize each bounded part once; never construct an oversized combined
  // string just to discover it cannot pass the original stdout pipe bound.
  const base = JSON.stringify(selected), baseBytes = Buffer.byteLength(base);
  if (baseBytes > OUTPUT_CAP) throw new Error('selected response exceeded cap');
  function append(value) {
    const text = JSON.stringify(value);
    if (baseBytes + Buffer.byteLength(PROPERTY) + Buffer.byteLength(text) > OUTPUT_CAP) return null;
    return base.slice(0, -1) + PROPERTY + text + '}';
  }
  let encoded = append(catalog);
  if (encoded !== null) return encoded;
  // Selected holdings retain their original fields even when full scope does
  // not fit. Absence must never be inferred from this empty incomplete block.
  encoded = append({...catalog, complete: false, reason: 'combined_stdout_bound', emittedRows: 0, rows: []});
  if (encoded !== null) return encoded;
  encoded = append({contractVersion: 1, complete: false, reason: 'selected_envelope_consumes_stdout'});
  // The old selected-only contract is still usable. An absent fullCatalog is
  // explicitly incomplete for consumers; it can never authorize catalog absence.
  return encoded === null ? base : encoded;
}
