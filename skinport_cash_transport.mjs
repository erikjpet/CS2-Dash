// Public bulk read only. Private held names stay in this process, not the URL.
import https from 'node:https';
import crypto from 'node:crypto';
import zlib from 'node:zlib';
import { FullCatalogBuilder, encodeSelectedWithCatalog } from './skinport_cash_full_catalog.mjs';

const URL = 'https://api.skinport.com/v1/items?app_id=730&currency=USD&tradable=0';
const INPUT_CAP = 1024 * 1024, OUTPUT_CAP = 8 * 1024 * 1024;
const ENCODED_CAP = 8 * 1024 * 1024, DECODED_CAP = 32 * 1024 * 1024;
const receivedAtUs = () => Date.now() * 1000;
function emit(value) {
  const text = JSON.stringify(value);
  if (Buffer.byteLength(text) > OUTPUT_CAP) throw new Error('selected response exceeded cap');
  process.stdout.write(text);
}
function safeHeaders(headers) {
  const bounded = value => typeof value === 'string' ? value.slice(0, 256) : null;
  return { retryAfter: bounded(headers['retry-after']), date: bounded(headers.date),
    age: bounded(headers.age), etag: bounded(headers.etag),
    lastModified: bounded(headers['last-modified']),
    contentType: bounded(headers['content-type']), contentEncoding: bounded(headers['content-encoding']) ?? 'identity' };
}
async function inputNames() {
  const chunks = []; let size = 0;
  for await (const chunk of process.stdin) {
    size += chunk.length;
    if (size > INPUT_CAP) throw new Error('local input exceeded cap');
    chunks.push(chunk);
  }
  const names = JSON.parse(Buffer.concat(chunks).toString('utf8'));
  if (!Array.isArray(names) || names.length < 1 || names.length > 10000 ||
      names.some(name => typeof name !== 'string' || !name || name.length > 512) ||
      new Set(names).size !== names.length) throw new Error('invalid exact-name set');
  return names;
}
async function request() {
  return new Promise((resolve, reject) => {
    let req;
    const timer = setTimeout(() => { req?.destroy(new Error('network deadline')); }, 20000);
    req = https.get(URL, { headers: { Accept: 'application/json', 'Accept-Encoding': 'br',
      'User-Agent': 'CS2-Dash/CashPrices' } }, response => {
      const headers = safeHeaders(response.headers), status = response.statusCode;
      response.on('error', reject);
      if (status !== 200) {
        response.destroy(); clearTimeout(timer);
        resolve({ status, headers, receivedAtUs: receivedAtUs() }); return;
      }
      if (!String(headers.contentType).toLowerCase().includes('application/json') ||
          !['br','identity'].includes(headers.contentEncoding)) {
        req.destroy(new Error('unexpected representation')); return;
      }
      const chunks = []; let size = 0;
      response.on('data', chunk => {
        size += chunk.length;
        if (size > ENCODED_CAP) req.destroy(new Error('encoded response exceeded cap'));
        else chunks.push(chunk);
      });
      response.on('end', () => {
        clearTimeout(timer);
        try {
          const encoded = Buffer.concat(chunks);
          const decoded = headers.contentEncoding === 'br'
            ? zlib.brotliDecompressSync(encoded, { maxOutputLength: DECODED_CAP }) : encoded;
          if (decoded.length > DECODED_CAP) throw new Error('decoded response exceeded cap');
          resolve({ status, headers, receivedAtUs: receivedAtUs(), encodedBytes: encoded.length,
            decodedBytes: decoded.length, decoded });
        } catch (error) { reject(error); }
      });
    });
    req.on('error', error => { clearTimeout(timer); reject(error); });
  });
}
try {
  const names = await inputNames();
  const response = await request();
  if (response.status !== 200) emit(response);
  else {
    const feed = JSON.parse(response.decoded.toString('utf8'));
    if (!Array.isArray(feed) || feed.length > 150000) throw new Error('unexpected feed shape');
    const wanted = new Set(names), allNames = new Set();
    const matchCounts = Object.fromEntries(names.map(name => [name, 0]));
    const rows = []; let currencyMismatches = 0, malformed = 0;
    const fullCatalog = new FullCatalogBuilder(feed.length);
    for (const row of feed) {
      fullCatalog.add(row);
      if (!row || typeof row !== 'object' || typeof row.market_hash_name !== 'string' ||
          !row.market_hash_name || row.market_hash_name.length > 512) { malformed++; continue; }
      allNames.add(row.market_hash_name);
      if (row.currency !== 'USD') currencyMismatches++;
      if (!wanted.has(row.market_hash_name)) continue;
      matchCounts[row.market_hash_name]++;
      rows.push(Object.fromEntries(['market_hash_name','currency','min_price','quantity',
        'updated_at','created_at','item_page','market_page','version'].map(key => [key, row[key] ?? null])));
    }
    const selected = { status: response.status, receivedAtUs: response.receivedAtUs,
      bodySHA256: crypto.createHash('sha256').update(response.decoded).digest('hex'),
      headers: response.headers, rowCount: feed.length, distinctNames: allNames.size,
      encodedBytes: response.encodedBytes, decodedBytes: response.decodedBytes,
      rows, matchCounts, currencyMismatches, malformed, requestUrl: URL,
      privateHoldingsSentToProvider: false };
    process.stdout.write(encodeSelectedWithCatalog(selected, fullCatalog.finish()));
  }
} catch (error) {
  emit({ status: null, receivedAtUs: receivedAtUs(), error: String(error.message).slice(0, 256) });
}
