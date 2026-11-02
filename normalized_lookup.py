"""Private C-only lookup normalization prototype; import performs no I/O."""
from collections import OrderedDict
import hashlib,json

LOOKUP_COLUMNS=('window_days name market_hash_name kind item_type weapon collection rarity latest_price '
 'prior_price change_pct change_dollar latest_at source volatility_pct history_points edge_case_count groups_json '
 'search_text metric_updated_at steam_price steam_lowest_sell_order steam_highest_buy_order steam_spread_pct '
 'steam_stability_pct price_basis confidence_pct provider_prices_json').split()
COMMON_COLUMNS=('name market_hash_name kind item_type weapon collection rarity latest_price latest_at source '
 'search_text steam_price steam_lowest_sell_order steam_highest_buy_order steam_spread_pct steam_stability_pct '
 'price_basis confidence_pct provider_prices_json').split()
WINDOW_COLUMNS=('window_days name prior_price change_pct change_dollar volatility_pct history_points '
 'edge_case_count groups_json metric_updated_at').split()
REAL_COLUMNS=set('latest_price prior_price change_pct change_dollar volatility_pct steam_price '
 'steam_lowest_sell_order steam_highest_buy_order steam_spread_pct steam_stability_pct confidence_pct'.split())
INT_COLUMNS=set('window_days latest_at history_points edge_case_count metric_updated_at'.split())
MAX_CACHE=4096

def declaration(column):
 kind='REAL' if column in REAL_COLUMNS else 'INTEGER' if column in INT_COLUMNS else 'TEXT'
 return column+' '+kind+(' NOT NULL' if column in ('name','window_days') else '')

def schema_sql():
 common=',\n'.join(declaration(c) for c in COMMON_COLUMNS)
 windows=',\n'.join(declaration(c) for c in WINDOW_COLUMNS)
 selection=['w.source_rowid AS rowid']+[(('w.' if c in WINDOW_COLUMNS else 'c.')+c+' AS '+c) for c in LOOKUP_COLUMNS]
 return ('CREATE TABLE lookup_common(common_id INTEGER PRIMARY KEY,fingerprint BLOB NOT NULL UNIQUE,'+common+');\n'
  'CREATE TABLE lookup_windows(source_rowid INTEGER PRIMARY KEY,common_id INTEGER NOT NULL,'+windows+',UNIQUE(window_days,name),FOREIGN KEY(common_id) REFERENCES lookup_common(common_id));\n'
  'CREATE VIEW market_item_lookup AS SELECT '+','.join(selection)+' FROM lookup_windows w JOIN lookup_common c ON c.common_id=w.common_id;\n')

class NormalizedLookup:
 def __init__(self,conn):
  self.conn=conn;self.cache=OrderedDict();self.common_indices=[LOOKUP_COLUMNS.index(c) for c in COMMON_COLUMNS]
  self.window_indices=[LOOKUP_COLUMNS.index(c) for c in WINDOW_COLUMNS]
  self.common_insert='INSERT INTO lookup_common(fingerprint,'+','.join(COMMON_COLUMNS)+') VALUES('+','.join('?' for _ in range(1+len(COMMON_COLUMNS)))+')'
  self.common_select='SELECT common_id,'+','.join(COMMON_COLUMNS)+' FROM lookup_common WHERE fingerprint=?'
  self.window_insert='INSERT INTO lookup_windows(source_rowid,common_id,'+','.join(WINDOW_COLUMNS)+') VALUES('+','.join('?' for _ in range(2+len(WINDOW_COLUMNS)))+')'
  self.reused=0;self.created=0

 def insert_many(self,rows):
  if not self.conn.in_transaction:raise ValueError('Caller-owned atomic private stage unit required')
  for source_rowid,row in rows:
   if len(row)!=len(LOOKUP_COLUMNS):raise ValueError('Exact28-column lookup row required')
   common=tuple(row[i] for i in self.common_indices)
   fingerprint=hashlib.sha256(b'lookup-common-v1\0'+json.dumps(common,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode()).digest()
   cached=self.cache.get(fingerprint)
   if cached is not None:
    identifier,prior=cached;self.cache.move_to_end(fingerprint)
   else:
    prior=self.conn.execute(self.common_select,(fingerprint,)).fetchone()
    if prior is not None:identifier,prior=prior[0],tuple(prior[1:])
    else:
     identifier=self.conn.execute(self.common_insert,(fingerprint,*common)).lastrowid;prior=common;self.created+=1
    self.cache[fingerprint]=(identifier,prior)
    if len(self.cache)>MAX_CACHE:self.cache.popitem(last=False)
   # A digest only locates a candidate; every field must match exactly.
   if len(prior)!=len(common) or any(type(a) is not type(b) or a!=b for a,b in zip(prior,common)):
    raise ValueError('Common tuple hash collision or SQLite type change; do not merge variants')
   self.reused+=int(prior==common)
   self.conn.execute(self.window_insert,(source_rowid,identifier,*(row[i] for i in self.window_indices)))

 def snapshot(self):
  row=self.conn.execute('SELECT COUNT(*),SUM(a.common_id=b.common_id),SUM(a.common_id!=b.common_id) FROM lookup_windows a JOIN lookup_windows b ON b.name=a.name AND b.window_days=30 WHERE a.window_days=7').fetchone()
  return dict(commonRows=self.conn.execute('SELECT COUNT(*) FROM lookup_common').fetchone()[0],windowRows=self.conn.execute('SELECT COUNT(*) FROM lookup_windows').fetchone()[0],compared7To30Names=row[0],shared7To30Tuples=row[1],distinct7To30Variants=row[2],cachedTupleLimit=MAX_CACHE)

def synthetic_preflight(sqlite3):
 """Small in-memory contract check before any source file can open."""
 conn=sqlite3.connect(':memory:',isolation_level=None)
 try:
  conn.executescript(schema_sql());writer=NormalizedLookup(conn)
  def row(name,window,price,provenance):
   values={c:None for c in LOOKUP_COLUMNS};values.update(name=name,market_hash_name='Exact \u03a9 identity',window_days=window,latest_price=price,provider_prices_json=provenance,groups_json='{"$g":[1]}',prior_price=0.0,history_points=0)
   return [values[c] for c in LOOKUP_COLUMNS]
  rows=[(100,row('Case \u03a9',7,1.25,'{"source":"one"}')),(200,row('Case \u03a9',30,1.25,'{"source":"one"}')),(300,row('Case \u03a9',90,1.25,'{"source":"two"}')),(400,row('case \u03a9',7,None,'{}'))]
  conn.execute('BEGIN');writer.insert_many(rows);conn.commit()
  actual=conn.execute('SELECT rowid,'+','.join(LOOKUP_COLUMNS)+' FROM market_item_lookup ORDER BY rowid').fetchall()
  assert [tuple((rid,*values)) for rid,values in rows]==actual
  assert writer.snapshot()['commonRows']==3 and writer.snapshot()['distinct7To30Variants']==0
  assert conn.execute('SELECT COUNT(*) FROM lookup_common WHERE name=?',('Case \u03a9',)).fetchone()[0]==2
  return dict(passed=True,groups=['all28columns_and_source_row_order_exact','only_exact_tuples_shared_provenance_variant_retained','case_Unicode_null_zero_preserved'])
 finally:conn.close()
