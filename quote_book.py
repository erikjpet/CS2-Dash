"""One bounded current-quote ledger. No network, raw bodies or import-time I/O."""
from collections import OrderedDict
from contextlib import closing, contextmanager
from decimal import Decimal, ROUND_HALF_UP
import email.utils
import hashlib
import json
from pathlib import Path
import sqlite3
import threading
import time
import uuid
import price_references

VERSION=2
MAX_BYTES=128*1024**2
MAX_SUBJECTS=150000
MAX_QUOTES=600000
MAX_CAPTURE_ROWS=150000
POLICY='current-cash:v4:usd-gross-ask:cluster-sales-or-direct-seller-depth:withdrawn-excluded'
KINDS=('cash_ask','cash_bid','wallet_ask','reference')
SCHEMA='''
CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL) WITHOUT ROWID;
CREATE TABLE subject(id INTEGER PRIMARY KEY,market_hash_name TEXT NOT NULL UNIQUE COLLATE BINARY);
CREATE TABLE provider(id TEXT PRIMARY KEY,kind TEXT NOT NULL,currency TEXT NOT NULL,
 enabled INTEGER NOT NULL,cadence INTEGER NOT NULL,ttl INTEGER NOT NULL,
 next_allowed INTEGER NOT NULL DEFAULT 0,lease TEXT,lease_until INTEGER NOT NULL DEFAULT 0,
 failures INTEGER NOT NULL DEFAULT 0,attempts INTEGER NOT NULL DEFAULT 0,
 last_capture INTEGER,last_received_us INTEGER,last_status INTEGER,last_error TEXT,
 note TEXT NOT NULL DEFAULT '',last_seconds REAL,last_changed INTEGER) WITHOUT ROWID;
CREATE TABLE capture(id INTEGER PRIMARY KEY,provider TEXT NOT NULL REFERENCES provider(id),
 received_us INTEGER NOT NULL,provider_time INTEGER,body_sha TEXT NOT NULL,
 row_count INTEGER NOT NULL,request_url TEXT NOT NULL,clock_semantics TEXT NOT NULL,
 digest_kind TEXT NOT NULL DEFAULT 'body',
 UNIQUE(provider,received_us,body_sha));
CREATE TABLE quote(subject INTEGER NOT NULL REFERENCES subject(id),
 provider TEXT NOT NULL REFERENCES provider(id),currency TEXT NOT NULL,kind TEXT NOT NULL,
 price_mills INTEGER CHECK(price_mills IS NULL OR price_mills>0),quantity INTEGER,
 state TEXT NOT NULL,observed_at INTEGER,capture INTEGER REFERENCES capture(id),
 reason TEXT,active INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(subject,provider,currency,kind)) WITHOUT ROWID;
CREATE INDEX quote_provider_kind ON quote(provider,kind,subject);
CREATE INDEX quote_capture ON quote(capture);
CREATE TABLE demand(subject INTEGER PRIMARY KEY REFERENCES subject(id),
 held INTEGER NOT NULL DEFAULT 0,hits INTEGER NOT NULL DEFAULT 0,last_requested INTEGER NOT NULL DEFAULT 0,
 pending_steam INTEGER NOT NULL DEFAULT 0);
INSERT INTO meta VALUES('revision','0');
PRAGMA user_version=2;
'''

def encode(value):
 return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)

def cents(value):
 return float(Decimal(str(value)).quantize(Decimal('.01'),rounding=ROUND_HALF_UP))

def millis(value):
 if isinstance(value,bool):return None
 try:
  amount=Decimal(str(value));scaled=amount*1000
  return int(scaled) if amount.is_finite() and 0<scaled<=2**53-1 and scaled==scaled.to_integral_value() else None
 except Exception:return None

def retry_deadline(value,now):
 try:return now+max(0,int(value))
 except (ValueError,TypeError):
  try:return max(now,int(email.utils.parsedate_to_datetime(value).timestamp()))
  except (ValueError,TypeError,OverflowError):return now

def initialize(path,providers):
 path=Path(path)
 if path.exists():raise ValueError('Existing quote authority must not be overwritten')
 with closing(sqlite3.connect(path,isolation_level=None)) as conn:
  conn.execute('PRAGMA journal_mode=WAL');conn.execute('PRAGMA synchronous=FULL')
  conn.execute('PRAGMA wal_autocheckpoint=1000');conn.execute('PRAGMA journal_size_limit=8388608')
  conn.executescript('BEGIN IMMEDIATE;'+SCHEMA+'COMMIT;')
  conn.execute('BEGIN IMMEDIATE')
  try:
   for p in providers:
    conn.execute('INSERT INTO provider(id,kind,currency,enabled,cadence,ttl,note,next_allowed) VALUES(?,?,?,?,?,?,?,?)',
      (p['id'],p['kind'],p.get('currency','USD'),int(p.get('enabled',False)),p['cadence'],p['ttl'],p.get('note',''),p.get('nextAllowedAt',0)))
   conn.commit()
  except BaseException:conn.rollback();raise

class Book:
 def __init__(self,path,clock=time.time):
  self.path=Path(path).resolve();self.clock=clock;self.lock=threading.RLock();self.cache=OrderedDict()
  with self.connect() as c:
   self.has_references=bool(c.execute("SELECT 1 FROM sqlite_schema WHERE name='cash_sales_reference'").fetchone())
   if c.execute('PRAGMA user_version').fetchone()[0]!=VERSION:raise ValueError('Unqualified current-quote schema')
   for table,required in {'capture':{'digest_kind','received_us'},'demand':{'pending_steam','held'},'quote':{'price_mills','capture','kind','active'},'provider':{'lease','next_allowed'}}.items():
    columns={r['name']for r in c.execute('PRAGMA table_info('+table+')')}
    if not required<=columns:raise ValueError('Incomplete current-quote schema: '+table)

 @contextmanager
 def connect(self,write=False):
  if not self.path.is_file() or self.path.stat().st_size>MAX_BYTES:raise ValueError('Current quote store missing or above128MiB cap')
  with closing(sqlite3.connect(self.path.as_uri()+('?mode=rw' if write else '?mode=ro'),uri=True,
                              isolation_level=None,timeout=.5)) as c:
   c.row_factory=sqlite3.Row;c.execute('PRAGMA foreign_keys=ON');c.execute('PRAGMA cache_size=-4096')
   if write:
    c.execute('PRAGMA synchronous=FULL');c.execute('PRAGMA wal_autocheckpoint=1000');c.execute('PRAGMA journal_size_limit=8388608');c.execute('PRAGMA max_page_count='+str(MAX_BYTES//c.execute('PRAGMA page_size').fetchone()[0]))
   else:c.execute('PRAGMA query_only=ON')
   until=time.monotonic()+8;c.set_progress_handler(lambda:int(time.monotonic()>until),1000)
   try:
    c.execute('BEGIN IMMEDIATE' if write else 'BEGIN');yield c
    if write:c.commit()
   except BaseException:
    if write:c.rollback()
    raise

 def revision(self,c=None):
  if c is None:
   with self.connect() as c:return self.revision(c)
  return hashlib.sha256((POLICY+':'+c.execute("SELECT value FROM meta WHERE key='revision'").fetchone()[0]).encode()).hexdigest()

 def providers(self):
  with self.connect() as c:
   return self._providers(c)

 def _providers(self,c):
  return [dict(r) for r in c.execute('SELECT p.*,c.body_sha,c.request_url,c.clock_semantics,c.row_count,c.provider_time FROM provider p LEFT JOIN capture c ON c.id=p.last_capture ORDER BY p.id')]

 def status_snapshot(self):
  with self.connect()as c:return self._providers(c),self.revision(c)

 def enable(self,provider,enabled):
  if provider!='steam' or type(enabled)is not bool:raise ValueError('Only explicit Steam on-demand control is supported')
  with self.lock,self.connect(True)as c:
   # An admitted request may finish; pausing prevents the next admission and
   # preserves pending demand, leases, cooldowns and all saved prices.
   c.execute('UPDATE provider SET enabled=? WHERE id=?',(int(enabled),provider))

 def claim(self,provider,*,requested=False):
  now=int(self.clock())
  with self.lock,self.connect(True) as c:
   p=c.execute('SELECT * FROM provider WHERE id=?',(provider,)).fetchone()
   if not p or not p['enabled']:return {'action':'blocked','reason':p['note'] if p else 'unknown_provider','retryAt':None}
   if p['lease']:
    if p['lease_until']>now:return {'action':'reused','reason':'single_flight','retryAt':p['lease_until']}
    c.execute("UPDATE provider SET lease=NULL,lease_until=0,next_allowed=MAX(next_allowed,?),last_error='Prior request outcome uncertain; paced recovery' WHERE id=?",(now+p['cadence'],provider))
    return {'action':'deferred','reason':'uncertain_request_recovery','retryAt':now+p['cadence']}
   if p['next_allowed']>now:return {'action':'deferred','reason':'provider_pacing_or_backoff','retryAt':p['next_allowed']}
   if provider.startswith('skinport'):
    row=c.execute("SELECT value FROM meta WHERE key='skinport_origin_next'").fetchone();origin_next=int(row[0])if row else 0
    if origin_next>now:
     c.execute('UPDATE provider SET next_allowed=MAX(next_allowed,?) WHERE id=?',(origin_next,provider))
     return {'action':'deferred','reason':'shared_skinport_origin_pacing','retryAt':origin_next}
    c.execute("INSERT INTO meta VALUES('skinport_origin_next',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(str(now+45),))
   token=uuid.uuid4().hex
   c.execute('UPDATE provider SET lease=?,lease_until=?,next_allowed=?,attempts=attempts+1 WHERE id=?',
             (token,now+45,now+p['cadence'],provider))
   return {'action':'request','provider':provider,'token':token,'startedAt':now}

 def fail(self,claim,status,error,retry_after=None):
  now=int(self.clock())
  with self.lock,self.connect(True) as c:
   p=c.execute('SELECT * FROM provider WHERE id=?',(claim['provider'],)).fetchone()
   if not p or p['lease']!=claim['token']:raise ValueError('Request lease changed')
   failures=p['failures']+1
   delay=min(21600,p['cadence']*2**min(failures,8))
   until=max(p['next_allowed'],now+delay,retry_deadline(retry_after,now))
   c.execute('UPDATE provider SET lease=NULL,lease_until=0,failures=?,next_allowed=?,last_status=?,last_error=?,enabled=CASE WHEN ? IN (401,403) THEN 0 ELSE enabled END WHERE id=?',
    (failures,until,status,str(error)[:400],status,claim['provider']))
   if claim['provider'].startswith('skinport')and status==429:
    c.execute("INSERT INTO meta VALUES('skinport_origin_next',?) ON CONFLICT(key) DO UPDATE SET value=CAST(MAX(CAST(value AS INTEGER),CAST(excluded.value AS INTEGER)) AS TEXT)",(str(until),))
    c.execute("UPDATE provider SET next_allowed=MAX(next_allowed,?) WHERE id LIKE 'skinport%'",(until,))
   return until

 def finish_request_without_quote(self,claim,status=200):
  """Close a shared-origin history request without creating a wallet quote.

  Admission/cooldown state is shared with priceoverview. HTTP success does not
  change capture receipts, current prices, source ages or the quote revision.
  """
  if claim.get('provider')!='steam' or type(status)is not int or not 200<=status<400:
   raise ValueError('An admitted successful or canonical-redirect Steam request is required')
  with self.lock,self.connect(True)as c:
   row=c.execute('SELECT lease FROM provider WHERE id=?',('steam',)).fetchone()
   if not row or row['lease']!=claim.get('token'):raise ValueError('Request lease changed')
   c.execute('UPDATE provider SET lease=NULL,lease_until=0,failures=0,last_status=?,last_error=NULL WHERE id=?',(status,'steam'))

 def publish(self,provider,receipt,rows,claim=None,*,item_scope=None):
  """Atomic bounded upserts. Existing history is not imported or discarded here."""
  if not isinstance(rows,list) or not 1<=len(rows)<=MAX_CAPTURE_ROWS:raise ValueError('Nonempty bounded source capture required')
  timestamp=receipt.get('receivedAtUs');sha=receipt.get('bodySHA256')
  if (type(timestamp)is not int or timestamp<=0 or timestamp//1000000>int(self.clock())+60
      or not isinstance(sha,str) or len(sha)!=64 or any(ch not in '0123456789abcdef' for ch in sha)):
   raise ValueError('Invalid source receipt')
  seen=set()
  for r in rows:
   if (not isinstance(r,dict) or not isinstance(r.get('name'),str) or not 0<len(r['name'])<=512
       or any(ch in r['name']for ch in '\0\r\n') or r['name'] in seen or r.get('state')not in ('priced','no_listings','ambiguous','invalid','missing')):raise ValueError('Invalid or duplicate economic identity')
   seen.add(r['name']);price=r.get('price_mills');quantity=r.get('quantity')
   if r['state']=='priced' and (type(price)is not int or not 0<price<=2**53-1 or quantity is not None and (type(quantity)is not int or quantity<=0)):raise ValueError('Positive exact price and availability required')
   if r['state']!='priced' and price is not None:raise ValueError('Unavailable quotes cannot have prices')
   if quantity is not None and(type(quantity)is not int or not 0<=quantity<=2**53-1):raise ValueError('Invalid availability')
   if r.get('reason')is not None and(not isinstance(r['reason'],str)or len(r['reason'])>512):raise ValueError('Bounded reason required')
   if r.get('observed_at')is not None and (type(r['observed_at'])is not int or r['observed_at']<0):raise ValueError('Invalid provider clock')
  if item_scope is not None and(provider!='csfloat'or seen!={item_scope}):raise ValueError('CSFloat publication must contain exactly its requested item')
  if provider=='csfloat'and item_scope is None:raise ValueError('CSFloat is an individual-item source, not a full feed')
  if receipt.get('providerTime')is not None and (type(receipt['providerTime'])is not int or not 0<receipt['providerTime']<=int(self.clock())+60):raise ValueError('Invalid source snapshot clock')
  started=time.monotonic()
  with self.lock,self.connect(True) as c:
   p=c.execute('SELECT * FROM provider WHERE id=?',(provider,)).fetchone()
   if not p:raise ValueError('Registered source required')
   if claim and p['lease']!=claim['token']:raise ValueError('Capture lost its request ownership')
   if p['last_received_us'] and timestamp<=p['last_received_us']:
    old=c.execute('SELECT body_sha FROM capture WHERE id=?',(p['last_capture'],)).fetchone()
    if timestamp==p['last_received_us'] and old[0]==sha:
     if claim:c.execute('UPDATE provider SET lease=NULL,lease_until=0,last_error=NULL,last_status=200 WHERE id=?',(provider,))
     return {'reused':True,'changedRows':0}
    raise ValueError('Older/conflicting capture cannot replace current quotes')
   old_count=c.execute('SELECT COUNT(*) FROM subject').fetchone()[0]
   existing={r[0] for r in c.execute('SELECT market_hash_name FROM subject')}
   if old_count+len(seen-existing)>MAX_SUBJECTS:raise ValueError('Current subject capacity exceeded')
   cap=c.execute('INSERT INTO capture(provider,received_us,provider_time,body_sha,row_count,request_url,clock_semantics) VALUES(?,?,?,?,?,?,?)',
     (provider,timestamp,receipt.get('providerTime'),sha,len(rows),receipt['requestUrl'],receipt.get('clockSemantics','capture_only'))).lastrowid
   c.executemany('INSERT INTO subject(market_hash_name) VALUES(?) ON CONFLICT DO NOTHING',((r['name'],)for r in rows))
   ids={r['market_hash_name']:r['id'] for r in c.execute('SELECT id,market_hash_name FROM subject')}
   changed=0;full=p['kind']=='cash_ask'and item_scope is None
   membership=1 if full else 2 if item_scope is not None else 0
   if full:
    c.execute('CREATE TEMP TABLE incoming_subject(id INTEGER PRIMARY KEY)')
    c.executemany('INSERT INTO incoming_subject VALUES(?)',((ids[r['name']],)for r in rows))
    changed+=c.execute('''UPDATE quote SET active=0,capture=? WHERE provider=? AND active=1
      AND subject NOT IN(SELECT id FROM incoming_subject)''',(p['last_capture'],provider)).rowcount
   # Full captures advance one shared head. Only changed facts or membership write item rows.
   # Removed subjects pin their last observed receipt and are never freshened by a sparse response.
   for start in range(0,len(rows),1000):
    chunk=rows[start:start+1000]
    cursor=c.executemany('''INSERT INTO quote(subject,provider,currency,kind,price_mills,quantity,state,observed_at,capture,reason,active) VALUES(?,?,?,?,?,?,?,?,?,?,?)
       ON CONFLICT(subject,provider,currency,kind) DO UPDATE SET price_mills=excluded.price_mills,
       quantity=excluded.quantity,state=excluded.state,observed_at=excluded.observed_at,capture=excluded.capture,reason=excluded.reason,active=excluded.active
       WHERE quote.price_mills IS NOT excluded.price_mills OR quote.quantity IS NOT excluded.quantity
       OR quote.state IS NOT excluded.state OR quote.observed_at IS NOT excluded.observed_at
       OR quote.reason IS NOT excluded.reason OR quote.active IS NOT excluded.active OR quote.capture IS NOT excluded.capture''',
      ((ids[r['name']],provider,p['currency'],p['kind'],r.get('price_mills'),r.get('quantity'),r['state'],r.get('observed_at'),None if full else cap,r.get('reason'),membership)for r in chunk))
    changed+=cursor.rowcount
   if full:c.execute('DROP TABLE incoming_subject')
   if c.execute('SELECT COUNT(*) FROM quote').fetchone()[0]>MAX_QUOTES:raise ValueError('Current quote capacity exceeded')
   c.execute('UPDATE provider SET last_capture=?,last_received_us=?,last_status=200,last_error=NULL,lease=NULL,lease_until=0,failures=0,last_seconds=?,last_changed=? WHERE id=?',
             (cap,timestamp,time.monotonic()-started,changed,provider))
   # Unseen subjects retain their old timestamp and expire; a sparse feed cannot freshen them.
   c.execute('DELETE FROM capture WHERE NOT EXISTS(SELECT 1 FROM quote WHERE quote.capture=capture.id) AND NOT EXISTS(SELECT 1 FROM provider WHERE provider.last_capture=capture.id)')
   c.execute("UPDATE meta SET value=CAST(value AS INTEGER)+1 WHERE key='revision'")
  with self.lock:self.cache.clear()
  return {'reused':False,'changedRows':changed,'seconds':time.monotonic()-started,'capture':cap}

 def set_held(self,names):
  names=list(dict.fromkeys(names))
  if len(names)>10000:raise ValueError('Bounded held-item scope required')
  with self.lock,self.connect(True) as c:
   existing={r[0]for r in c.execute('SELECT market_hash_name FROM subject')}
   if len(existing|set(names))>MAX_SUBJECTS:raise ValueError('Subject capacity reached')
   c.execute('UPDATE demand SET held=0')
   c.executemany('INSERT INTO subject(market_hash_name) VALUES(?) ON CONFLICT DO NOTHING',((n,)for n in names))
   c.executemany('INSERT INTO demand(subject,held) SELECT id,1 FROM subject WHERE market_hash_name=? ON CONFLICT(subject) DO UPDATE SET held=1',((n,)for n in names))

 def request_steam(self,name):
  if not isinstance(name,str)or not 0<len(name)<=512 or '\0'in name:raise ValueError('One exact public market hash required')
  with self.lock,self.connect(True)as c:
   if c.execute('SELECT COUNT(*) FROM subject').fetchone()[0]>=MAX_SUBJECTS and not c.execute('SELECT 1 FROM subject WHERE market_hash_name=?',(name,)).fetchone():raise ValueError('Subject capacity reached')
   if c.execute('SELECT COUNT(*) FROM demand WHERE pending_steam=1').fetchone()[0]>=1000:raise ValueError('Explicit request queue capacity reached')
   c.execute('INSERT INTO subject(market_hash_name) VALUES(?) ON CONFLICT DO NOTHING',(name,))
   c.execute('INSERT INTO demand(subject,hits,last_requested,pending_steam) SELECT id,1,?,1 FROM subject WHERE market_hash_name=? ON CONFLICT(subject) DO UPDATE SET hits=MIN(hits+1,1000000000),last_requested=excluded.last_requested,pending_steam=1',(int(self.clock()),name))

 def next_steam(self):
  with self.connect()as c:
   row=c.execute('''SELECT s.market_hash_name FROM demand d JOIN subject s ON s.id=d.subject
    LEFT JOIN quote q ON q.subject=s.id AND q.provider='steam' AND q.kind='wallet_ask' AND q.currency='USD'
    LEFT JOIN capture cap ON cap.id=q.capture WHERE d.pending_steam=1
    ORDER BY d.held DESC,COALESCE(cap.received_us,0),d.hits DESC,d.last_requested,s.id LIMIT 1''').fetchone()
   return row[0]if row else None

 def complete_steam(self,name):
  with self.lock,self.connect(True)as c:c.execute('UPDATE demand SET pending_steam=0 WHERE subject=(SELECT id FROM subject WHERE market_hash_name=?)',(name,))

 def prices(self,names,*,kind='cash_ask',include_sources=False):
  if not isinstance(names,list) or len(names)>MAX_SUBJECTS or any(not isinstance(n,str)or not 0<len(n)<=512 or '\0'in n for n in names):raise ValueError('Bounded exact names required')
  names=list(dict.fromkeys(names));now=int(self.clock())
  with self.connect() as c:
   revision=self.revision(c);key=(revision,kind,include_sources,tuple(names)) if len(names)<=10000 else None
   with self.lock:
    cached=self.cache.get(key) if key else None
    if cached and now<cached[0]:self.cache.move_to_end(key);return cached[1]
   grouped={name:[] for name in names};receipts={};references={};other_quotes={name:[]for name in names};sales_windows={name:[]for name in names}
   for start in range(0,len(names),500):
    chunk=names[start:start+500]
    for r in c.execute('''SELECT s.market_hash_name,q.*,p.ttl,c.received_us,c.body_sha,c.clock_semantics,c.provider_time,c.digest_kind
      FROM subject s JOIN quote q ON q.subject=s.id JOIN provider p ON p.id=q.provider
      JOIN capture c ON c.id=CASE WHEN q.active=1 THEN p.last_capture ELSE q.capture END WHERE s.market_hash_name IN('''+','.join('?'for _ in chunk)+''') AND q.currency='USD' AND q.kind=?''',chunk+[kind]):
     grouped[r['market_hash_name']].append(dict(r))
    if self.has_references:
     for r in c.execute('SELECT s.market_hash_name,r.* FROM cash_sales_reference r JOIN subject s ON s.id=r.subject WHERE s.market_hash_name IN('+','.join('?'for _ in chunk)+')',chunk):
      sales_windows[r['market_hash_name']].append(dict(r))
      if r['days']==7:references[r['market_hash_name']]=dict(r)
    if kind=='cash_ask':
     for r in c.execute('''SELECT s.market_hash_name,q.*,p.ttl,c.received_us,c.clock_semantics,c.provider_time
      FROM subject s JOIN quote q ON q.subject=s.id JOIN provider p ON p.id=q.provider JOIN capture c ON c.id=q.capture
      WHERE s.market_hash_name IN('''+','.join('?'for _ in chunk)+''')AND q.currency='USD'AND q.kind IN('reference','wallet_ask')''',chunk):other_quotes[r['market_hash_name']].append(dict(r))
   for r in c.execute('SELECT p.id,p.ttl,c.received_us,c.body_sha,c.request_url,c.clock_semantics FROM provider p JOIN capture c ON c.id=p.last_capture WHERE p.kind=? ORDER BY p.id',(kind,)):
    receipts[r['id']]=dict(r)
  result={'ok':True,'provider':'canonical_cash'if kind=='cash_ask'else'steam','revision':revision,'currency':'USD',
   'basis':'lowest_comparable_cash_listing'if kind=='cash_ask'else'lowest_ask','valuationLane':'canonical_cash',
   'freshnessSeconds':600,'sourceReceipts':[{'provider':r['id'],'receivedAtUs':r['received_us'],'bodySHA256':r['body_sha'],'requestUrl':r['request_url']}for r in receipts.values()],
   'sourceSummary':[{'provider':r['id'],'currency':'USD','quoteKind':'lowest_available_listing','feeBasis':'gross_listing_price_usd','freshnessSeconds':r['ttl']}for r in receipts.values()],
   'items':[select(name,grouped[name],now,kind,references.get(name))for name in names]}
  result['sourceReceipt']=max(result['sourceReceipts'],key=lambda r:r['receivedAtUs'],default=None)
  if kind=='cash_ask':
   for item in result['items']:
    name=item['marketHashName'];item['referencePrice']=price_references.choose(name,item,grouped[name],other_quotes[name],sales_windows[name],now)
  if include_sources:
   for item in result['items']:
    item['sourceQuotes']=[]
    for row in sorted(grouped[item['marketHashName']],key=lambda r:r['provider']):
     captured=row['received_us']//1000000;snapshot=row['provider_time']if row['clock_semantics']=='provider_snapshot_time'else None
     expiry=min(captured,snapshot or captured)+row['ttl'];fresh=captured<=now+60 and now<expiry
     present=bool(row.get('active',1));available=row['state']=='priced'and fresh and present and bool(row['price_mills'])and bool(row['quantity'])
     item['sourceQuotes'].append({'provider':row['provider'],'currency':row['currency'],'kind':row['kind'],
      'currentAskUsd':row['price_mills']/1000 if available else None,'lastObservedAskUsd':row['price_mills']/1000 if row['price_mills']else None,
      'quantityListed':row['quantity'],'state':'absent_from_latest_feed'if not present else row['state']if fresh else'stale','capturedAt':captured,
      'providerUpdatedAt':snapshot or row['observed_at'],'providerTimeSemantics':row['clock_semantics'],'expiresAt':expiry,
      'selected':item['selectedProvider']==row['provider'],'reason':row['reason'],'feeBasis':'gross_listing_price_usd'})
   if self.has_references:
    with self.connect()as c:
     for item in result['items']:
      item['salesWindows']=[dict(r)for r in c.execute('SELECT r.days,r.median_mills,r.minimum_mills,r.maximum_mills,r.sales_count,r.captured_us FROM cash_sales_reference r JOIN subject s ON s.id=r.subject WHERE s.market_hash_name=? ORDER BY r.days',(item['marketHashName'],))]
  if key:
   expiry=min([item['expiresAt']for item in result['items']if item.get('expiresAt')and item['expiresAt']>now]+[now+30])
   with self.lock:
    self.cache[key]=(expiry,result);self.cache.move_to_end(key)
    while len(self.cache)>8 or sum(len(v[1]['items'])for v in self.cache.values())>20000:self.cache.popitem(last=False)
  return result

def select(name,rows,now,kind='cash_ask',reference=None):
 result={'name':name,'marketHashName':name,'canonicalPriceUsd':None,'status':'unpriced','state':'missing',
  'selectedProvider':None,'capturedAt':None,'providerUpdatedAt':None,'expiresAt':None,
  'quoteKind':'lowest_available_listing','feeBasis':'gross_listing_price_usd','currency':'USD',
  'freshnessState':'missing','freshnessSeconds':600,'reason':'no_qualified_current_cash_listing',
  'providerTimeSemantics':'capture_time_and_provider_change_time_are_separate'}
 eligible=[];diagnostics=[]
 for r in rows:
  captured=r['received_us']//1000000
  snapshot=r.get('provider_time') if r['clock_semantics']=='provider_snapshot_time'else None
  expiry=min(captured,snapshot)+r['ttl'] if snapshot else captured+r['ttl']
  if kind=='cash_ask'and not r.get('active',1):
   diagnostics.append((captured,'missing',dict(r,reason='not_returned_by_latest_source_feed')));continue
  if r['state']=='priced' and captured<=now+60 and now<expiry and r['price_mills'] and (kind!='cash_ask'or r['quantity']and r['quantity']>0):eligible.append(r)
  else:diagnostics.append((captured,'stale'if r['state']=='priced'and now>=expiry else r['state'],r))
 if 'doppler'in name.casefold():result.update(state='ambiguous',status='ambiguous',reason='variant_phase_not_independently_bound');return result
 if eligible:
  eligible.sort(key=lambda r:(r['price_mills'],r['provider']));best=eligible[0];corroborated=False;excluded=0;transaction_supported=False;depth_supported=False
  if kind=='cash_ask':
   clusters=[[r for r in eligible if low['price_mills']<=r['price_mills']<=low['price_mills']*2]for low in eligible]
   clusters=[group for group in clusters if len(group)>=2 and any(r['quantity']>=3 for r in group)]
   clusters.sort(key=lambda group:(-len(group),group[-1]['price_mills']/group[0]['price_mills'],group[0]['price_mills']))
   if clusters:
    cluster=clusters[0];best=cluster[0];corroborated=True;excluded=len(eligible)-len(cluster)
   else:
    trusted=next((r for r in eligible if r['provider']=='skinport_live'and r['quantity']>=3),None)
    if trusted:best=trusted;excluded=len(eligible)-1
    else:
     valid_ref=bool(reference and reference['sales_count']>=5 and reference['median_mills']and reference['captured_us']//1000000<=now+60 and now<reference['captured_us']//1000000+3600)
     supported=[r for r in eligible if valid_ref and reference['median_mills']<=r['price_mills']*2 and r['price_mills']<=reference['median_mills']*2]
     def direct_depth(r):
      if r['provider']!='csfloat':return False
      parts=(r.get('reason')or'').split(':')
      return len(parts)==3 and parts[0]=='direct_listing_depth_v1'and all(v.isdigit()for v in parts[1:])and 3<=int(parts[2])<=int(parts[1])<=r['quantity']<=50
     direct=next((r for r in eligible if direct_depth(r)and(not valid_ref or reference['median_mills']<=r['price_mills']*2 and r['price_mills']<=reference['median_mills']*2)),None)
     if direct:
      best=direct;depth_supported=True;excluded=len(eligible)-1
      parts=direct['reason'].split(':');result['listingDepthEvidence']={'provider':'csfloat','nearbyListings':int(parts[1]),'reportedSellers':int(parts[2]),'maximumPriceRatio':1.25}
     elif supported:
      best=supported[0];transaction_supported=True;excluded=len(eligible)-1
      result['salesEvidence']={'provider':'skinport_sales','days':7,'medianUsd':reference['median_mills']/1000,'sales':reference['sales_count'],'capturedAt':reference['captured_us']//1000000,'expiresAt':reference['captured_us']//1000000+3600}
     else:
      result.update(state='ambiguous',status='ambiguous',qualityState='outlier'if valid_ref else'thin'if best['quantity']<3 else'uncorroborated',
       indicativeAskUsd=best['price_mills']/1000,quantityListed=best['quantity'],
       indicativeProvider=best['provider'],capturedAt=best['received_us']//1000000,expiresAt=best['received_us']//1000000+best['ttl'],
       reason='ask_outside_recent_sales_reference'if valid_ref else'thin_or_new_cash_source_without_independent_price_corroboration');return result
  effective_clock=best.get('provider_time')if best['clock_semantics']=='provider_snapshot_time'else None
  result.update(capturedAt=best['received_us']//1000000,providerUpdatedAt=effective_clock or best['observed_at'],
   expiresAt=min(best['received_us']//1000000,effective_clock or best['received_us']//1000000)+best['ttl'],quantityListed=best['quantity'],
   indicativeAskUsd=best['price_mills']/1000,receiptBodySHA256=best['body_sha']if best.get('digest_kind','body')=='body'else None,
   receiptSHA256=best['body_sha'],receiptDigestKind=best.get('digest_kind','body'),freshnessSeconds=best['ttl'],
   providerTimeSemantics=best['clock_semantics'],excludedSourceQuotes=excluded,
   corroboratedSources=len(clusters[0])if kind=='cash_ask'and corroborated else 1)
  result.update(canonicalPriceUsd=best['price_mills']/1000,selectedProvider=best['provider'],status='priced',state='priced',freshnessState='fresh',reason=None,qualityState='listing_depth_supported'if depth_supported else'transaction_supported'if transaction_supported else'corroborated'if corroborated else'listed')
  if transaction_supported:result['expiresAt']=min(result['expiresAt'],result['salesEvidence']['expiresAt'])
 elif diagnostics:
  captured,state,r=max(diagnostics,key=lambda item:item[0]);result.update(capturedAt=captured,providerUpdatedAt=r['observed_at'],expiresAt=captured+r['ttl'],
    state=state,status='stale'if state=='stale'else'ambiguous'if state=='ambiguous'else'unpriced',freshnessState='stale'if state=='stale'else'missing',reason=r['reason']or'capture_expired'if state=='stale'else r['reason']or'no_available_listing')
 return result
