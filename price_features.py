"""Small current sales references, durable refresh intent and bounded quote closes."""
from contextlib import closing
import json,time,uuid
import quote_book as qb

HOUR=3600;DAY=86400;HISTORY_ROWS=240000;HISTORY_ALLOWANCE=32*1024**2
SCHEMA='''
CREATE TABLE IF NOT EXISTS cash_sales_reference(
 subject INTEGER NOT NULL REFERENCES subject(id),days INTEGER NOT NULL CHECK(days IN(1,7,30,90)),
 median_mills INTEGER,minimum_mills INTEGER,maximum_mills INTEGER,sales_count INTEGER NOT NULL,
 captured_us INTEGER NOT NULL,PRIMARY KEY(subject,days)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS cash_observed_close(
 subject INTEGER NOT NULL REFERENCES subject(id),grain INTEGER NOT NULL CHECK(grain IN(3600,86400)),
 bucket INTEGER NOT NULL,price_mills INTEGER NOT NULL CHECK(price_mills>0),as_of INTEGER NOT NULL,
 quote_captured_at INTEGER NOT NULL,provider TEXT NOT NULL CHECK(length(provider)<=32),
 PRIMARY KEY(subject,grain,bucket)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS cash_close_expiry ON cash_observed_close(grain,bucket);
'''

def meta(c,key,default=None):
 row=c.execute('SELECT value FROM meta WHERE key=?',(key,)).fetchone()
 return json.loads(row[0])if row else default
def put(c,key,value):c.execute('INSERT INTO meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',(key,qb.encode(value)))

def initialize(book,reference_provider):
 """Explicit additive controller step; does not change existing quotes or user state."""
 with book.connect(True)as c:
  for statement in SCHEMA.split(';'):
   if statement.strip():c.execute(statement)
  p=reference_provider
  c.execute('INSERT INTO provider(id,kind,currency,enabled,cadence,ttl,note,next_allowed) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING',
   (p['id'],p['kind'],'USD',int(p['enabled']),p['cadence'],p['ttl'],p['note'],int(book.clock())+60))
  c.execute("UPDATE provider SET note=? WHERE id='waxpeer_live'",('Public grouped purchase offers including trade lock; delivery may be delayed. Capture age only.',))
  if meta(c,'features_v1')is None:
   base=c.execute('PRAGMA page_count').fetchone()[0]*c.execute('PRAGMA page_size').fetchone()[0]
   put(c,'features_v1',{'version':1,'historyBaselineBytes':base,'historyGrowthAllowanceBytes':HISTORY_ALLOWANCE,'hourlyBuckets':48,'dailyBuckets':93,'maxHistoryRows':HISTORY_ROWS})
 book.has_references=True

class Features:
 def __init__(self,book):self.book=book
 def request(self):
  with self.book.connect()as c:return meta(c,'refresh_request')
 def enqueue(self,frozen):
  now=int(self.book.clock())
  with self.book.lock,self.book.connect(True)as c:
   old=meta(c,'refresh_request')
   if old and old['state']in('queued','fetching','waiting'):return old,True
   sources={}
   for p in self.book._providers(c):
    if p['enabled']and(p['kind']=='cash_ask'or p['id']=='skinport_sales'and(not p['last_received_us']or p['next_allowed']<=now)):
     sources[p['id']]={'state':'queued','baselineAttempts':p['attempts'],'beforeCaptureUs':p['last_received_us']or 0,'nextAllowedAt':p['next_allowed']}
   request={'id':uuid.uuid4().hex,'state':'queued','requestedAt':now,'requestedAtUs':int(self.book.clock()*1000000),
    'portfolioId':frozen['portfolioId'],'sources':sources,'completedAt':None}
   put(c,'refresh_request',request);return request,False
 def synchronize(self):
  now=int(self.book.clock())
  saved=self.request()
  if not saved or saved['state']not in('queued','fetching','waiting'):return
  with self.book.lock,self.book.connect(True)as c:
   request=meta(c,'refresh_request')
   if not request or request['state']not in('queued','fetching','waiting'):return
   old=qb.encode(request);providers={p['id']:p for p in self.book._providers(c)}
   for source,state in request['sources'].items():
    if state['state']in('completed','failed'):continue
    p=providers[source];state.update(nextAllowedAt=p['next_allowed'],lastError=p['last_error'],lastStatus=p['last_status'])
    if(p['last_received_us']or 0)>state['beforeCaptureUs']and(p['last_received_us']or 0)>=request['requestedAtUs']:
     state.update(state='completed',capturedAt=p['last_received_us']//1000000)
    elif not p['enabled']or(p['attempts']>state['baselineAttempts']and not p['lease']and p['last_error']):state['state']='failed'
    elif p['lease']:state['state']='fetching'
    else:state['state']='waiting'
   states=[s['state']for s in request['sources'].values()]
   if all(s in('completed','failed')for s in states):
    request['state']='completed'if all(s=='completed'for s in states)else'partial'if 'completed'in states else'failed';request['completedAt']=now
   else:request['state']='fetching'if 'fetching'in states else'waiting'
   if qb.encode(request)!=old:put(c,'refresh_request',request)

 def publish_sales(self,result,claim,held):
  rows=result.get('sales');timestamp=result.get('receivedAtUs')
  if not isinstance(rows,list)or len(rows)>10000 or type(timestamp)is not int or timestamp//1000000>int(self.book.clock())+60:raise ValueError('Bounded sales-reference capture required')
  held=set(held);seen=set()
  for r in rows:
   if r.get('name')not in held or r['name']in seen or not isinstance(r.get('windows'),list):raise ValueError('Invalid held sales-reference identity')
   seen.add(r['name'])
   for w in r['windows']:
    if w.get('days')not in(1,7,30,90)or type(w.get('volume'))is not int or w['volume']<0:raise ValueError('Invalid sales window')
    for field in('median_mills','minimum_mills','maximum_mills'):
     if w.get(field)is not None and(type(w[field])is not int or not 0<w[field]<=2**53-1):raise ValueError('Invalid sales amount')
  started=time.monotonic()
  with self.book.lock,self.book.connect(True)as c:
   p=c.execute("SELECT * FROM provider WHERE id='skinport_sales'").fetchone()
   if not p or p['lease']!=claim['token']:raise ValueError('Sales request ownership changed')
   cap=c.execute('INSERT INTO capture(provider,received_us,body_sha,row_count,request_url,clock_semantics) VALUES(?,?,?,?,?,?)',
    ('skinport_sales',timestamp,result['bodySHA256'],len(rows),result['requestUrl'],'rolling_completed_sales_windows_captured_at_not_current_ask')).lastrowid
   ids={r['market_hash_name']:r['id']for r in c.execute('SELECT id,market_hash_name FROM subject')}
   changed=0
   for row in rows:
    for w in row['windows']:
     changed+=c.execute('INSERT INTO cash_sales_reference VALUES(?,?,?,?,?,?,?) ON CONFLICT(subject,days) DO UPDATE SET median_mills=excluded.median_mills,minimum_mills=excluded.minimum_mills,maximum_mills=excluded.maximum_mills,sales_count=excluded.sales_count,captured_us=excluded.captured_us',
      (ids[row['name']],w['days'],w.get('median_mills'),w.get('minimum_mills'),w.get('maximum_mills'),w['volume'],timestamp)).rowcount
   c.execute("UPDATE provider SET last_capture=?,last_received_us=?,last_status=200,last_error=NULL,lease=NULL,lease_until=0,failures=0,last_seconds=?,last_changed=? WHERE id='skinport_sales'",(cap,timestamp,time.monotonic()-started,changed))
   c.execute("UPDATE meta SET value=CAST(value AS INTEGER)+1 WHERE key='revision'")
  with self.book.lock:self.book.cache.clear()
  return {'changedRows':changed,'seconds':time.monotonic()-started,'capturedItems':len(rows)}

 def record(self):
  """Only qualified current estimates for watched holdings; no old data backfill."""
  now=int(self.book.clock())
  with self.book.connect()as c:
   names=[r[0]for r in c.execute('SELECT s.market_hash_name FROM demand d JOIN subject s ON s.id=d.subject WHERE d.held=1')]
  if not names:return
  value=self.book.prices(names)
  try:
   with self.book.lock,self.book.connect(True)as c:
    if self.book.revision(c)!=value['revision']:return
    policy=meta(c,'features_v1');previous=meta(c,'history_status',{})
    if now<previous.get('lastRecordedAt',0):return
    c.execute('DELETE FROM cash_observed_close WHERE (grain=3600 AND bucket<?) OR (grain=86400 AND bucket<?)',(now//HOUR-47,now//DAY-92))
    ids={r['market_hash_name']:r['id']for r in c.execute('SELECT id,market_hash_name FROM subject')}
    for q in value['items']:
     if q['state']!='priced':continue
     for grain in(HOUR,DAY):
      c.execute('INSERT INTO cash_observed_close VALUES(?,?,?,?,?,?,?) ON CONFLICT(subject,grain,bucket) DO UPDATE SET price_mills=excluded.price_mills,as_of=excluded.as_of,quote_captured_at=excluded.quote_captured_at,provider=excluded.provider',
       (ids[q['marketHashName']],grain,now//grain,qb.millis(q['canonicalPriceUsd']),now,q['capturedAt'],q['selectedProvider']))
    rows=c.execute('SELECT COUNT(*) FROM cash_observed_close').fetchone()[0]
    size=c.execute('PRAGMA page_count').fetchone()[0]*c.execute('PRAGMA page_size').fetchone()[0]
    if rows>HISTORY_ROWS or size>policy['historyBaselineBytes']+HISTORY_ALLOWANCE:raise ValueError('History budget reached; current pricing continues and prior aggregates are retained')
    put(c,'history_status',{'startedAt':previous.get('startedAt',now),'lastRecordedAt':now,'rows':rows,'error':None})
  except ValueError as error:
   with self.book.connect(True)as c:
    previous=meta(c,'history_status',{});previous['error']=str(error);put(c,'history_status',previous)
 def history_status(self):
  with self.book.connect()as c:return dict(meta(c,'history_status',{}),policy=meta(c,'features_v1'))
 def history(self,name):
  with self.book.connect()as c:
   rows=[dict(r)for r in c.execute('SELECT h.* FROM cash_observed_close h JOIN subject s ON s.id=h.subject WHERE s.market_hash_name=? ORDER BY grain,bucket',(name,))]
   return {'marketHashName':name,'kind':'observed_qualified_cash_estimate','points':rows,'status':dict(meta(c,'history_status',{}),policy=meta(c,'features_v1')),
    'semantics':'Observed saved listing estimates, not trades or guaranteed sale proceeds. as_of is observation time; quote_captured_at is source freshness. No invented past points.'}
