"""One persisted provider gate; item snapshots never freshen other held items."""
import json,time
import csfloat_read

NOTE='Backend-only buy-now listing GETs; one item per admission, not a full-market snapshot. Earlier unauthenticated probe403; authenticated access awaits qualification.'
NEGATIVE_RECHECK_SECONDS=6*3600

def state(book):
 with book.connect()as c:
  row=c.execute("SELECT value FROM meta WHERE key='csfloat_control'").fetchone()
  saved=json.loads(row[0])if row else {}
 return dict(saved,credentialConfigured=csfloat_read.configured(),scope='per_item_held_priority',localMinimumIntervalSeconds=60,
  schedulingPolicy='positive_missing_then_bounded_discovery',negativeRecheckSeconds=NEGATIVE_RECHECK_SECONDS)

def request_item(book,name):
 if not isinstance(name,str)or not 0<len(name)<=512 or any(ch in name for ch in '\0\r\n')or'doppler'in name.casefold():raise ValueError('One exact non-phase item identity is required')
 now=int(book.clock())
 with book.lock,book.connect(True)as c:
  p=c.execute("SELECT enabled,next_allowed FROM provider WHERE id='csfloat'").fetchone()
  if not p['enabled']:raise ValueError('CSFloat read access is paused or unavailable; no request sent')
  if not c.execute('SELECT 1 FROM subject WHERE market_hash_name=?',(name,)).fetchone():raise ValueError('Item is not in the known current catalog; no request sent')
  row=c.execute("SELECT cap.received_us FROM quote q JOIN subject s ON s.id=q.subject JOIN capture cap ON cap.id=q.capture WHERE s.market_hash_name=?AND q.provider='csfloat'",(name,)).fetchone()
  if row and row[0]//1000000>now-60:return {'accepted':True,'cached':True,'nextAllowedAt':p['next_allowed']}
  saved=c.execute("SELECT value FROM meta WHERE key='csfloat_requested'").fetchone();queue=json.loads(saved[0])if saved else[]
  reused=name in queue
  if not reused:
   if len(queue)>=100:raise ValueError('The bounded item queue is full; wait for existing requests')
   queue.append(name);c.execute("INSERT INTO meta VALUES('csfloat_requested',?)ON CONFLICT(key)DO UPDATE SET value=excluded.value",(json.dumps(queue),))
  return {'accepted':True,'cached':False,'reused':reused,'nextAllowedAt':p['next_allowed']}

def activate(book):
 # An explicit operator step, never an automatic re-enable after401/403.
 csfloat_read.credential();now=int(book.clock())
 with book.lock,book.connect(True)as c:
  p=c.execute("SELECT lease,lease_until FROM provider WHERE id='csfloat'").fetchone()
  if p['lease']and p['lease_until']>now:raise ValueError('CSFloat request is already in flight')
  c.execute("UPDATE provider SET enabled=1,cadence=60,ttl=600,note=?,next_allowed=MAX(next_allowed,?) WHERE id='csfloat'",(NOTE,now))
  c.execute("INSERT INTO meta VALUES('csfloat_control',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(json.dumps({'activatedAt':now,'qualified':False,'lastItem':None}),))

def next_item(book):
 control=state(book)
 if not control.get('qualified'):return 'Prisma 2 Case'
 now=int(book.clock())
 with book.connect()as c:
  requested=c.execute("SELECT value FROM meta WHERE key='csfloat_requested'").fetchone()
  if requested:
   queue=json.loads(requested[0])
   if queue:return queue[0]
  rows=[dict(r)for r in c.execute("""SELECT s.market_hash_name,q.state,q.price_mills,q.quantity,cap.received_us FROM demand d JOIN subject s ON s.id=d.subject
    LEFT JOIN quote q ON q.subject=s.id AND q.provider='csfloat' AND q.currency='USD' AND q.kind='cash_ask'
    LEFT JOIN capture cap ON cap.id=q.capture WHERE d.held=1 AND lower(s.market_hash_name) NOT LIKE '%doppler%'
    AND(cap.received_us IS NULL OR cap.received_us<?) ORDER BY COALESCE(cap.received_us,0),s.id LIMIT 10000""",((now-600)*1000000,))]
 if not rows:return None
 prices=book.prices([r['market_hash_name']for r in rows],include_sources=True);items={q['marketHashName']:q for q in prices['items']}
 positives=[];discovery=[]
 for r in rows:
  current=items[r['market_hash_name']]
  if current['state']=='priced':continue # Bulk feeds already cover this item; spend admissions on gaps.
  stamp=(r['received_us']or 0)//1000000
  if r['state']=='priced':
   # Old quotes guide request priority only. Never publish or value them as fresh.
   comparable=any(s['provider']!='csfloat'and s['currentAskUsd']is not None and
    s['currentAskUsd']*.5<=r['price_mills']/1000<=s['currentAskUsd']*2 and max(r['quantity']or 0,s['quantityListed']or 0)>=3
    for s in current['sourceQuotes'])
   positives.append((not comparable,stamp,r['market_hash_name']))
  elif not stamp or now>=stamp+NEGATIVE_RECHECK_SECONDS:discovery.append((stamp,r['market_hash_name']))
 # One of every five successful automatic admissions explores never/negative items.
 # Empty markets have a retry age, not an extended quote-freshness allowance.
 if discovery and (not positives or control.get('successfulRequests',0)%5==4):return min(discovery)[1]
 if positives:return min(positives)[2]
 return min(discovery)[1]if discovery else None

def finish(book,name,result):
 now=int(book.clock());deadline=csfloat_read.admission(result.get('headers'),now)
 with book.lock,book.connect(True)as c:
  c.execute("UPDATE provider SET next_allowed=MAX(next_allowed,?) WHERE id='csfloat'",(deadline,))
  if result.get('terminal'):
   c.execute("UPDATE provider SET enabled=0 WHERE id='csfloat'")
  if result.get('status')==200 and result.get('rows'):
   requested=c.execute("SELECT value FROM meta WHERE key='csfloat_requested'").fetchone()
   if requested:
    queue=[n for n in json.loads(requested[0])if n!=name];c.execute("UPDATE meta SET value=?WHERE key='csfloat_requested'",(json.dumps(queue),))
   c.execute("UPDATE provider SET note=? WHERE id='csfloat'",('Authenticated buy-now listing capture; individual items, up to50 returned listings. Not full-market coverage or guaranteed sale proceeds.',))
   previous=c.execute("SELECT value FROM meta WHERE key='csfloat_control'").fetchone();previous=json.loads(previous[0])if previous else{}
   saved={'qualified':True,'lastItem':name,'lastCaptureAt':result['receivedAtUs']//1000000,'rateHeaders':result.get('headers',{}),'successfulRequests':previous.get('successfulRequests',0)+1}
   c.execute("INSERT INTO meta VALUES('csfloat_control',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(json.dumps(saved),))
