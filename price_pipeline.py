"""Shared provider scheduling and one current-price interpretation for all views."""
from collections import Counter,OrderedDict
from decimal import Decimal
import json
import shutil
import threading
import time
import uuid
import price_transport
import quote_book
import price_features
import csfloat_queue
import csfloat_read

PROVIDERS=[
 {'id':'skinport_live','kind':'cash_ask','cadence':300,'ttl':600,'enabled':True,'note':'Public USD listing snapshot; provider cache5minutes'},
 {'id':'skinport_sales','kind':'reference','cadence':1800,'ttl':3600,'enabled':True,'note':'Public completed-sale window summaries; reference evidence, never a current ask'},
 {'id':'market_csgo_live','kind':'cash_ask','cadence':300,'ttl':600,'enabled':True,'note':'Public best-offer USD snapshot; new-source valuations require another cash provider'},
 {'id':'waxpeer_live','kind':'cash_ask','cadence':120,'ttl':600,'enabled':True,'note':'Public grouped purchase offers including trade lock; delivery may be delayed. Capture age only.'},
 {'id':'steam','kind':'wallet_ask','cadence':60,'ttl':600,'enabled':True,'note':'Explicit on-demand only;60seconds is local pacing, not a documented Valve quota'},
 {'id':'csfloat','kind':'cash_ask','cadence':60,'ttl':600,'enabled':False,'note':csfloat_queue.NOTE},
]

class Pipeline:
 def __init__(self,book,holdings,capacity,allow_network=False,clock=time.time):
  self.book=book;self.holdings=holdings;self.capacity=capacity;self.allow_network=allow_network;self.clock=clock
  self.stop_event=threading.Event();self.wake=threading.Event();self.thread=None;self.lock=threading.RLock()
  self.worker=None;self.job=None;self.portfolio_id=None;self.value_cache=OrderedDict();self.steam_lock=threading.Lock()
  self.start_after=int(clock())+60
  self.features=price_features.Features(book)if book.has_references else None;self.history_error=None

 def start(self):
  if not self.allow_network:return False
  with self.lock:
   if self.thread and self.thread.is_alive():return False
   self.thread=threading.Thread(target=self._loop,name='current-provider-coordinator',daemon=True);self.thread.start();return True

 def stop(self,timeout=40):
  self.stop_event.set();self.wake.set()
  deadline=time.monotonic()+max(0,float(timeout))
  if self.thread:self.thread.join(max(0,deadline-time.monotonic()))
  if self.worker:self.worker.join(max(0,deadline-time.monotonic()))
  return not(self.thread and self.thread.is_alive()or self.worker and self.worker.is_alive())

 def _loop(self):
  while not self.stop_event.is_set():
   try:
    if int(self.clock())>=self.start_after:
     with self.lock:busy=self.worker and self.worker.is_alive()
     if not busy:
      due=[p for p in self.book.providers()if p['enabled']and p['kind']in('cash_ask','reference')and p['next_allowed']<=int(self.clock())]
      if due:self._launch()
     source=next(p for p in self.book.providers()if p['id']=='steam')
     if source['enabled']and source['next_allowed']<=int(self.clock())and self.steam_lock.acquire(blocking=False):
      try:
       name=self.book.next_steam()
       if name:
        outcome=self._fetch('steam',name)
        if outcome.get('action')=='completed':self.book.complete_steam(name)
      finally:self.steam_lock.release()
    if self.features:self.features.synchronize()
    with self.lock:self.coordinator_error=None
   except Exception as error:
    with self.lock:self.coordinator_error=str(error)[:300]
   self.wake.wait(5);self.wake.clear()

 def status(self,ignored=None):
  providers,revision=self.book.status_snapshot();cash=[p for p in providers if p['kind']=='cash_ask']
  last=max([p for p in cash if p['last_received_us']],key=lambda p:p['last_received_us'],default=None)
  receipt={'receivedAtUs':last['last_received_us'],'bodySHA256':last['body_sha'],'provider':last['id'],'requestUrl':last['request_url']}if last else None
  with self.lock:job=dict(self.job)if self.job else None;running=bool(self.worker and self.worker.is_alive())
  deadlines=[p['next_allowed']for p in cash if p['enabled']];next_at=min(deadlines,default=int(self.clock()))
  return {'provider':'canonical_cash','currency':'USD','job':job,'workerRunning':running,
   'cashSourceRevision':revision,'nextAllowedAt':next_at,'retryAt':next_at if next_at>int(self.clock())else None,
   'holdingsIdentityRevision':self.holdings_revision()if hasattr(self,'holdings_revision')else None,
   'lastSuccessfulFetch':receipt,'lastError':getattr(self,'coordinator_error',None),
   'refreshRequest':self.features.request()if self.features else None,'history':dict(self.features.history_status(),controllerError=self.history_error)if self.features else None,
   'csfloat':csfloat_queue.state(self.book),
   'cooldownReason':'per_provider_pacing_and_backoff','freshnessSeconds':600,
   'sources':[dict(provider=p['id'],enabled=bool(p['enabled']),kind=p['kind'],nextAllowedAt=p['next_allowed'],
    capturedAt=p['last_received_us']//1000000 if p['last_received_us']else None,lastStatus=p['last_status'],
    lastError=p['last_error'],note=p['note'],cadenceSeconds=p['cadence'],ttlSeconds=p['ttl'],attempts=p['attempts'],
    publicationSeconds=p['last_seconds'],changedRows=p['last_changed'],currentRows=p['row_count'],providerAsOf=p['provider_time'],clockSemantics=p['clock_semantics'])for p in providers],
   'storage':{'authority':'normalized_current_quotes_v2','legacyWrites':False,'historicalMarketDataIncluded':False,'rawPayloadRows':0}}

 def cadence_status(self,saved=None):
  status=saved or self.status();return {'enabled':bool(self.thread and self.thread.is_alive()),'scope':'shared_public_bulk_feeds',
   'portfolioId':self.portfolio_id,'intervalSeconds':120,'pollSeconds':5,'nextAllowedAt':status['nextAllowedAt'],
   'state':'worker_active'if status['workerRunning']else'provider_or_cadence_wait','lastError':status['lastError']}

 def action(self,request,**ignored):
  if not isinstance(request,dict)or set(request)-{'provider','portfolio_id','action','job_id','market_hash_name'}or request.get('provider')not in ('skinport_live','canonical_cash','csfloat')or request.get('action','start')not in ('start','resume'):raise ValueError('One current-price refresh request required')
  if request['provider']=='csfloat':
   if not self.allow_network:raise ValueError('Provider calls disabled in offline qualification')
   result=csfloat_queue.request_item(self.book,request.get('market_hash_name'));self.wake.set();return result
  frozen=self.holdings(request.get('portfolio_id'));self.portfolio_id=frozen['portfolioId']
  if hasattr(self,'all_holdings'):self.book.set_held(self.all_holdings())
  else:self.book.set_held(frozen['hashes'])
  if not self.allow_network:raise ValueError('Provider calls disabled in offline qualification')
  if not self.features:raise ValueError('Current refresh features are not initialized')
  request,reused=self.features.enqueue(frozen);self.wake.set()
  return {'accepted':True,'reused':reused,'provider':'canonical_cash','refreshRequest':request,'reason':'queued_with_provider_pacing','job':self.job}

 def _launch(self,frozen=None):
  with self.lock:
   if self.worker and self.worker.is_alive():return {'accepted':False,'reused':True,'provider':'canonical_cash','job':self.job}
   self.capacity()
   if frozen is None:
    try:frozen=self.holdings(self.portfolio_id)
    except Exception:frozen=None
   self.job={'id':uuid.uuid4().hex,'portfolioId':frozen['portfolioId']if frozen else None,
    'holdingsFingerprint':frozen['holdingsFingerprint']if frozen else None,'status':'queued','phase':'provider_admission',
    'startedAt':int(self.clock()),'createdAt':int(self.clock()),'updatedAt':int(self.clock()),'receivedAt':None}
   self.worker=threading.Thread(target=self._refresh,name='current-cash-refresh',daemon=True);self.worker.start()
   return {'accepted':True,'reused':False,'provider':'canonical_cash','job':dict(self.job)}

 def _fetch(self,source,name=None):
  if not self.allow_network:raise ValueError('Provider calls disabled in offline qualification')
  if source=='csfloat':
   name=csfloat_queue.next_item(self.book)
   if not name:
    with self.book.lock,self.book.connect(True)as c:c.execute("UPDATE provider SET next_allowed=MAX(next_allowed,?) WHERE id='csfloat'",(int(self.clock())+60,))
    return {'action':'deferred','reason':'all_watched_csfloat_quotes_within_age_policy'}
  self.capacity();claim=self.book.claim(source)
  if claim['action']!='request':return claim
  try:
   held=None
   if source=='skinport_sales':
    if hasattr(self,'all_holdings'):self.book.set_held(self.all_holdings())
    with self.book.connect()as c:held=[r[0]for r in c.execute('SELECT s.market_hash_name FROM demand d JOIN subject s ON s.id=d.subject WHERE d.held=1')]
   result=csfloat_read.fetch(name)if source=='csfloat'else price_transport.fetch(source,name,held_names=held)
   if result.get('status')!=200:
    until=self.book.fail(claim,result.get('status'),result.get('error')or('HTTP'+str(result.get('status'))),(result.get('headers')or{}).get('retry-after'))
    if source=='csfloat':csfloat_queue.finish(self.book,name,result)
    return {'action':'failed','status':result.get('status'),'retryAt':until}
   if source!='skinport_sales'and not result.get('rows'):raise ValueError('Qualified normalized rows missing')
   if source=='market_csgo_live'and (not result.get('providerTime')or int(self.clock())-result['providerTime']>=600):raise ValueError('Provider snapshot is stale; old quotes were not freshened')
   self.capacity();publication=self.features.publish_sales(result,claim,held)if source=='skinport_sales'else self.book.publish(source,result,result['rows'],claim,item_scope=name if source=='csfloat'else None)
   if source=='csfloat':csfloat_queue.finish(self.book,name,result)
   with self.lock:self.value_cache.clear()
   if self.features and source!='steam':
    try:self.features.record();self.history_error=None
    except Exception as error:self.history_error=str(error)[:300]
   return {'action':'completed','receivedAt':result['receivedAtUs']//1000000,**publication}
  except Exception as error:
   safe_error='CSFloat private credential or bounded acquisition unavailable'if source=='csfloat'else str(error)
   self.book.fail(claim,None,safe_error)
   if source=='csfloat'and isinstance(error,csfloat_read.CredentialUnavailable):csfloat_queue.finish(self.book,name,{'terminal':True})
   return {'action':'failed','error':safe_error[:300]}

 def _refresh(self):
  outcomes={}
  try:
   for p in sorted(self.book.providers(),key=lambda p:({'skinport_sales':-1,'skinport_live':0,'market_csgo_live':1,'waxpeer_live':2}.get(p['id'],9),p['id'])):
    if self.stop_event.is_set():break
    if p['enabled']and p['kind']in('cash_ask','reference'):
     with self.lock:self.job.update(status='fetching',phase=p['id'],updatedAt=int(self.clock()))
     outcomes[p['id']]=self._fetch(p['id'])
   with self.lock:
    self.job.update(status='completed'if any(r.get('action')=='completed'for r in outcomes.values())else'paused',
      phase='completed',updatedAt=int(self.clock()),receivedAt=max([r.get('receivedAt',0)for r in outcomes.values()]+[0])or None,sources=outcomes)
  except Exception as error:
   with self.lock:self.job.update(status='failed',phase='failed',error=str(error)[:300]);self.coordinator_error=str(error)[:300]

 def prices(self,names):
  if not isinstance(names,list)or len(names)>500:raise ValueError('At most500 exact market hashes per quote request')
  return self.book.prices(names)

 def value(self,portfolio_id):
  frozen=self.holdings(portfolio_id);now=int(self.clock());revision=self.book.revision()
  key=(frozen['portfolioId'],frozen['holdingsFingerprint'],revision)
  with self.lock:
   old=self.value_cache.get(key)
   if old and now<old[0]:self.value_cache.move_to_end(key);return old[1]
  result=dict(self.book.prices(frozen['hashes']));quotes={q['marketHashName']:q for q in result.pop('items')};items=[]
  counts={k:0 for k in ('pricedRows','freshRows','staleRows','missingRows','noListingsRows','ambiguousRows','invalidRows','unresolvedRows','zeroQuantityRows','thinRows','uncorroboratedRows','outlierRows','transactionSupportedRows')}
  total=Decimal(0)
  for row in frozen['rows']:
   q=dict(quotes.get(row['marketHashName'])or quote_book.select(row['marketHashName'],[],now))
   state='invalid'if row['quantityIssue']else'zero_quantity'if row['qty']==0 else'unresolved'if row['identityIssue']or not row['marketHashName']else q['state']
   amount=None;unit=None
   if state=='priced':
    unit=q['canonicalPriceUsd'];amount=quote_book.cents(Decimal(str(unit))*Decimal(row['quantityKey']));total+=Decimal(str(amount));counts['freshRows']+=1
   elif state=='zero_quantity':amount=0
   counts[{'priced':'pricedRows','stale':'staleRows','no_listings':'noListingsRows','ambiguous':'ambiguousRows','invalid':'invalidRows','unresolved':'unresolvedRows','zero_quantity':'zeroQuantityRows','missing':'missingRows'}[state]]+=1
   if q.get('qualityState')=='thin':counts['thinRows']+=1
   if q.get('qualityState')=='uncorroborated':counts['uncorroboratedRows']+=1
   if q.get('qualityState')=='outlier':counts['outlierRows']+=1
   if q.get('qualityState')=='transaction_supported':counts['transactionSupportedRows']+=1
   q.update(displayName=row['displayName'],qty=row['qty'],marketHashName=row['marketHashName'],state=state,
     unitPriceUsd=unit,valueUsd=amount,reason=row['quantityIssue']or row['identityIssue']or q.get('reason'))
   if state!='priced':q.update(canonicalPriceUsd=None,status='stale'if state=='stale'else'ambiguous'if state=='ambiguous'else'unpriced',freshnessState='stale'if state=='stale'else'missing')
   items.append(q)
  full=counts['pricedRows']+counts['zeroQuantityRows']==len(items)
  result.update(portfolioId=frozen['portfolioId'],holdingsFingerprint=frozen['holdingsFingerprint'],displayHoldings=frozen['displayHoldings'],items=items,
   coverage=dict(counts,referencePriceRows=sum(bool(q.get('referencePrice'))for q in items),withoutAnyPriceEvidenceRows=sum(q['state']!='priced'and not q.get('referencePrice')for q in items),holdingRows=len(items),distinctHashes=len(frozen['hashes']),fullCoverage=full,truncated=False),
   totals={'coveredValueUsd':quote_book.cents(total),'fullValueUsd':quote_book.cents(total)if full else None,
    'freshCoveredValueUsd':quote_book.cents(total),'freshFullValueUsd':quote_book.cents(total)if full else None,'realizableSaleValueUsd':None},
   quoteScope='One-unit gross cash listing estimate; not a bid or full-depth liquidation value')
  expiry=min([q['expiresAt']for q in items if q.get('expiresAt')and q['expiresAt']>now]+[now+15])
  with self.lock:
   self.value_cache[key]=(expiry,result);self.value_cache.move_to_end(key)
   while len(self.value_cache)>4:self.value_cache.popitem(last=False)
  return result

 def steam_value(self,portfolio_id=None,fresh_seconds=600,summary=False):
  """Saved wallet asks from the SAME ledger; no old observation-table reads."""
  frozen=self.holdings(portfolio_id);now=int(self.clock())
  quotes={q['marketHashName']:q for q in self.book.prices(frozen['hashes'],kind='wallet_ask')['items']}
  observed=stale=missing=unresolved=0;known=covered_qty=Decimal(0);total=Decimal(0);captures=[];items=[]
  for row in frozen['rows']:
   if not row['quantityIssue']:known+=Decimal(row['quantityKey'])
   q=quotes.get(row['marketHashName'],{});amount=None
   if row['quantityIssue']or row['identityIssue']or not row['marketHashName']:state='unresolved';unresolved+=1
   elif row['qty']==0:state='zero_quantity';observed+=1;amount=0
   elif q.get('state')=='priced':
    state='priced';observed+=1;covered_qty+=Decimal(row['quantityKey']);captures.append(q['capturedAt'])
    amount=quote_book.cents(Decimal(str(q['canonicalPriceUsd']))*Decimal(row['quantityKey']));total+=Decimal(str(amount))
   elif q.get('state')=='stale':state='stale';stale+=1
   else:state='missing';missing+=1
   if not summary:items.append(dict(q,displayName=row['displayName'],qty=row['qty'],state=state,valueUsd=amount))
  complete=observed==len(frozen['rows'])and bool(observed);subtotal=quote_book.cents(total)
  value={'scope':'portfolio:'+frozen['portfolioId'],'portfolioId':frozen['portfolioId'],'lane':'steam_lowest_ask','source':'steam_priceoverview','currency':'USD',
   'holdingsFingerprint':frozen['holdingsFingerprint'],'summaryOnly':bool(summary),'valuationAsOf':now,'jobId':None,
   'jobStateAtValuation':None,'jobUpdatedAtValuation':None,'holdingsChangedSinceJob':None,
   'acquisitionFreshnessSeconds':600,'oldestFetchedAt':min(captures,default=None),'newestFetchedAt':max(captures,default=None),
   'coveredSubtotalUsd':subtotal,'freshCoveredSubtotalUsd':subtotal,'totalValueUsd':subtotal if complete else None,'freshTotalValueUsd':subtotal if complete else None,
   'coverage':{'totalItems':len(frozen['rows']),'observedItems':observed,'freshItems':observed,'staleItems':stale,'missingItems':missing,
    'unresolvedItems':unresolved,'knownQuantity':float(known),'totalQuantity':float(known)if not any(r['quantityIssue']for r in frozen['rows'])else None,
    'observedQuantity':float(covered_qty),'complete':complete,'freshComplete':complete},
   'quoteScope':'Current cached Steam wallet asks; expired or missing asks never filled with cash prices'}
  if not summary:value['items']=items
  return value

 def steam_ask(self,name):
  def saved():
   q=self.book.prices([name],kind='wallet_ask')['items'][0]
   return q if q['state']=='priced'else None
  q=saved();cached=bool(q)
  if not q:
   if not self.allow_network:return {'success':False,'status':'offline','lowestAsk':None}
   self.book.request_steam(name);self.wake.set()
   if not self.steam_lock.acquire(blocking=False):return {'success':False,'status':'deferred','reason':'single_flight','lowestAsk':None}
   try:
    q=saved()
    if q:cached=True
    else:
     if self.book.next_steam()!=name:return {'success':False,'status':'deferred','reason':'held_or_older_explicit_request_priority','lowestAsk':None}
     outcome=self._fetch('steam',name);q=saved()
     if q:self.book.complete_steam(name)
     if not q:return {'success':False,'status':'deferred'if outcome.get('action')in('deferred','reused')else'unavailable','lowestAsk':None,**outcome}
   finally:self.steam_lock.release()
  return {'success':True,'status':'cached'if cached else'fetched','cached':cached,'provider':'steam','source':'steam_priceoverview','marketHashName':name,
   'appid':730,'currency':'USD','priceBasis':'lowest_ask','lowestAsk':q['canonicalPriceUsd'],'lowestAskCents':round(q['canonicalPriceUsd']*100),
   'rawLowestPrice':'$'+format(q['canonicalPriceUsd'],'.2f'),'fetchedAt':q['capturedAt'],'expiresAt':q['expiresAt'],
   'marketObservedAt':None,'durable':True,'receiptId':q.get('receiptSHA256')or q.get('receiptBodySHA256')}
