"""Current decision support from normalized quotes and real bounded observations.

No network, migrations, raw archive or confidence/return imputation on reads.
"""
from collections import Counter,OrderedDict,defaultdict,deque
from contextlib import closing
from decimal import Decimal
import html,math,statistics,threading,time
import quote_book as qb

RULES={'referenceDays':7,'minimumCompletedSales':5,'comparisonThresholdPct':10,'scope':'Review candidates, not trade instructions or guaranteed proceeds'}

def observation(rows):
 if not rows:return {'points':0,'spanHours':0,'changePct':None,'volatilityPct':None}
 returns=[(b['price_mills']/a['price_mills']-1)*100 for a,b in zip(rows,rows[1:])if b['bucket']==a['bucket']+1]
 return {'points':len(rows),'oldestAt':rows[0]['as_of'],'newestAt':rows[-1]['as_of'],'spanHours':round((rows[-1]['as_of']-rows[0]['as_of'])/3600,1),
  'changePct':round((rows[-1]['price_mills']/rows[0]['price_mills']-1)*100,2)if len(rows)>=2 else None,
  'volatilityPct':round(statistics.pstdev(returns),3)if len(returns)>=6 else None,'volatilityIntervals':len(returns),'volatilityBasis':'Unannualized standard deviation of actual consecutive hourly listing changes; at least6 intervals required'}

class Analysis:
 def __init__(self,app,pipeline):self.app=app;self.pipeline=pipeline;self.cache=OrderedDict();self.lock=threading.RLock()
 def read(self,portfolio_id=None):
  for attempt in range(2):
   value=self.pipeline.value(portfolio_id);now=int(self.pipeline.clock());key=(value['portfolioId'],value['holdingsFingerprint'],value['revision'],now//15)
   with self.lock:
    if key in self.cache:return self.cache[key]
   result=self.build(value,now)
   if self.pipeline.book.revision()!=value['revision']:continue
   with self.lock:
    self.cache[key]=result
    while len(self.cache)>4:self.cache.popitem(last=False)
   return result
  raise ValueError('Prices changed during analysis; retry the current read')

 def build(self,value,now):
  names=list(dict.fromkeys(i['marketHashName']for i in value['items']if i['marketHashName']));refs={};hist=defaultdict(list);offers=defaultdict(list)
  with self.pipeline.book.connect()as c:
   if self.pipeline.book.revision(c)!=value['revision']:return {'ok':False}
   for start in range(0,len(names),500):
    chunk=names[start:start+500];marks=','.join('?'for _ in chunk)
    for r in c.execute('SELECT s.market_hash_name,r.* FROM cash_sales_reference r JOIN subject s ON s.id=r.subject WHERE days=7 AND s.market_hash_name IN('+marks+')',chunk):refs[r['market_hash_name']]=dict(r)
    for r in c.execute('SELECT s.market_hash_name,h.* FROM cash_observed_close h JOIN subject s ON s.id=h.subject WHERE grain=3600 AND bucket>=? AND bucket<? AND s.market_hash_name IN('+marks+') ORDER BY bucket', [now//3600-47,now//3600]+chunk):hist[r['market_hash_name']].append(dict(r))
    for r in c.execute('''SELECT s.market_hash_name,q.provider,q.price_mills,q.quantity,p.ttl,cap.received_us,cap.provider_time,cap.clock_semantics
      FROM subject s JOIN quote q ON q.subject=s.id JOIN provider p ON p.id=q.provider
      JOIN capture cap ON cap.id=CASE WHEN q.active=1 THEN p.last_capture ELSE q.capture END
      WHERE q.active<>0 AND q.state='priced' AND q.kind='cash_ask' AND q.currency='USD' AND s.market_hash_name IN('''+marks+')',chunk):
     at=r['received_us']//1000000;clock=r['provider_time']if r['clock_semantics']=='provider_snapshot_time'else at
     if r['price_mills']and r['quantity']and at<=now+60 and now<min(at,clock or at)+r['ttl']:
      offers[r['market_hash_name']].append({'provider':r['provider'],'askUsd':r['price_mills']/1000,'listings':r['quantity'],'capturedAt':at})
  metadata={};imports=defaultdict(deque)
  with closing(self.app.steam_ask_db())as c:
   deadline=time.monotonic()+3;c.set_progress_handler(lambda:int(time.monotonic()>deadline),10000)
   portfolio=dict(c.execute('SELECT id,label,imported_at FROM portfolios WHERE id=?',(value['portfolioId'],)).fetchone())
   for r in c.execute('SELECT name,qty,category,group_name,import_curr,import_unit_price FROM portfolio_items WHERE portfolio_id=? ORDER BY sort_key',(value['portfolioId'],)):
    imports[(r['name'],r['qty'])].append(dict(r))
   for start in range(0,len(names),500):
    chunk=names[start:start+500]
    for r in c.execute('SELECT name,item_type,collection,weapon,rarity FROM item_catalog WHERE name IN('+','.join('?'for _ in chunk)+')',chunk):metadata[r['name']]=dict(r)
  items=[];counts=Counter();groups={key:{}for key in('category','collection','type')}
  for q in value['items']:
   name=q['marketHashName'];saved=imports[(q['displayName'],q['qty'])].popleft()if imports[(q['displayName'],q['qty'])]else{};meta=metadata.get(name,{})
   reference=refs.get(name);valid_ref=bool(reference and reference['captured_us']//1000000<=now+60 and now<reference['captured_us']//1000000+3600 and reference['median_mills']and reference['sales_count']>0)
   sales={'medianUsd':reference['median_mills']/1000,'sales':reference['sales_count'],'capturedAt':reference['captured_us']//1000000,'days':7}if valid_ref else None
   comparison=round((q['unitPriceUsd']/sales['medianUsd']-1)*100,2)if q['state']=='priced'and sales else None
   row=dict(name=q['displayName'],marketHashName=name,qty=q['qty'],category=saved.get('group_name')or'Other',type=meta.get('item_type')or saved.get('category')or'Other',collection=meta.get('collection')or'Unclassified',
    unitPriceUsd=q['unitPriceUsd'],valueUsd=q['valueUsd'],state=q['state'],quality=q.get('qualityState'),source=q.get('selectedProvider'),capturedAt=q.get('capturedAt'),expiresAt=q.get('expiresAt'),
    reason=q.get('reason'),offers=sorted(offers.get(name,[]),key=lambda o:o['askUsd']),reference=sales,referenceDifferencePct=comparison,observed=observation(hist.get(name,[])),
    importedBenchmark={'valueUsd':saved.get('import_curr'),'importedAt':portfolio['imported_at'],'comparableToCash':False},acquisitionCostUsd=None,referencePrice=q.get('referencePrice'))
   if q['state']!='priced':row.update(decision='data_needed',explanation=(q.get('reason')or'Current comparable cash listing unavailable').replace('_',' '))
   elif not sales or sales['sales']<RULES['minimumCompletedSales']:row.update(decision='evidence_needed',explanation='Current ask available; fewer than5 recent completed sales. No directional signal assigned.')
   elif comparison<=-RULES['comparisonThresholdPct']:row.update(decision='buy_watch',explanation=f"Qualifying ask {abs(comparison):.1f}% below the7day completed-sale median ({sales['sales']} sales). Review fees, condition and available depth before buying.")
   elif comparison>=RULES['comparisonThresholdPct']:row.update(decision='sell_review',explanation=f"Qualifying ask {comparison:.1f}% above the7day completed-sale median ({sales['sales']} sales). Check an actual bid and seller fees; this is not executable sale value.")
   else:row.update(decision='monitor',explanation='Current ask is within10% of the7day completed-sale median. No directional listing signal.')
   counts[row['decision']]+=1
   for dimension,buckets in groups.items():
    label=row[dimension];g=buckets.setdefault(label,{'name':label,'holdingRows':0,'pricedRows':0,'coveredValueUsd':0,'_cents':0,'buyWatch':0,'sellReview':0})
    g['holdingRows']+=1
    if q['state']=='priced':g['pricedRows']+=1;g['_cents']+=round(q['valueUsd']*100)
    if row['decision']=='buy_watch':g['buyWatch']+=1
    if row['decision']=='sell_review':g['sellReview']+=1
   items.append(row)
  for dimension,buckets in groups.items():
   for g in buckets.values():g['coveredValueUsd']=g.pop('_cents')/100;g['sharePct']=round(g['coveredValueUsd']/value['totals']['coveredValueUsd']*100,2)if value['totals']['coveredValueUsd']else None
   groups[dimension]=sorted(buckets.values(),key=lambda g:-g['coveredValueUsd'])
  series=self.curve(items,hist)
  signals={key:sorted([i for i in items if i['decision']==key],key=lambda i:-abs(i['referenceDifferencePct']or 0))[:20]for key in('buy_watch','sell_review','monitor','data_needed','evidence_needed')}
  return {'ok':True,'generatedAt':now,'portfolio':portfolio,'portfolioId':value['portfolioId'],'holdingsFingerprint':value['holdingsFingerprint'],'currentSourceRevision':value['revision'],'currency':'USD','basis':'qualified_cash_listing_estimate',
   'coverage':dict(value['coverage'],decisionCounts=dict(counts),referenceRows=sum(bool(i['reference'])for i in items),observedRows=sum(i['observed']['points']>=2 for i in items),volatilityRows=sum(i['observed']['volatilityPct']is not None for i in items)),
   'totals':value['totals'],'items':items,'groups':groups,'signals':signals,'rules':RULES,'series':series,
   'limitations':{'executableBidsAvailable':False,'sellerFeesConfigured':False,'acquisitionCostsAvailable':False,'periodReturnsAvailable':False,'reason':'No executable bids, fee basis or explicit acquisition-cost records are present. Imported snapshot values are preserved benchmarks, not cash cost basis. Longer returns need genuine dated observations.'}}

 def group(self,portfolio_id,dimension,group):
  dimension={'source':'collection','item_type':'type'}.get(dimension,dimension)
  if dimension not in('category','collection','type')or not group:raise ValueError('Choose a category, collection or type group')
  data=self.read(portfolio_id);items=[i for i in data['items']if i[dimension]==group];names=list(dict.fromkeys(i['marketHashName']for i in items if i['marketHashName']));hist=defaultdict(list);now=int(self.pipeline.clock())
  with self.pipeline.book.connect()as c:
   for start in range(0,len(names),500):
    chunk=names[start:start+500]
    for row in c.execute('SELECT s.market_hash_name,h.* FROM cash_observed_close h JOIN subject s ON s.id=h.subject WHERE grain=3600 AND bucket>=? AND bucket<? AND s.market_hash_name IN('+','.join('?'for _ in chunk)+') ORDER BY bucket',[now//3600-47,now//3600]+chunk):hist[row['market_hash_name']].append(dict(row))
  series=self.curve(items,hist)
  return {'ok':True,'scope':'portfolio','dimension':dimension,'group':group,'points':series['points'],'series':series,'coverage':{'holdingRows':len(items),'cohortRows':series['cohortRows']},'items':[{'name':i['name'],'qty':i['qty'],'currentPriceUsd':i['unitPriceUsd'],'points':hist.get(i['marketHashName'],[])}for i in items[:10]],'requestedPeriodReturnsAvailable':False}

 @staticmethod
 def curve(items,hist):
  priced=[i for i in items if i['state']=='priced'and i['qty']and i['qty']>0];buckets=sorted({p['bucket']for i in priced for p in hist.get(i['marketHashName'],[])})[-48:]
  if len(buckets)<2:return {'points':[],'cohortRows':0,'reason':'At least two genuine completed hourly observations are needed'}
  byname={name:{p['bucket']:p for p in rows}for name,rows in hist.items()};cohort=[i for i in priced if all(b in byname.get(i['marketHashName'],{})for b in buckets)]
  if not cohort:return {'points':[],'cohortRows':0,'reason':'No fixed cohort has observations at every displayed hour; gaps are not filled with current prices'}
  points=[]
  for b in buckets:
   total=sum(Decimal(str(qb.cents(Decimal(byname[i['marketHashName']][b]['price_mills'])/1000*Decimal(str(i['qty'])))))for i in cohort)
   points.append({'at':(b+1)*3600,'valueUsd':qb.cents(total),'observationOldestAt':min(byname[i['marketHashName']][b]['as_of']for i in cohort)})
  return {'points':points,'cohortRows':len(cohort),'currentPricedRows':len(priced),'spanHours':(points[-1]['at']-points[0]['at'])/3600,
   'changePct':round((points[-1]['valueUsd']/points[0]['valueUsd']-1)*100,2)if points[0]['valueUsd']else None,
   'basis':'Fixed observed subset with current quantities; completed UTC hourly closes only. Not full historical portfolio value or a cost-basis return.'}

def render_report(data):
 esc=lambda v:html.escape(str(v if v is not None else'Unavailable'),quote=True)
 money=lambda v:'Unavailable'if v is None else'$'+format(v,',.2f')
 def reference(row):
  r=row.get('referencePrice')
  return 'Unavailable'if not r else esc(r['label'])+' '+money(r['priceUsd'])+' · '+esc(r['provider'])+' · '+time.strftime('%Y-%m-%d %H:%M UTC',time.gmtime(r['capturedAt']))+' · excluded from cash'
 def table(rows):
  return '<table><thead><tr><th>Item</th><th>Qty</th><th>Cash ask</th><th>Covered holding</th><th>Other price reference (excluded)</th><th>Review</th><th>Evidence / missing input</th></tr></thead><tbody>'+''.join('<tr><td>'+esc(r['name'])+'</td><td>'+esc(r['qty'])+'</td><td>'+money(r['unitPriceUsd'])+'</td><td>'+money(r['valueUsd'])+'</td><td>'+reference(r)+'</td><td>'+esc(r['decision'].replace('_',' '))+'</td><td>'+esc(r['explanation'])+'</td></tr>'for r in rows)+'</tbody></table>'
 return '<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>CS2 current decision report</title><style>body{background:#0b0e17;color:#e6e8ef;font:14px system-ui;margin:24px}a{color:#f0a500}table{width:100%;border-collapse:collapse}td,th{padding:10px;border-bottom:1px solid #293047;text-align:left}p{color:#a9b0c5}section{margin:24px 0}</style></head><body><a href="/">Back to dashboard</a><h1>Current cash-price decision report</h1><p>'+esc(data['portfolio']['label'])+' · '+str(data['coverage']['pricedRows'])+'/'+str(data['coverage']['holdingRows'])+' rows priced · covered listing estimate '+money(data['totals']['coveredValueUsd'])+'</p><p>Full value: '+money(data['totals']['fullValueUsd'])+'. Listing asks are not bids or guaranteed sale proceeds. Review signals compare a qualified current ask with genuine completed-sale summaries; no forecast or confidence score is invented.</p><h2>Buy / watch review</h2>'+table(data['signals']['buy_watch'])+'<h2>Sell review</h2>'+table(data['signals']['sell_review'])+'<h2>All held items</h2>'+table(data['items'])+'<p>'+esc(data['limitations']['reason'])+'</p></body></html>'
