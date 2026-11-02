"""Compatibility binding: existing UI, one normalized quote ledger and coordinator."""
from contextlib import closing
from collections import OrderedDict
from decimal import Decimal
import json
import threading
import time
import urllib.parse
import quote_book
import price_pipeline
import current_analysis

def install(app,path,held,allow_network):
 book=quote_book.Book(path)
 if not book.has_references:raise ValueError('Explicit small current-features migration is required before this source activation')
 cache=app.holdings_cache.HoldingsCache(lambda:app.steam_ask_db(shared_read=True),held)
 helper=app.cash_prices.CashPrices(None,cache.read,None,app.cash_price_capacity,threading.RLock())
 frozen_cache=OrderedDict();frozen_lock=threading.RLock()
 def frozen(portfolio_id):
  if not portfolio_id:
   with closing(app.steam_ask_db())as conn:
    row=app.latest_portfolio(conn)
   if not row:raise app.cash_prices.CashNotFound('No saved portfolio')
   portfolio_id=row['portfolio_id']
  # The existing read-only monitor observes commits outside a transaction. Reuse
  # identity resolution AND its immutable fingerprint until that epoch changes.
  with frozen_lock:
   before=cache.identity_revision();key=(portfolio_id,before)
   if key in frozen_cache:
    result=frozen_cache[key]
   else:
    result=helper._holdings(portfolio_id)
   if before!=cache.identity_revision():
    frozen_cache.clear();raise app.holdings_cache.HoldingsChanged('Holdings changed during valuation; retry the saved read')
   frozen_cache[key]=result;frozen_cache.move_to_end(key)
   while len(frozen_cache)>4:frozen_cache.popitem(last=False)
   return result
 pipeline=price_pipeline.Pipeline(book,frozen,app.cash_price_capacity,allow_network)
 try:pipeline.portfolio_id=frozen(None)['portfolioId']
 except app.cash_prices.CashNotFound:pipeline.portfolio_id=None
 pipeline.holdings_revision=cache.identity_revision
 pipeline.all_holdings=lambda:sorted({name for portfolio in app.get_portfolios()for name in frozen(portfolio['id'])['hashes']})
 analysis=current_analysis.Analysis(app,pipeline)
 app.CURRENT_ANALYSIS=analysis
 book.set_held(sorted({name for portfolio in app.get_portfolios()for name in frozen(portfolio['id'])['hashes']}))
 app.cash_price_service=lambda:pipeline
 app.canonical_cash_service=lambda:pipeline
 app.CASH_PRICE_HOLDINGS_CACHE=cache
 class Cadence:
  start=pipeline.start
  stop=pipeline.stop
  status=pipeline.cadence_status
 app.CASH_PRICE_CADENCE=Cadence()
 app.PRICE_PIPELINE=pipeline
 app.durable_steam_current_ask=lambda name,claim=None:pipeline.steam_ask(name)
 def steam_status(job_id=None):
  source=next(p for p in book.providers()if p['id']=='steam');now=int(time.time())
  return {'provider':'steam','source':'steam_priceoverview','job':None,'workerRunning':False,'durable':True,'currency':'USD',
   'cooldown':{'retryAt':source['next_allowed']if source['next_allowed']>now else None,'recoveryRequired':bool(source['lease'])},
   'governor':{'operatorPaused':not bool(source['enabled']),'bulkAcquisitionPaused':True,'pauseReason':'operator_paused'if not source['enabled']else None,'onDemandAvailable':bool(source['enabled']),
    'nextAllowedAt':source['next_allowed'],'retryAt':source['next_allowed']if source['next_allowed']>now else None,'sharedHistoryAdmission':True,
    'recoveryRequired':bool(source['lease']),'lastError':source['last_error']}}
 app.steam_ask_job_response=steam_status
 app.steam_ask_value_response=pipeline.steam_value
 app.steam_governor_status=lambda:steam_status()['governor']
 app.steam_ask_reconcile_startup=lambda:None
 def steam_control(data):
  if not isinstance(data,dict)or set(data)-{'action'}or data.get('action')not in('pause','resume_on_demand'):
   raise ValueError('Only pause or resume_on_demand is supported; bulk acquisition remains paused')
  book.enable('steam',data['action']=='resume_on_demand');pipeline.wake.set()
  return steam_status()
 app.steam_governor_control=steam_control
 filter_cache={}

 def slice_current(history_days=90,limit=500,search='',terms=None,blacklist=None,collection='',item_type='',grade='',min_price=None,max_price=None,sort_key='changePct',sort_dir=-1,projection='',offset=0):
  maximum=app.parse_int_param(limit,500,minimum=25,maximum=1000);offset=app.parse_int_param(offset,0,minimum=0,maximum=100000);filters=[];params=[]
  for term in app.split_filter_terms(terms or [])+app.split_filter_terms(search):filters.append('name LIKE ?');params.append('%'+term+'%')
  for term in app.split_filter_terms(blacklist or []):filters.append('name NOT LIKE ?');params.append('%'+term+'%')
  for column,value in [('collection',collection),('item_type',item_type),('rarity',grade)]:
   if value:filters.append(column+'=?');params.append(value)
  where=' WHERE '+' AND '.join(filters)if filters else''
  ranked=sort_key in ('latestPrice','realWorldPrice','source','latestAt')or min_price not in (None,'')or max_price not in (None,'')
  reverse=str(sort_dir)not in('1','asc','ASC')
  metadata_sort={'name':'name','type':'item_type','rarity':'rarity','grade':'rarity'}.get(sort_key,'name')
  with closing(app.steam_ask_db())as conn:
   deadline=time.monotonic()+3;conn.set_progress_handler(lambda:int(time.monotonic()>deadline),10000)
   total=conn.execute('SELECT COUNT(*) FROM item_catalog'+where,params).fetchone()[0]
   sql='SELECT name,market_hash_name,kind,item_type,weapon,collection,rarity FROM item_catalog'+where+' ORDER BY '+metadata_sort+' COLLATE NOCASE '+('DESC'if reverse else'ASC')+',name COLLATE BINARY ASC'
   records=[dict(r)for r in conn.execute(sql+(''if ranked else' LIMIT ? OFFSET ?'),params+([]if ranked else[maximum,offset]))]
  names=list(dict.fromkeys(r['market_hash_name']for r in records if r['market_hash_name']))
  prices=book.prices(names);quotes={q['marketHashName']:q for q in prices['items']};rows=[]
  for row in records:
   q=quotes.get(row['market_hash_name'],{});amount=q.get('canonicalPriceUsd')
   if min_price not in (None,'')and(amount is None or amount<float(min_price)):continue
   if max_price not in (None,'')and(amount is None or amount>float(max_price)):continue
   rows.append({'name':row['name'],'marketHashName':row['market_hash_name'],'kind':row['kind'],'type':row['item_type'],
    'weapon':row['weapon'],'collection':row['collection'],'rarity':row['rarity'],'latestPrice':amount,'realWorldPrice':amount,
    'realWorldProvider':q.get('selectedProvider'),'realWorldProviderLabel':{'skinport_live':'Skinport','market_csgo_live':'Market.CSGO','waxpeer_live':'WAXPEER'}.get(q.get('selectedProvider')),
    'steamPrice':None,'priceBasis':'lowest_comparable_cash_listing','confidencePct':None,'providerPrices':{},
    'priorPrice':None,'changePct':None,'changeDollar':None,'volatilityWindows':{},'latestAt':q.get('capturedAt'),
    'source':q.get('selectedProvider'),'groups':[],'cashState':q.get('state','missing'),'cashFreshness':q.get('freshnessState','missing'),
    'cashQuality':q.get('qualityState'),'priceReason':q.get('reason')})
  if ranked:
   total=len(rows)
   field={'latestPrice':'latestPrice','realWorldPrice':'latestPrice','source':'source','latestAt':'latestAt'}.get(sort_key)
   if field:
    # Stable name tie-break, with unpriced/missing values last in either direction.
    rows.sort(key=lambda r:r['name'])
    rows.sort(key=lambda r:r[field]if r[field]is not None else(''if field=='source'else 0),reverse=reverse)
    rows.sort(key=lambda r:r[field]is None)
   rows=rows[offset:offset+maximum]
  identity=cache.identity_revision()
  if filter_cache.get('identity')!=identity or time.monotonic()>filter_cache.get('until',0):
   with closing(app.steam_ask_db())as c:
    deadline=time.monotonic()+3;c.set_progress_handler(lambda:int(time.monotonic()>deadline),10000)
    available={key:[r[0]for r in c.execute('SELECT DISTINCT '+column+' FROM item_catalog WHERE '+column+" IS NOT NULL AND "+column+"<>'' ORDER BY "+column)]for key,column in(('collections','collection'),('types','item_type'),('grades','rarity'))}
   filter_cache.update(identity=identity,until=time.monotonic()+300,value=available)
  filters_available=filter_cache['value']
  result={'ok':True,'generatedAt':int(time.time()),'historyDays':int(history_days),'metricWindowDays':None,'metricWindowExact':False,
   'metricRows':0,'limit':maximum,'offset':offset,'hasMore':offset+len(rows)<total,'nextOffset':offset+len(rows)if offset+len(rows)<total else None,'totalItems':total,'returnedItems':len(rows),'items':rows,
   'filters':filters_available,'dataState':'current_cash','cacheState':'saved','rebuildRequired':False,
   'freshness':{'currentness':'per_quote_capture_and_snapshot','historyAvailable':False},
   'coverage':{'catalogItems':total,'returnedItems':len(rows),'priceSource':'canonical_cash','historicalAnalyticsAvailable':False}}
  return app.project_market_item_response(result,projection)
 # Keep the version-one research table and its historical metrics intact.
 # The cash-only projection has a separate, explicit route below.
 app.current_market_item_slice=slice_current
 original=app.Handler
 class Handler(original):
  def _do_GET_with_capacity_response(self):
   parts=urllib.parse.urlsplit(self.path);query=urllib.parse.parse_qs(parts.query)
   if parts.path=='/api/research-capabilities':
    if not self.require_auth(parts.path,'GET'):return
    return self.send_json(app.research_capabilities())
   if parts.path=='/api/current-market-items':
    if not self.require_auth(parts.path,'GET'):return
    try:
     return self.send_json(slice_current(history_days=query.get('days',['90'])[0],limit=query.get('limit',['500'])[0],
      search=query.get('search',[''])[0],terms=query.get('terms',[''])[0],blacklist=query.get('blacklist',[''])[0],
      collection=query.get('collection',[''])[0],item_type=query.get('type',[''])[0],grade=query.get('grade',[''])[0],
      min_price=query.get('min_price',[None])[0],max_price=query.get('max_price',[None])[0],
      sort_key=query.get('sort',['name'])[0],sort_dir=query.get('dir',['1'])[0],projection=query.get('projection',[''])[0],offset=query.get('offset',['0'])[0]))
    except(ValueError,TypeError)as error:return self.send_json({'ok':False,'error':str(error)},400)
   if parts.path in('/api/current-analysis','/api/current-report','/current-report'):
    if not self.require_auth(parts.path,'GET'):return
    try:
     data=analysis.read(query.get('portfolio',query.get('portfolio_id',[None]))[0])
     if parts.path=='/current-report':
      body=current_analysis.render_report(data).encode();self.send_response(200);self.send_header('Content-Type','text/html; charset=utf-8');self.send_header('Content-Length',str(len(body)));self.send_header('Cache-Control','no-store');self.end_headers();return self.safe_write(body)
     return self.send_json(data)
    except(ValueError,TypeError)as error:return self.send_json({'ok':False,'error':str(error)},409)
   if parts.path=='/api/canonical-cash-prices'and query.get('include_sources')==['1']:
    if not self.require_auth('/api/canonical-cash-prices','GET'):return
    try:
     names=json.loads(query.get('names',['[]'])[0])
     if not isinstance(names,list)or not 1<=len(names)<=8:raise ValueError('One to eight exact item hashes required')
     result=book.prices(names,include_sources=True);result['walletAsks']=book.prices(names,kind='wallet_ask')['items']
     result['observedHistory']=[pipeline.features.history(name)for name in names]
     return self.send_json(result)
    except(ValueError,TypeError)as error:return self.send_json({'error':str(error)},400)
   if parts.path=='/api/price-sources':
    if not self.require_auth('/api/price-sources','GET'):return
    return self.send_json(pipeline.status())
   return super()._do_GET_with_capacity_response()
 app.Handler=Handler
 return pipeline
