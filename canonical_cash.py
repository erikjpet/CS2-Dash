"""One policy for all current cash prices; no aggregate or wallet fallback.

The reader is injected so legacy storage and the compact store share identical
semantics during migration. Import has no I/O. GET consumers never fetch prices.
"""
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import contextlib, hashlib, json, math, time

POLICY='canonical-cash:v1:positive-fresh-gross-usd-exact-hash'
FRESHNESS=600
PROVIDERS=('skinport_live',)
SOURCE='skinport:items:730:USD:tradable0'
URL='https://api.skinport.com/v1/items?app_id=730&currency=USD&tradable=0'
FEE_BASIS='gross_listing_price_usd'

def money(value):return float(value.quantize(Decimal('.01'),rounding=ROUND_HALF_UP))
def positive(value):
 try:
  if isinstance(value,bool):return None
  x=Decimal(str(value))
  return x if x.is_finite() and x>0 and math.isfinite(float(x)) else None
 except (InvalidOperation,ValueError,TypeError,OverflowError):return None

def select_quote(name,rows,now):
 result=dict(name=name,marketHashName=name,canonicalPriceUsd=None,status='unpriced',
  state='missing',selectedProvider=None,capturedAt=None,providerUpdatedAt=None,
  quoteKind='lowest_available_listing',feeBasis=FEE_BASIS,currency='USD',
  freshnessState='missing',freshnessSeconds=FRESHNESS,expiresAt=None,reason='no_qualified_current_cash_listing',
  providerTimeSemantics='upstream_updated_at_not_claimed_trade_time')
 eligible=[];failures=[]
 for row in rows:
  if row.get('name')!=name or row.get('provider') not in PROVIDERS:continue
  try:raw=json.loads(row.get('raw') or '{}')
  except (ValueError,TypeError):raw={}
  if not isinstance(raw,dict):raw={}
  capture=raw.get('receivedAtUs');captured=capture//1000000 if type(capture) is int else None
  digest=raw.get('bodySHA256')
  valid=(raw.get('contractVersion')==1 and raw.get('provider')==row['provider'] and raw.get('marketHashName')==name
   and raw.get('currency')=='USD' and raw.get('source')==SOURCE and raw.get('basis')=='lowest_listed_price'
   and raw.get('requestUrl')==URL and type(capture) is int and capture>0 and captured==row.get('updated_at')
   and isinstance(digest,str) and len(digest)==64 and all(c in '0123456789abcdef' for c in digest))
  if not valid:failures.append(('invalid','invalid_quote_provenance',raw,captured));continue
  state=raw.get('state')
  if state!='priced':
   failures.append((state if state in ('missing','no_listings','ambiguous','invalid') else 'invalid',raw.get('reason') or 'unlisted_or_invalid',raw,captured));continue
  amount=positive(raw.get('priceText'));stored=positive(row.get('price'));quantity=raw.get('quantityListed')
  if amount is None or stored is None or amount!=stored or type(quantity) is not int or quantity<=0:
   failures.append(('invalid','nonpositive_or_unavailable_listing',raw,captured));continue
  # This capture lineage discarded Skinport version. Never represent a generic
  # Doppler minimum as a verified phase/gem price for a holding.
  if 'doppler' in name.casefold() and not raw.get('verifiedVersionBinding'):
   failures.append(('invalid','variant_version_not_qualified',raw,captured));continue
  if captured>now+60:failures.append(('invalid','future_capture_clock',raw,captured));continue
  if now>=captured+FRESHNESS:
   failures.append(('stale','capture_expired',raw,captured));continue
  eligible.append((amount,row['provider'],raw,captured))
 if eligible:
  amount,provider,raw,captured=min(eligible,key=lambda v:(v[0],v[1]))
  result.update(canonicalPriceUsd=float(amount),status='priced',state='priced',selectedProvider=provider,
   capturedAt=captured,providerUpdatedAt=raw.get('providerUpdatedAt'),freshnessState='fresh',
   expiresAt=captured+FRESHNESS,reason=None,quantityListed=raw['quantityListed'],receiptBodySHA256=raw['bodySHA256'])
 elif failures:
  # If no source qualifies, the diagnostic retains the newest source outcome.
  state,reason,raw,captured=max(failures,key=lambda v:v[3] or 0)
  result.update(status='stale' if state=='stale' else 'ambiguous' if state=='ambiguous' else 'unpriced',
   state=state,reason=reason,capturedAt=captured,providerUpdatedAt=raw.get('providerUpdatedAt'),
   freshnessState='stale' if state=='stale' else 'missing',expiresAt=captured+FRESHNESS if captured else None)
 return result

class CanonicalCash:
 def __init__(self,connection_factory,cash_service,clock=time.time):
  self.connect=connection_factory;self.cash=cash_service;self.clock=clock
 def _rows(self,conn,names):
  result={n:[] for n in names}
  if callable(getattr(self.cash,'current_rows',None)):
   for row in self.cash.current_rows(conn,names):result[row['name']].append(dict(row))
   return result
  for start in range(0,len(names),256):
   chunk=names[start:start+256]
   for row in conn.execute('SELECT name,provider,price,raw,updated_at FROM market_item_prices WHERE name IN ('+','.join('?' for _ in chunk)+') AND provider IN ('+','.join('?' for _ in PROVIDERS)+')',chunk+list(PROVIDERS)):
    result[row['name']].append(dict(row))
  return result
 def _meta(self,conn):
  row=conn.execute("SELECT value FROM settings WHERE key='cash_source_revision:skinport_live'").fetchone()
  revision=hashlib.sha256((POLICY+':'+(row['value'] if row else 'none')).encode()).hexdigest()
  receipt=self.cash._control(conn).get('lastSuccessfulFetch')
  return dict(ok=True,revision=revision,currency='USD',basis='lowest_comparable_cash_listing',
   provider='canonical_cash',valuationLane='canonical_cash',freshnessSeconds=FRESHNESS,
   sourceSummary=[dict(provider='skinport_live',eligible=True,quoteKind='lowest_available_listing',
    feeBasis=FEE_BASIS,currency='USD',cacheSeconds=300,providerTimeSemantics='upstream_updated_at_not_claimed_trade_time')],sourceReceipts=[receipt] if receipt else [],
   sourceReceipt=receipt,quoteScope='one_unit_gross_listing_reference_not_net_sale_proceeds_or_full_depth')
 def prices(self,names):
  if not isinstance(names,list) or len(names)>500 or any(not isinstance(n,str) or not n or len(n)>512 or '\0' in n for n in names):raise ValueError('At most500 exact market hash names are required')
  names=list(dict.fromkeys(names))
  with contextlib.closing(self.connect(write=False)) as conn:
   conn.execute('BEGIN');rows=self._rows(conn,names);result=self._meta(conn)
  now=int(self.clock());result['items']=[select_quote(n,rows[n],now) for n in names];return result
 def value(self,portfolio_id):
  with contextlib.closing(self.connect(write=False)) as conn:
   conn.execute('BEGIN');frozen=self.cash._holdings(portfolio_id,conn);rows=self._rows(conn,frozen['hashes']);result=self._meta(conn)
  now=int(self.clock());quotes={n:select_quote(n,r,now) for n,r in rows.items()}
  counts={k:0 for k in ('pricedRows','freshRows','staleRows','missingRows','noListingsRows','ambiguousRows','invalidRows','unresolvedRows','zeroQuantityRows')}
  covered=Decimal(0);items=[]
  for held in frozen['rows']:
   q=dict(quotes.get(held['marketHashName']) or select_quote(held['marketHashName'],[],now))
   state=('invalid' if held['quantityIssue'] else 'zero_quantity' if held['qty']==0 else 'unresolved' if held['identityIssue'] or not held['marketHashName'] else q['state'])
   value=None;unit=None
   if state=='priced':
    unit=q['canonicalPriceUsd'];value=money(Decimal(str(unit))*Decimal(held['quantityKey']));covered+=Decimal(str(value));counts['freshRows']+=1
   elif state=='zero_quantity':value=0
   key={'priced':'pricedRows','stale':'staleRows','missing':'missingRows','no_listings':'noListingsRows','ambiguous':'ambiguousRows','invalid':'invalidRows','unresolved':'unresolvedRows','zero_quantity':'zeroQuantityRows'}[state]
   counts[key]+=1
   q.update(displayName=held['displayName'],marketHashName=held['marketHashName'],qty=held['qty'],state=state,
    unitPriceUsd=unit,valueUsd=value,reason=held['quantityIssue'] or held['identityIssue'] or q.get('reason'))
   if state!='priced':
    q.update(canonicalPriceUsd=None,status='stale' if state=='stale' else 'ambiguous' if state=='ambiguous' else 'unpriced',
     freshnessState='stale' if state=='stale' else 'missing')
   items.append(q)
  full=counts['pricedRows']+counts['zeroQuantityRows']==len(frozen['rows'])
  result.update(portfolioId=frozen['portfolioId'],holdingsFingerprint=frozen['holdingsFingerprint'],
   displayHoldings=frozen['displayHoldings'],items=items,
   coverage=dict(counts,holdingRows=len(frozen['rows']),distinctHashes=len(frozen['hashes']),fullCoverage=full,truncated=False),
   totals=dict(coveredValueUsd=money(covered),fullValueUsd=money(covered) if full else None,
    freshCoveredValueUsd=money(covered),freshFullValueUsd=money(covered) if full else None))
  return result
