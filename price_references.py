"""Useful price evidence, explicitly excluded from canonical current cash totals."""

def install_retained(book,groups):
 """Explicit bounded import of existing typed USD records; no DDL or network."""
 if not isinstance(groups,list)or len(groups)>10:raise ValueError('Bounded reference groups required')
 outcomes=[]
 for group in groups:
  provider=group['provider']
  if provider not in('saved_cash_skinport','saved_cash_buff163','saved_cash_youpin','saved_cash_csfloat','saved_wallet_steam'):raise ValueError('Known retained reference source required')
  if not isinstance(group['rows'],list)or not 0<len(group['rows'])<=10000:raise ValueError('Bounded retained records required')
  if any('doppler'in r['name'].casefold()for r in group['rows']):raise ValueError('Per-unit phase identity required')
  with book.lock,book.connect(True)as c:
   c.execute('''INSERT INTO provider(id,kind,currency,enabled,cadence,ttl,note) VALUES(?,'reference','USD',0,21600,0,?)ON CONFLICT DO NOTHING''',
    (provider,'Retained recorded USD snapshot; not current, not a qualified cash ask, and excluded from cash totals'))
   saved=c.execute('SELECT kind,currency,enabled FROM provider WHERE id=?',(provider,)).fetchone()
   if tuple(saved)!=('reference','USD',0):raise ValueError('Retained source must remain a disabled reference')
  outcomes.append({'provider':provider,**book.publish(provider,group['receipt'],group['rows'])})
 return outcomes

def choose(name,current,cash_rows,other_rows,sales,now):
 if current['state']=='priced'or'doppler'in name.casefold():return None
 def make(price,provider,kind,at,label,confidence,reason,**extra):
  if not price or price<=0 or not at or at>now+60:return None
  return dict(priceUsd=price/1000,provider=provider,kind=kind,capturedAt=at,ageSeconds=max(0,now-at),label=label,
   confidence=confidence,reason=reason,currency='USD',excludedFromCash=True,marketTimestamp=None,**extra)
 def at(row):return row['received_us']//1000000
 def available(row):
  stamp=at(row);clock=row.get('provider_time')if row['clock_semantics']=='provider_snapshot_time'else None
  return row['state']=='priced'and row['price_mills']and stamp<=now+60 and now<min(stamp,clock or stamp)+row['ttl']
 fresh=[r for r in cash_rows if available(r)and r.get('active',1)and r['quantity']and r['quantity']>0]
 if fresh and current.get('qualityState')!='outlier':
  r=min(fresh,key=lambda r:(r['price_mills'],r['provider']))
  return make(r['price_mills'],r['provider'],'unverified_cash_ask',at(r),'Unverified cash ask','insufficient_independent_evidence',current.get('reason'),listings=r['quantity'])
 windows=[r for r in sales if r['median_mills']and r['sales_count']>0 and r['captured_us']//1000000<=now+60 and now<r['captured_us']//1000000+3600]
 if windows:
  r=min(windows,key=lambda r:([7,1,30,90].index(r['days']),-r['sales_count']))
  return make(r['median_mills'],'skinport_sales','completed_sales_median',r['captured_us']//1000000,str(r['days'])+'d completed-sales median','historical_sales_reference',
   'A rolling completed-sale summary, not a currently available listing or bid',days=r['days'],completedSales=r['sales_count'])
 wallets=[r for r in other_rows if r['kind']=='wallet_ask'and available(r)]
 if wallets:
  r=max(wallets,key=at);return make(r['price_mills'],r['provider'],'steam_wallet_ask',at(r),'Steam wallet ask','current_wallet_listing',
   'Steam wallet funds, not cash marketplace proceeds',expiresAt=at(r)+r['ttl'])
 oldcash=[r for r in cash_rows if r['state']=='priced'and r['price_mills']and r['received_us']//1000000<=now+60]
 if oldcash:
  r=max(oldcash,key=at);return make(r['price_mills'],r['provider'],'last_observed_cash_ask',at(r),'Last observed cash ask','stale_or_withdrawn_listing',
   'Historical observed ask; current availability and price are unconfirmed')
 saved=[r for r in other_rows if r['kind']=='reference'and r['state']=='priced'and r['price_mills']and not r['provider'].startswith('saved_wallet_')]
 if saved:
  r=min(saved,key=lambda r:('skinport'not in r['provider'],-at(r),r['price_mills']))
  return make(r['price_mills'],r['provider'],'retained_cash_snapshot',at(r),'Saved cash-source reference','unqualified_historical_snapshot',
   'Recorded USD snapshot; original availability and precise quote type were not retained')
 saved=[r for r in other_rows if r['state']=='priced'and r['price_mills']and(r['kind']=='wallet_ask'or r['provider'].startswith('saved_wallet_'))]
 if saved:
  r=max(saved,key=at);return make(r['price_mills'],r['provider'],'retained_steam_reference',at(r),'Saved Steam wallet reference','historical_wallet_aggregate',
   'Historical Steam aggregate or expired wallet ask; not current cash')
 return None
