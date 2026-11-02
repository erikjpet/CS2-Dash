// Fixed public GET endpoints only. No inventory, credentials, raw files or trading.
import https from 'node:https';
import zlib from 'node:zlib';
import crypto from 'node:crypto';
let text='';for await(const block of process.stdin){text+=block;if(text.length>262144)throw new Error('Bounded local request required');}
const input=JSON.parse(text), provider=input.provider;
if(provider!=='skinport_sales'&&text.length>4096)throw new Error('Small public lookup required');
const held=provider==='skinport_sales'?new Set(input.heldNames):null;
if(held&&(!Array.isArray(input.heldNames)||held.size>10000||[...held].some(n=>typeof n!=='string'||!n||n.length>512)))throw new Error('Bounded local filter required');
const urls={skinport_live:'https://api.skinport.com/v1/items?app_id=730&currency=USD&tradable=0',
 skinport_sales:'https://api.skinport.com/v1/sales/history?app_id=730&currency=USD',
 waxpeer_live:'https://api.waxpeer.com/v1/prices?game=csgo&include_hold=1',
 market_csgo_live:'https://market.csgo.com/api/v2/prices/USD.json',
 csfloat_probe:'https://csfloat.com/api/v1/listings?market_hash_name=Prisma%202%20Case&sort_by=lowest_price&limit=1'};
if(provider==='steam'){
 if(typeof input.name!=='string'||!input.name||input.name.length>512||input.name.includes('\0'))throw new Error('Exact public market hash required');
 urls.steam='https://steamcommunity.com/market/priceoverview/?appid=730&currency=1&l=english&market_hash_name='+encodeURIComponent(input.name);
}
const url=urls[provider];if(!url)throw new Error('Unregistered source');
let encoded=0,decoded=0,req,res,decode;
function mills(value){if(typeof value==='boolean'||value==null)return null;const match=String(value).match(/^(\d+)(?:\.(\d{1,3}))?$/);if(!match)return null;const n=Number(BigInt(match[1])*1000n+BigInt((match[2]||'').padEnd(3,'0')));return Number.isSafeInteger(n)&&n>0?n:null;}
function count(value){const s=String(value);const n=Number(s);return /^\d+$/.test(s)&&Number.isSafeInteger(n)?n:null;}
try{
const result=await new Promise((resolve,reject)=>{
 let closed=false;const chunks=[];
 const finish=(error,value)=>{if(closed)return;closed=true;clearTimeout(timer);req?.destroy();res?.destroy();decode?.destroy();error?reject(error):resolve(value);};
 const timer=setTimeout(()=>finish(new Error('20s provider deadline')),20000);
 req=https.get(url,{headers:{Accept:'application/json','Accept-Encoding':'br,gzip,deflate','User-Agent':'CS2Dash/CurrentPrices'}},response=>{
  res=response;const headers=Object.fromEntries(['retry-after','date','age','etag','last-modified','content-type','content-encoding'].map(k=>[k,typeof res.headers[k]==='string'?res.headers[k].slice(0,256):null]));
  if(res.statusCode!==200)return finish(null,{status:res.statusCode,headers,requestUrl:url,receivedAtUs:Date.now()*1000});
  if(!String(headers['content-type']).includes('application/json'))return finish(new Error('Expected provider JSON'));
  const enc=headers['content-encoding']||'identity';
  decode=enc==='br'?zlib.createBrotliDecompress():enc==='gzip'?zlib.createGunzip():enc==='deflate'?zlib.createInflate():null;
  if(!decode&&enc!=='identity')return finish(new Error('Unsupported representation'));
  res.on('data',block=>{encoded+=block.length;if(encoded>8*1024**2)finish(new Error('8MiB encoded cap'));});res.on('error',error=>finish(error));
  const stream=decode?res.pipe(decode):res;stream.on('error',error=>finish(error));
  stream.on('data',block=>{decoded+=block.length;if(decoded>32*1024**2)finish(new Error('32MiB decoded cap'));else chunks.push(block);});
  stream.on('end',()=>{
   if(closed)return;
   try{
    const body=Buffer.concat(chunks),feed=JSON.parse(body.toString('utf8'));
    let rows=[],sales=null,providerTime=null,clock='api_capture_only_no_market_timestamp';
    if(provider==='skinport_live'){
     if(!Array.isArray(feed)||!feed.length||feed.length>150000)throw new Error('Nonempty bounded Skinport feed required');
     const grouped=new Map();
     for(const r of feed){
      if(typeof r.market_hash_name!=='string'||!r.market_hash_name||r.market_hash_name.length>512||r.currency!=='USD')throw new Error('Invalid exact hash/currency');
      const name=r.market_hash_name;
      if(grouped.has(name)){grouped.set(name,{name,state:'ambiguous',reason:'duplicate_or_variant_hash'});continue;}
      const quantity=count(r.quantity),price=mills(r.min_price),observed=Number.isSafeInteger(r.updated_at)?r.updated_at:null;
      let state=quantity===0?'no_listings':quantity>0&&price?'priced':'invalid';
      if(name.toLowerCase().includes('doppler'))state='ambiguous';
      grouped.set(name,{name,state,price_mills:state==='priced'?price:null,quantity,observed_at:observed,reason:state==='ambiguous'?'variant_phase_not_independently_bound':state==='invalid'?'invalid_availability_or_price':null});
     }
     rows=[...grouped.values()];clock='capture_time_and_provider_price_change_time';
    }else if(provider==='skinport_sales'){
     if(!Array.isArray(feed)||!feed.length||feed.length>150000)throw new Error('Nonempty bounded completed-sales feed required');
     const references=new Map();
     for(const r of feed){
      if(!held.has(r.market_hash_name))continue;
      if(r.currency!=='USD')throw new Error('Invalid sales currency');
      if(r.market_hash_name.toLowerCase().includes('doppler')||r.version!=null)continue;
      if(references.has(r.market_hash_name)){references.set(r.market_hash_name,null);continue;}
      const windows=[];
      for(const [key,days]of [['last_24_hours',1],['last_7_days',7],['last_30_days',30],['last_90_days',90]]){
       const w=r[key];if(!w||count(w.volume)===null)throw new Error('Invalid sales statistics');
       windows.push({days,volume:count(w.volume),median_mills:mills(w.median),minimum_mills:mills(w.min),maximum_mills:mills(w.max)});
      }
      references.set(r.market_hash_name,{name:r.market_hash_name,windows});
     }
     sales=[...references.values()].filter(Boolean);
     clock='rolling_completed_sales_windows_captured_at_not_current_ask';
    }else if(provider==='waxpeer_live'){
     if(feed.success!==true||!Array.isArray(feed.items)||!feed.items.length||feed.items.length>100000)throw new Error('Invalid WAXPEER grouped feed');
     const seen=new Set();
     for(const r of feed.items){const name=r.name;if(typeof name!=='string'||!name||name.length>512||seen.has(name))throw new Error('Invalid WAXPEER hash');seen.add(name);
      const quantity=count(r.count),raw=count(r.min),price=raw>0?raw:null;
      const state=name.toLowerCase().includes('doppler')?'ambiguous':quantity===0?'no_listings':quantity>0&&price?'priced':'invalid';
      rows.push({name,state,price_mills:state==='priced'?price:null,quantity,reason:state==='ambiguous'?'variant_phase_not_independently_bound':null});}
    }else if(provider==='market_csgo_live'){
     if(feed.success!==true||feed.currency!=='USD'||!Array.isArray(feed.items)||!feed.items.length||feed.items.length>150000||!Number.isSafeInteger(feed.time))throw new Error('Invalid market snapshot contract');
     providerTime=feed.time;clock='provider_snapshot_time';const seen=new Set();
     for(const r of feed.items){const name=r.market_hash_name;if(typeof name!=='string'||!name||name.length>512||seen.has(name))throw new Error('Invalid market identity');seen.add(name);
      const quantity=count(r.volume),price=mills(r.price),state=name.toLowerCase().includes('doppler')?'ambiguous':quantity===0?'no_listings':quantity>0&&price?'priced':'invalid';
      rows.push({name,state,price_mills:state==='priced'?price:null,quantity,reason:state==='ambiguous'?'variant_phase_not_independently_bound':null});}
    }else if(provider==='steam'){
     if(feed.success!==true||typeof feed.lowest_price!=='string'||!/^\$[\d,]+\.\d{2}$/.test(feed.lowest_price))throw new Error('Steam lowest USD ask unavailable');
     const price=mills(feed.lowest_price.slice(1).replaceAll(',',''));if(!price)throw new Error('Invalid Steam ask');
     rows=[{name:input.name,state:'priced',price_mills:price,quantity:null}];
    }else{
     return finish(null,{status:200,requestUrl:url,headers,receivedAtUs:Date.now()*1000,bodySHA256:crypto.createHash('sha256').update(body).digest('hex'),outerKeys:Object.keys(feed),listingRows:Array.isArray(feed.data)?feed.data.length:null,qualificationOnly:true});
    }
    const result={status:200,headers,requestUrl:url,receivedAtUs:Date.now()*1000,providerTime,clockSemantics:clock,
     bodySHA256:crypto.createHash('sha256').update(body).digest('hex'),encodedBytes:encoded,decodedBytes:decoded,rows,sales};
    if(Buffer.byteLength(JSON.stringify(result))>8*1024**2)throw new Error('8MiB normalized cap');
    finish(null,result);
   }catch(error){finish(error);}
  });
 });req.on('error',error=>finish(error));
});process.stdout.write(JSON.stringify(result));
}catch(error){process.stdout.write(JSON.stringify({status:null,error:String(error.message).slice(0,300),encodedBytes:encoded,decodedBytes:decoded,requestUrl:url,receivedAtUs:Date.now()*1000}));process.exitCode=1;}
