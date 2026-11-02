// Exactly one authenticated listing GET. No cookies, account or trading methods.
import https from 'node:https';
import crypto from 'node:crypto';
import {pathToFileURL} from 'node:url';

export function normalize(name,feed){
 if(!feed||!Array.isArray(feed.data)||feed.data.length>50)throw new Error('Bounded listing envelope required');
 if(name.toLowerCase().includes('doppler'))throw new Error('Per-unit phase identity required');
 let minimum=null,count=0;const seen=new Set(),listings=[];
 for(const row of feed.data){
  if(!row||row.item?.market_hash_name!==name||row.type!=='buy_now'||row.state!=='listed'||(row.currency!=null&&row.currency!=='USD'))throw new Error('Exact available USD buy-now listings required');
  if(typeof row.id!=='string'||!row.id||seen.has(row.id))throw new Error('Unique listing identity required');seen.add(row.id);
  if(!Number.isSafeInteger(row.price)||row.price<=0||row.price>Math.floor(Number.MAX_SAFE_INTEGER/10))throw new Error('Positive integer USD cents required');
  minimum=minimum===null?row.price:Math.min(minimum,row.price);count++;
  listings.push({price:row.price,seller:typeof row.seller?.steam_id==='string'&&/^\d{17}$/.test(row.seller.steam_id)?row.seller.steam_id:null});
 }
 const near=listings.filter(r=>r.price<=minimum*1.25),sellers=new Set(near.map(r=>r.seller).filter(Boolean));
 const reason=count?(near.length>=3&&sellers.size>=3?'direct_listing_depth_v1:'+near.length+':'+sellers.size:null):'no_available_buy_now_listing';
 // Only bounded evidence counts survive normalization; seller/listing identities are discarded.
 return {name,state:count?'priced':'no_listings',price_mills:count?minimum*10:null,quantity:count,reason};
}

export function requestCurrent(name,{key=process.env.CS2DASH_CSFLOAT_API_KEY,get=https.get,now=()=>Date.now()}={}){
 if(typeof name!=='string'||!name||name.length>512||/[\0\r\n]/.test(name)||name.toLowerCase().includes('doppler'))return Promise.resolve({status:null,error:'Exact non-phase market hash required'});
 if(typeof key!=='string'||key.length<8||key.length>1024||/[^\x21-\x7e]/.test(key))return Promise.resolve({status:null,error:'Private CSFloat credential unavailable'});
 const url='https://csfloat.com/api/v1/listings?'+new URLSearchParams({market_hash_name:name,sort_by:'lowest_price',type:'buy_now',limit:'50'});
 return new Promise(resolve=>{
  let req,res,closed=false,bytes=0;const blocks=[];
  const finish=value=>{if(closed)return;closed=true;clearTimeout(timer);req?.destroy();res?.destroy();resolve(value);};
  const timer=setTimeout(()=>finish({status:null,error:'CSFloat20seconddeadline'}),20000);
  try{
   req=get(url,{headers:{Accept:'application/json','Accept-Encoding':'identity',Authorization:key,'User-Agent':'CS2Dash/CurrentPrices'}},response=>{
    res=response;const headers={};
    for(const field of ['x-ratelimit-limit','x-ratelimit-remaining','x-ratelimit-reset','ratelimit-limit','ratelimit-remaining','ratelimit-reset']){
     const value=res.headers[field];if(typeof value==='string'&&/^\d{1,16}$/.test(value)&&!value.includes(key))headers[field]=value;
    }
    const retry=res.headers['retry-after'];if(typeof retry==='string'&&retry.length<=128&&!retry.includes(key)&&/^[\w\s,:+\-]+$/.test(retry))headers['retry-after']=retry;
    const common={status:res.statusCode,headers,requestUrl:url,receivedAtUs:now()*1000};
    if(res.statusCode!==200)return finish({...common,error:'CSFloat HTTP'+res.statusCode});
    if(!String(res.headers['content-type']).includes('application/json')||res.headers['content-encoding']&&res.headers['content-encoding']!=='identity')return finish({...common,status:null,terminal:true,error:'CSFloat JSON identity representation required'});
    res.on('data',block=>{bytes+=block.length;if(bytes>1024*1024)finish({...common,status:null,error:'CSFloat1MiBresponsecap'});else blocks.push(block);});
    res.on('error',()=>finish({...common,status:null,error:'CSFloat response failed'}));
    res.on('end',()=>{if(closed)return;try{
     const body=Buffer.concat(blocks);if(body.includes(Buffer.from(key)))return finish({...common,status:null,terminal:true,error:'CSFloat response reflection rejected'});
     const row=normalize(name,JSON.parse(body.toString('utf8')));
     finish({...common,bodySHA256:crypto.createHash('sha256').update(body).digest('hex'),rows:[row],clockSemantics:'individual_listing_capture_time_not_trade_time',responseBytes:bytes});
    }catch{finish({...common,status:null,terminal:true,error:'CSFloat listing contract not qualified'});}});
   });req.on('error',()=>finish({status:null,error:'CSFloat connection failed'}));
  }catch{finish({status:null,error:'CSFloat connection failed'});}
 });
}

if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href){
 let input='';for await(const part of process.stdin){input+=part;if(input.length>4096)throw new Error('Bounded public lookup required');}
 try{const result=await requestCurrent(JSON.parse(input).name);process.stdout.write(JSON.stringify(result));}
 catch{process.stdout.write(JSON.stringify({status:null,error:'CSFloat bounded request rejected'}));process.exitCode=1;}
}
