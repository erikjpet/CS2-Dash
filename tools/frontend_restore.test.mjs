/** Source / synthetic-fixture acceptance only. No browser, service, network or real portfolio data. */
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';

const source=readFileSync(new URL('../index.html',import.meta.url),'utf8');
const markup=source.replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi,'');
function fn(name){
 const m=new RegExp('^[\\t ]*(?:async[\\t ]+)?function[\\t ]+'+name+'\\s*\\(','m').exec(source);
 assert.ok(m,`function ${name} exists`);let end=source.indexOf('}',m.index);
 while(end>=0){const body=source.slice(m.index,end+1);try{new vm.Script('('+body+'\n)');return body;}catch(e){if(!(e instanceof SyntaxError))throw e;}end=source.indexOf('}',end+1);}
 assert.fail(`function ${name} does not parse`);
}
function run(names,expression,globals={}){return new vm.Script(names.map(fn).join('\n')+'\n'+expression).runInNewContext(globals,{timeout:3000});}
const IDX={n:0,q:1,pct:2,d:3,ct:4,pt:5,cat:6,grp:7};

test('all inline scripts compile without running the dashboard',()=>{
 for(const script of source.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g))new vm.Script(script[1]);
});
test('historical controls and canvas shells are mounted, current additions retained',()=>{
 const ids=['snap-select','global-scale','data-task-mode','data-pull-mode','data-history-limit','data-task-cycles','data-retry-failures','data-task-start-btn','overview-curve-days','warm-portfolio-history-btn','decision-note','ov-return-split','ov-return-top','ov-return-bottom','bk-vol-thin','bk-vol-min-price','bk-vol-min-coverage','bk-sort-value','bk-sort-dollar','bk-sort-pct','bk-sort-vol7','bk-sort-vol30','bk-sort-vol90','bk-sort-count','bk-sort-name','market-bk-vol-thin','market-bk-vol-min-price','market-bk-vol-min-coverage','market-bk-sort-value','market-bk-sort-dollar','market-bk-sort-pct','market-bk-sort-vol7','market-bk-sort-vol30','market-bk-sort-vol90','market-bk-sort-count','market-bk-sort-name','items-mode-market','items-mode-portfolio','items-basis','item-search','item-coll-filter','item-type-filter','item-grade-filter','item-min-price','item-max-price','item-preset-name','item-preset-icon','file-input','paste-input','snap-label','portfolio-cash-panel','current-decision-summary','current-observed-curve','current-market-sources','history-observed-curve'];
 for(const id of ids)assert.match(markup,new RegExp('id="'+id+'"'));
 for(const id of ['pie-chart','rollup-bar','overview-curve-chart','overview-sector-return-chart','bk-chart-value','bk-chart-return','market-bk-chart-value','market-bk-chart-return','trend-chart'])assert.match(markup,new RegExp('<canvas\\b[^>]*id="'+id+'"'));
 assert.doesNotMatch(markup,/<select[^>]*id="global-scale"[^>]*\bhidden/);
 assert.doesNotMatch(fn('renderCurrentSources'),/legacyControls\.hidden\s*=\s*true/);
});
test('static controls have unique IDs',()=>{
 const ids=[...markup.matchAll(/\bid="([^"]+)"/g)].map(m=>m[1]);
 assert.equal(new Set(ids).size,ids.length);
});
test('restored research reads and separate current decision endpoint are wired',()=>{
 assert.match(fn('requestInventoryAnalytics'),/fetch\('\/api\/inventory/);
 assert.match(fn('requestInventoryAnalytics'),/indexInventoryAnalytics/);
 assert.match(fn('buildBreakdown'),/loadTaxonomyAnalysis\('portfolio'/);
 assert.match(fn('buildMarketAnalysis'),/loadTaxonomyAnalysis\('market'/);
 assert.match(fn('requestCurrentAnalysis'),/\/api\/current-analysis\?portfolio=/);
 assert.match(fn('startDataTask'),/method:\s*'POST'/);
 assert.match(fn('startDataTask'),/\/api\/data-tasks/);
 assert.match(fn('pollDataTasks'),/\/api\/data-tasks/);
});
test('public readonly portfolio curve uses cached GET without acquisition flags',async()=>{
 let captured='';
 const globals={_readonly:true,_portfolioCurveKey:'',_portfolioCurveLoading:false,_portfolioCurveRequestId:0,_savedReadGeneration:0,_globalScale:90,active:{id:'SYNTHETIC',data:{}},document:{getElementById:id=>id==='overview-curve-days'?{value:'90'}:{classList:{contains:()=>false}}},fetch:async url=>{captured=url;return {ok:true,json:async()=>({ok:true,points:[],coverage:{}})};},updateWarmPortfolioStatusFromCoverage(){}};
 await run(['requestPortfolioCurve'],'requestPortfolioCurve({refresh:true,forceSteam:true,force:true})',globals);
 const query=new URL(captured,'https://synthetic.invalid');assert.equal(query.pathname,'/api/portfolio-history');assert.equal(query.searchParams.has('refresh'),false);assert.equal(query.searchParams.has('force'),false);
});

test('portfolio warm and sync controls submit selected tracked history jobs',async()=>{
 for(const control of ['warmPortfolioHistory','syncPortfolioSteamData']){
  const requests=[];const elements={};
  const globals={Object,JSON,_readonly:false,_steamSyncRunning:false,_portfolioHistoryTask:null,
   _dataTasksUnsupported:false,_dataTaskPolling:false,_lastDataTaskId:'',active:{id:'SYNTHETIC-SELECTED'},
   document:{getElementById:id=>(elements[id]??={value:'',checked:true,disabled:false})},
   setWarmPortfolioStatus(){},renderMarketDataStatus(){},renderDataTasks(){},
   renderPortfolioHistoryTask(data){globals._portfolioHistoryTask=data.task;},
   fetch:async(url,options)=>{requests.push({url,...options});const payload=JSON.parse(options.body);return{ok:true,status:200,json:async()=>({ok:true,task:{id:'SYNTHETIC-JOB',mode:'steam_history',status:'queued',options:payload}})};},
   requestPortfolioCurve(){assert.fail('A button must not invoke one-shot curve acquisition');},
   syncMarketUniverseData(){assert.fail('Portfolio history must not fall back to unrelated market work');}};
  await run(['startDataTask','queuePortfolioHistoryTask','warmPortfolioHistory','syncPortfolioSteamData'],control+'()',globals);
  assert.equal(requests.length,1);assert.equal(requests[0].url,'/api/data-tasks');assert.equal(requests[0].method,'POST');
  const body=JSON.parse(requests[0].body);assert.equal(body.mode,'steam_history');assert.equal(body.portfolio,'SYNTHETIC-SELECTED');
  assert.equal(body.historyScope,'portfolio');assert.equal(body.historyLimit,control==='warmPortfolioHistory'?80:250);
  assert.equal(body.historyAll,control==='syncPortfolioSteamData');assert.equal(body.force,false);assert.equal(body.cycles,1);
  assert.equal(globals._lastDataTaskId,'SYNTHETIC-JOB');
 }
});

test('portfolio history waiting status stays tracked and completion only reloads cached curve',()=>{
 const elements={},messages=[],reads=[];
 const globals={active:{id:'SYNTHETIC-SELECTED'},_portfolioHistoryTask:null,_portfolioHistoryRefreshKey:'',_steamSyncRunning:false,
  document:{getElementById:id=>(elements[id]??={disabled:false,textContent:''})},
  setWarmPortfolioStatus:(message,state)=>messages.push({message,state}),exactTime:value=>'synthetic-time-'+value,
  invalidateSavedReadCaches(){},requestPortfolioCurve:options=>reads.push(options)};
 const task={id:'SYNTHETIC-JOB',mode:'steam_history',status:'waiting',message:'Saved provider deadline',progress:{retryAt:123},options:{historyScope:'portfolio',portfolio:'SYNTHETIC-SELECTED'}};
 globals.payload={active:task};run(['renderPortfolioHistoryTask'],'renderPortfolioHistoryTask(payload)',globals);
 assert.equal(messages.at(-1).state,'loading');assert.match(messages.at(-1).message,/SYNTHETIC-JOB: waiting/);
 assert.equal(elements['warm-portfolio-history-btn'].disabled,true);assert.equal(reads.length,0);
 globals.payload={tasks:[{...task,status:'complete',message:'Data task complete'}]};
 run(['renderPortfolioHistoryTask'],'renderPortfolioHistoryTask(payload);renderPortfolioHistoryTask(payload)',globals);
 assert.equal(messages.at(-1).state,'ready');assert.equal(elements['steam-sync-btn'].disabled,false);
 assert.equal(reads.length,1);assert.equal(reads[0].force,true);assert.equal(reads[0].refresh,undefined);assert.equal(reads[0].forceSteam,undefined);
 globals.active={id:'SYNTHETIC-OTHER'};
 run(['renderPortfolioHistoryTask'],'renderPortfolioHistoryTask(payload)',globals);
 assert.equal(globals._portfolioHistoryTask,null);assert.equal(globals._steamSyncRunning,false);
});
test('both item bases send true sort, bounds and page offset before server limit',()=>{
 for(const basis of ['research','cash'])for(const sort of ['latestPrice','source','latestAt']){
  const values={'item-search':'SYNTHETIC','item-coll-filter':'','item-type-filter':'','item-grade-filter':'','item-min-price':'1000','item-max-price':'10000'};
  const globals={URLSearchParams,_itemsBasis:basis,_marketItemsOffset:500,_globalScale:90,_searchTerms:[],_blTerms:[],_sortKey:sort,_sortDir:-1,document:{getElementById:id=>({value:values[id]})}};
  const url=new URL(run(['researchPeriodDays','marketItemsQueryState','marketItemsUrl'],'marketItemsUrl()',globals),'https://fixture.invalid');
  assert.equal(url.pathname,basis==='cash'?'/api/current-market-items':'/api/market-items');
  assert.equal(url.searchParams.get('sort'),sort);assert.equal(url.searchParams.get('min_price'),'1000');assert.equal(url.searchParams.get('max_price'),'10000');assert.equal(url.searchParams.get('offset'),'500');assert.equal(url.searchParams.get('limit'),'500');
 }
});
test('research and current item renderers stay explicitly separate',()=>{
 assert.match(fn('buildItemsTab'),/buildCashItemsTab/);assert.match(fn('buildItemsTab'),/buildResearchItemsTab/);
 for(const role of ['changePct','changeDollar','steamPrice','steamPremiumPct','csfloatPrice','buffPrice','priorPrice','volatility7','volatility30','volatility90'])assert.match(fn('buildResearchItemsTab'),new RegExp("itemSortHeader\\('"+role+"'"));
 assert.match(fn('sortAndRenderResearchItems'),/steam-history-box/);
 assert.match(fn('buildCashItemsTab'),/canonical|Cash price/);
 assert.match(fn('sortAndRenderCashItemsWithinHoldingsScope'),/canonicalItemCells/);
});
test('research and cash headlines cannot overwrite each other',()=>{
 assert.doesNotMatch(fn('applySteamSummary'),/getElementById\('s-curr'\)/);
 assert.match(fn('applySteamSummary'),/s-benchmark-curr/);
 assert.match(fn('renderCanonicalHeadline'),/s-curr/);
 assert.match(markup,/Provider benchmark valuation/);
 assert.match(markup,/separate from current cash/);
});
test('missing research prior values are not flat-filled from current quotes',()=>{
 assert.doesNotMatch(fn('steamOverviewData'),/if\s*\(past === null\)\s*past = curr/);
 assert.doesNotMatch(fn('steamOverviewData'),/pct \|\| 0, dollar \|\| 0, curr, past \|\| 0/);
 assert.match(fn('steamOverviewData'),/r\.pct = r\.history === r\.count/);
 const summary=run(['finiteNum','steamSummaryForData'],'steamSummaryForData({items:[["SYNTHETIC",1]]})',{IDX,analyticsForItem:()=>({marketCurr:12,marketPast:null})});
 assert.equal(summary.baseline,0);assert.equal(summary.baselineFilled,0);assert.equal(summary.past,null);assert.equal(summary.dollar,null);assert.equal(summary.pct,null);
 const group=run(['finiteNum','breakdownRow'],'breakdownRow(fixture)',{fixture:{name:'SYNTHETIC',count:1,priced:1,currentValue:12,priorValue:null,avgChangePct:null,avgVolatilityPct:null}});
 assert.equal(group.avgItemPct,null);assert.equal(group.volatility,null);assert.equal(group.prior,null);
 const panel={style:{}};
 run(['finiteNum','renderPortfolioCurveConfidence'],'renderPortfolioCurveConfidence({items:2,itemsWithHistory:0,backfilledCurrentItems:0},true)',{document:{getElementById:()=>panel}});
 assert.match(panel.innerHTML,/missing raw history stays unavailable/);
 assert.doesNotMatch(panel.innerHTML,/current-price backfilled/);
});
test('nullable TXT export round-trips exact synthetic identities and quantities',()=>{
 const items=Array.from({length:12},(_,i)=>['SYNTHETIC '+i+' | Quoted "name", Ω',i+1,null,null,null,null,'Synthetic','Synthetic']);
 const result=run(['finiteNum','snapshotExportText','parseTextFormat'],`(()=>{const text=snapshotExportText(fixture);return {text,rows:parseTextFormat(text)};})()`,{IDX,fixture:{data:{items}}});
 assert.ok(result.text.startsWith('CS2Dash holdings export v2'));assert.match(result.text,/N\/A/);assert.equal(result.rows.length,items.length);
 result.rows.forEach((row,i)=>{assert.equal(row.name,items[i][0]);assert.equal(row.qty,items[i][1]);assert.equal(row.curr,null);assert.equal(row.past,null);assert.equal(row.pct,null);assert.equal(row.dollar,null);});
 assert.match(fn('exportSnap'),/const text=snapshotExportText\(snap\)/);
});
test('numeric TXT benchmark export retains unit values and signs',()=>{
 const result=run(['finiteNum','snapshotExportText','parseTextFormat'],'parseTextFormat(snapshotExportText(fixture))',{IDX,fixture:{data:{items:[['SYNTHETIC',3,-12.5,-7.5,52.5,60,'x','x']]}}});
 assert.equal(result[0].qty,3);assert.equal(result[0].curr,17.5);assert.equal(result[0].past,20);assert.equal(result[0].pct,-12.5);assert.equal(result[0].dollar,-7.5);
});
test('CSV import handles LF/CRLF, quoted commas/escaped quotes and unknown financial fields',()=>{
 for(const newline of ['\n','\r\n']){
  const lines=['Item Name,Amount,Past Price,Current Price,Return %,Return',...Array.from({length:10},(_,i)=>`"SYNTHETIC ${i}, ""Ω""",2,N/A,,N/A,`)];
  const rows=run(['parseCSVRows','parseCSVFormat'],'parseCSVFormat(fixture)',{fixture:lines.join(newline)});
  assert.equal(rows.length,10);assert.equal(rows[0].name,'SYNTHETIC 0, "Ω"');assert.equal(rows[0].qty,2);assert.equal(rows[0].curr,null);assert.equal(rows[0].past,null);
 }
});
test('latest historical duplicate import fix remains functional',()=>{
 const raw=[{name:'SYNTHETIC',qty:2,past:10,curr:12,pct:20,dollar:4},{name:'SYNTHETIC',qty:2,past:10,curr:12,pct:20,dollar:4},{name:'SYNTHETIC',qty:3,past:20,curr:24,pct:20,dollar:12},{name:'BAD',qty:0}];
 const result=run(['importNumber','importWeightedAvg','importExactKey','normalizeImportRows'],'normalizeImportRows(fixture)',{fixture:raw,cleanText:v=>String(v??'').trim()});
 assert.equal(result.items.length,1);assert.equal(result.items[0].qty,5);assert.equal(result.stats.exactDuplicates,1);assert.equal(result.stats.mergedRows,1);assert.equal(result.stats.invalidRows,1);
});
test('advanced filter disclosure is not forcibly collapsed by every response',()=>{
 assert.match(fn('updateItemsModeControls'),/!advanced\.dataset\.initialized/);
 assert.match(fn('buildMarketItemsTab'),/updateMarketPagination\(\)/);
 assert.match(markup,/id="items-page-previous"/);assert.match(markup,/id="items-page-next"/);
});
test('notes and preset keys are preserved, basis added to captured state',()=>{
 assert.match(source,/DECISION_NOTE_KEY\s*=\s*'cs2dash_decision_note_v1'/);
 assert.match(source,/ITEM_PRESET_STORAGE_KEY\s*=\s*'cs_item_filter_presets_v1'/);
 assert.match(fn('captureCurrentItemFilters'),/basis:\s*_itemsBasis/);
 assert.match(fn('applyItemFilterState'),/filters\.basis==='cash'/);
 assert.match(fn('hydrateDecisionNote'),/localStorage\.getItem\(DECISION_NOTE_KEY\)/);
});
test('cash rendering indexes holdings once while preserving mutation detection outside render scope',()=>{
 assert.doesNotMatch(fn('canonicalHoldingValue'),/\.find\(/);
 const n=1600,rows=Array.from({length:n},(_,i)=>['SYNTHETIC '+i,1]);let maps=0;
 const originalMap=rows.map.bind(rows);rows.map=(...args)=>{maps++;return originalMap(...args);};
 const globals={IDX,active:{id:'synthetic',data:{items:rows}},canonicalAcceptedHoldingsCache:null,canonicalHoldingsRenderScope:null,canonicalCashState:{revision:'synthetic'},portfolioCashState:{value:{items:rows.map(row=>({displayName:row[0],qty:1,valueUsd:1}))},acceptedRevision:'synthetic',holdingsKey:JSON.stringify(['synthetic',rows.map(row=>[row[0],row[1]])])},canonicalHoldingsBodyReusable:()=>true,canonicalQuote:()=>({}),canonicalHoldingQuoteMatches:()=>true};
 maps=0;
 const result=run(['finiteNum','portfolioCashHoldingsKey','withCanonicalHoldingsScope','canonicalHoldingValue'],`(()=>{let result;withCanonicalHoldingsScope(()=>{for(let i=0;i<1000;i++)result=canonicalHoldingValue('SYNTHETIC 0',1);});return result;})()`,globals);
 assert.equal(result,1);assert.equal(maps,1);
 rows[0][1]=2;
 const changed=run(['finiteNum','portfolioCashHoldingsKey','withCanonicalHoldingsScope','canonicalHoldingValue'],"canonicalHoldingValue('SYNTHETIC 0',1)",globals);
 assert.equal(changed,null);
 assert.doesNotMatch(fn('sortItems'),/\n\s*sortAndRender\(\);/);
});
test('restored tables scroll locally rather than clipping fields on mobile',()=>{
 assert.match(source,/\.item-table-wrap\{overflow-x:auto/);
 assert.match(source,/\.card:has\(\.pulse-table\),#bk-table-wrap,#market-bk-table-wrap\{overflow-x:auto/);
 assert.match(source,/\.header-center\{flex-wrap:wrap;min-width:0\}/);
 assert.match(source,/\.snap-info h3[^}]*overflow-wrap:anywhere/);
});

function presetReplayHarness(){
 const elements=new Map(),rendered=[];
 const metadata={Alpha:{collection:'Collection A',rarity:'Covert',type:'Rifle',weapon:'Alpha weapon'},Beta:{collection:'Collection B',rarity:'Classified',type:'Pistol'},Unknown:{collection:'Collection A',rarity:'Covert',type:'Rifle'}};
 function element(id){
  let html='',value='',options=[{value:''}];
  const select=['item-coll-filter','item-type-filter','item-grade-filter','global-scale'].includes(id);
  const el={id,style:{},textContent:'',disabled:false,checked:false,classList:{contains:()=>id==='tab-items'}};
  Object.defineProperties(el,{
   innerHTML:{get:()=>html,set:v=>{html=String(v);if(select){options=[...html.matchAll(/<option\b[^>]*value="([^"]*)"/g)].map(m=>({value:m[1]}));if(!options.some(o=>o.value===value))value='';}}},
   options:{get:()=>options},
   value:{get:()=>value,set:v=>{const next=String(v);value=!select||options.some(o=>o.value===next)?next:'';}},
  });
  return el;
 }
 const globals={IDX,Map,Set,Array,Number,String,JSON,Math,URLSearchParams,AbortController,
  active:{id:'SYNTHETIC',label:'Invented portfolio',data:{items:[['Alpha',1,null,null,null,null,'Category A','Rifle'],['Beta',1,null,null,null,null,'Category B','Pistol']],totals:{count:2}}},
  _itemsBasis:'cash',_itemsMode:'portfolio',_marketItemsOffset:500,_marketItemsKey:'old',_marketItemsCache:new Map(),_searchTerms:[],_blTerms:[],_sortKey:'ct',_sortDir:-1,_filtered:[],
  document:{getElementById:id=>{if(!elements.has(id))elements.set(id,element(id));return elements.get(id);},querySelector:()=>({id:'tab-items'})},
  itemMeta:name=>metadata[name]||{},findCollection:()=>'',findEvent:name=>name==='Alpha'?'Event A':'',dispName:v=>v,esc:v=>String(v??''),
  analyticsForItem:name=>({marketUnitPrice:name==='Alpha'?2:name==='Beta'?20:null}),canonicalPrice:name=>name==='Alpha'?20:name==='Beta'?2:null,
  cancelMarketItemsRequest(){},updateItemsModeControls(){},requestInventoryAnalytics(){},requestCanonicalNames(){},renderTags(){},itemSortHeader(){return '';},renderItemPresets(){},
  sortAndRender(){rendered.push({basis:globals._itemsBasis,rows:Array.from(globals._filtered,i=>i[IDX.n])});},
  itemPresetById(){return{id:'SYNTHETIC-PRESET',name:'Invented preset',filters:globals.preset};},
 };
 const context=vm.createContext(globals);
 new vm.Script(['finiteNum','steamMetricForItem','setSelectIfPresent','applyItemFilterState','selectItemPreset','buildItemsTab','buildCashItemsTab','buildResearchItemsTab','filterItems','filterCashItems','filterResearchItems'].map(fn).join('\n')).runInContext(context);
 return {context,elements,rendered,metadata,rows:()=>Array.from(context._filtered,i=>i[IDX.n])};
}

test('saved research preset replay matches v1 positive and negative search and price cases',async()=>{
 const h=presetReplayHarness(),g=h.context;
 // Exact expected results were reproduced on published v1 (0e98899). Cash
 // quotes are deliberately inverted so a basis leak cannot satisfy the test.
 const cases=[
  [{searchText:'aLpHa'},['Alpha']],
  [{searchText:'missing'},[]],
  [{minPrice:'1',maxPrice:'5'},['Alpha']],
  [{minPrice:'3',maxPrice:'5'},[]],
  [{minPrice:'2',maxPrice:'2'},['Alpha']],
  [{},['Alpha','Beta']],
 ];
 for(const [filters,expected]of cases){
  g.preset={basis:'research',mode:'portfolio',...filters};
  await g.selectItemPreset('SYNTHETIC-PRESET');
  assert.deepEqual(h.rows(),expected,JSON.stringify(filters));
  assert.equal(g._itemsBasis,'research');
  // A subsequent render/sort must not discard the same saved filters.
  g.buildItemsTab(g.active.data);assert.deepEqual(h.rows(),expected);
 }
 g.active.data.items.push(['Unknown',1,null,null,null,null,'Category A','Rifle']);
 g.preset={basis:'research',mode:'portfolio'};await g.selectItemPreset('SYNTHETIC-PRESET');assert.deepEqual(h.rows(),['Alpha','Beta','Unknown']);
 g.preset={basis:'research',mode:'portfolio',minPrice:'1',maxPrice:'5'};await g.selectItemPreset('SYNTHETIC-PRESET');assert.deepEqual(h.rows(),['Alpha']);
 g.preset={basis:'cash',mode:'portfolio',minPrice:'1',maxPrice:'5'};await g.selectItemPreset('SYNTHETIC-PRESET');assert.deepEqual(h.rows(),['Beta']);
});

test('research preset rebuild restores metadata options and preserves AND tags and blacklist',async()=>{
 const h=presetReplayHarness(),g=h.context;
 // The old cash/scope select has no matching option until research is rebuilt.
 for(const id of ['item-coll-filter','item-type-filter','item-grade-filter'])g.document.getElementById(id).innerHTML='<option value="">All</option><option value="OLD">Old scope</option>';
 g.preset={basis:'research',mode:'portfolio',collection:'Collection A',type:'Rifle',grade:'Covert'};
 await g.selectItemPreset('SYNTHETIC-PRESET');assert.deepEqual(h.rows(),['Alpha']);
 assert.equal(h.elements.get('item-coll-filter').value,'Collection A');assert.equal(h.elements.get('item-type-filter').value,'Rifle');assert.equal(h.elements.get('item-grade-filter').value,'Covert');
 g.preset={basis:'research',mode:'portfolio',collection:'Collection A',type:'Pistol'};await g.selectItemPreset('SYNTHETIC-PRESET');assert.deepEqual(h.rows(),[]);
 g.preset={basis:'research',mode:'portfolio',searchText:'cOvErT',searchTerms:['rifle','event a']};await g.selectItemPreset('SYNTHETIC-PRESET');assert.deepEqual(h.rows(),['Alpha']);
 g._blTerms=['alpha'];await g.selectItemPreset('SYNTHETIC-PRESET');assert.deepEqual(h.rows(),[]);assert.deepEqual(Array.from(g._blTerms),['alpha']);
 g._blTerms=[];g.preset={basis:'research',mode:'portfolio',searchTerms:['alpha','missing']};await g.selectItemPreset('SYNTHETIC-PRESET');assert.deepEqual(h.rows(),[]);
});

test('snapshot A to B inventory race queues B and rejects stale success, error and cleanup',async()=>{
 const h=presetReplayHarness(),g=h.context,requests=[],statuses=[];
 Object.assign(g,{_snapshotLoadSeq:0,_globalScale:90,_savedReadGeneration:0,_inventoryAnalytics:{},_inventoryAnalyticsKey:'',_inventoryAnalyticsLoading:false,_inventoryAnalyticsController:null,_inventoryAnalyticsRequestKey:'',_inventoryAnalyticsPromise:null,
  portfolioCashState:{timer:null,serial:0,generation:0},canonicalCashState:{generation:0},portfolioSteamAskState:{timer:null,readSerial:0,generation:0},_itemCatalog:{},
  clearTimeout(){},renderPortfolioCash(){},portfolioCashVisible:()=>false,renderPortfolioSteamAskPanel(){},portfolioSteamAskRelevant:()=>false,
  syncScaleSelect(){},resetOverviewRepairState(){},renderStats(){},renderOverviewMetadataOnly(){},queueCurrentViewRender(){},applySteamSummary(){},
  renderDataStatus:(coverage,options)=>statuses.push(options),exactTime:()=> 'synthetic-time',console:{warn(){assert.fail('A stale request must not warn or change error status');}},
  analyticsForItem:name=>g._inventoryAnalytics[name],
  fetch:(url,options)=>new Promise((resolve,reject)=>requests.push({url,options,resolve,reject})),
 });
 g.snaps=[{id:'A',label:'Invented A',data:{items:[['Alpha',1,null,null,null,null,'Category A','Rifle']],totals:{count:1}}},{id:'B',label:'Invented B',data:{items:[['Beta',1,null,null,null,null,'Category B','Pistol']],totals:{count:1}}}];
 new vm.Script(['researchPeriodDays','analysisQuery','cancelInventoryAnalyticsRequest','requestInventoryAnalytics','indexInventoryAnalytics','loadSnapshot','stopPortfolioSteamAskRead','resetPortfolioSteamAskSelection','stopPortfolioCashRead','resetPortfolioCashSelection','portfolioCashHoldingsKey'].map(fn).join('\n')).runInContext(g);
 const response=(name,price)=>({ok:true,json:async()=>({items:[{name,marketUnitPrice:price}],coverage:{items:1,dataset:{id:'SYNTHETIC'}}})});
 await g.loadSnapshot('A');const promiseA=g._inventoryAnalyticsPromise;
 await g.loadSnapshot('B');const promiseB=g._inventoryAnalyticsPromise,ownerB=g._inventoryAnalyticsController;
 assert.equal(requests.length,2);assert.equal(new URL(requests[0].url,'https://fixture.invalid').searchParams.get('portfolio'),'A');assert.equal(new URL(requests[1].url,'https://fixture.invalid').searchParams.get('portfolio'),'B');
 assert.equal(requests[0].options.signal.aborted,true);assert.equal(g.portfolioCashState.portfolioId,'B');assert.equal(g.portfolioSteamAskState.portfolioId,'B');
 const beforeStale=statuses.length;requests[0].resolve(response('Alpha',999));await promiseA;
 assert.equal(statuses.length,beforeStale);assert.equal(g._inventoryAnalytics.Alpha,undefined);assert.equal(g._inventoryAnalyticsLoading,true);assert.equal(g._inventoryAnalyticsController,ownerB);assert.equal(g._inventoryAnalyticsRequestKey,'B:90');
 requests[1].resolve(response('Beta',20));await promiseB;
 assert.equal(g._inventoryAnalyticsKey,'B:90');assert.equal(g._inventoryAnalytics.Beta.marketUnitPrice,20);assert.equal(g._inventoryAnalytics.Alpha,undefined);assert.equal(g._inventoryAnalyticsLoading,false);assert.deepEqual(h.rows(),['Beta']);
 // Re-enter A while an older A is still pending: key equality is not ownership.
 await g.loadSnapshot('A');const oldA=g._inventoryAnalyticsPromise;
 await g.loadSnapshot('B');const oldB=g._inventoryAnalyticsPromise;
 await g.loadSnapshot('A');const latestA=g._inventoryAnalyticsPromise,latestOwner=g._inventoryAnalyticsController;
 assert.equal(requests.length,5);
 requests[2].resolve(response('Alpha',888));requests[3].reject(new Error('Invented stale error'));await Promise.all([oldA,oldB]);
 assert.equal(g._inventoryAnalytics.Alpha,undefined);assert.equal(g._inventoryAnalyticsController,latestOwner);assert.equal(g._inventoryAnalyticsLoading,true);
 requests[4].resolve(response('Alpha',2));await latestA;
 assert.equal(g._inventoryAnalyticsKey,'A:90');assert.equal(g._inventoryAnalytics.Alpha.marketUnitPrice,2);assert.equal(g._inventoryAnalyticsLoading,false);
});

test('inherited full-market All period remains zero in requests, caches and selectors',async()=>{
 const values={};const globals={_globalScale:0,active:{id:'SYNTHETIC'},_itemsBasis:'research',_itemsMode:'market',_marketItemsOffset:0,_searchTerms:[],_blTerms:[],_sortKey:'name',_sortDir:1,URLSearchParams,
  document:{getElementById:id=>id==='global-scale'?{options:[{value:'90'},{value:'0'}],set value(v){values.scale=v;}}:{value:''}}};
 const zero=run(['researchPeriodDays','marketItemsQueryState','marketItemsUrl'],'marketItemsUrl()',globals);
 assert.equal(new URL(zero,'https://fixture.invalid').searchParams.get('days'),'0');
 const allKey=run(['researchPeriodDays','taxonomyScopeKey'],"taxonomyScopeKey('market')",globals);
 assert.match(run(['researchPeriodDays','periodOptionHtml'],'periodOptionHtml(0)',globals),/<option value="0" selected>All<\/option>/);
 run(['syncScaleSelect'],'syncScaleSelect(0)',globals);assert.equal(values.scale,'0');
 globals._globalScale=90;
 const ninety=run(['researchPeriodDays','marketItemsQueryState','marketItemsUrl'],'marketItemsUrl()',globals);
 assert.equal(new URL(ninety,'https://fixture.invalid').searchParams.get('days'),'90');assert.notEqual(allKey,run(['researchPeriodDays','taxonomyScopeKey'],"taxonomyScopeKey('market')",globals));
 const requests=[];Object.assign(globals,{_savedReadGeneration:0,_taxonomyMarketKey:'',_taxonomyMarketData:null,fetch:async url=>{requests.push(url);return{ok:true,json:async()=>({dimensions:[],marker:new URL(url,'https://fixture.invalid').searchParams.get('days')})};}});
 const context=vm.createContext(globals);new vm.Script(['researchPeriodDays','taxonomyScopeKey','loadTaxonomyAnalysis'].map(fn).join('\n')).runInContext(context);
 context._globalScale=0;assert.equal((await context.loadTaxonomyAnalysis('market',false)).marker,'0');
 context._globalScale=90;assert.equal((await context.loadTaxonomyAnalysis('market',false)).marker,'90');assert.equal(requests.length,2);
});

test('missing Chart fallback targets canvas for both canvas and context callers',()=>{
 for(const contextInput of [false,true]){
  const notes=[];const parent={querySelector:()=>notes[0]||null,appendChild:node=>notes.push(node)};
  const canvas={hidden:false,parentElement:parent},input=contextInput?{canvas}:canvas;
  const globals={input,config:{type:'line'},document:{createElement:()=>({})}};
  run(['createResearchChart'],'createResearchChart(input,config);createResearchChart(input,config)',globals);
  assert.equal(canvas.hidden,true);assert.equal(notes.length,1);assert.match(notes[0].textContent,/Chart library unavailable/);
  if(contextInput)assert.equal(Object.hasOwn(input,'hidden'),false);
  const calls=[];globals.Chart=function(received,config){calls.push({received,config});};
  run(['createResearchChart'],'createResearchChart(input,config)',globals);
  assert.equal(canvas.hidden,false);assert.equal(calls[0].received,input);assert.equal(calls[0].config,globals.config);
 }
});

test('complete inline script renders research Overview, Items and both group scopes with synthetic DOM/data',async()=>{
 const elements=new Map(),chartKinds=[];
 function element(id=''){
  const classes=new Set();let content='';
  const el={id,style:{},dataset:{},value:'',checked:false,hidden:false,disabled:false,options:[],parentElement:null,
   classList:{add:n=>classes.add(n),remove:n=>classes.delete(n),contains:n=>classes.has(n),toggle:(n,v)=>v?classes.add(n):classes.delete(n)},
   addEventListener(){},removeEventListener(){},setAttribute(){},getAttribute(){return null;},querySelector(){return null;},querySelectorAll(){return [];},appendChild(){},getContext(){return {};},focus(){},getBoundingClientRect(){return {left:0,right:100,top:0,bottom:100,width:100,height:100};},
  };
  Object.defineProperty(el,'innerHTML',{get:()=>content,set:value=>{content=String(value);for(const m of content.matchAll(/\bid="([^"]+)"/g))if(!elements.has(m[1]))elements.set(m[1],element(m[1]));}});
  return el;
 }
 for(const m of markup.matchAll(/\bid="([^"]+)"/g))elements.set(m[1],element(m[1]));
 elements.get('tab-overview').classList.add('active');
 const document={hidden:false,body:{classList:{toggle(){}}},getElementById:id=>elements.get(id)||null,querySelector:selector=>selector==='.section.active'?elements.get('tab-overview'):null,querySelectorAll:()=>[],createElement:()=>element(),addEventListener(){}};
 class Chart {static defaults={font:{}};constructor(canvas,config){chartKinds.push(config.type);}destroy(){}update(){}}
 const fakeFetch=async()=>({ok:true,status:200,json:async()=>({})});
 const window={fetch:fakeFetch,location:{href:'https://synthetic.invalid/'},addEventListener(){},innerWidth:1280,innerHeight:900};
 const context=vm.createContext({document,window,fetch:fakeFetch,URL,URLSearchParams,AbortController,Chart,localStorage:{getItem:()=>null,setItem(){},removeItem(){}},setTimeout:()=>1,clearTimeout(){},setInterval:()=>1,clearInterval(){},console:{warn(){},error(){},log(){}},Intl,Date});
 const app=[...source.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g)].map(m=>m[1]).join('\n').replace(/^init\(\);$/m,'/* explicit startup omitted in synthetic smoke */').replace(/^loadBlacklist\(\);$/m,'/* no shared settings reads */');
 new vm.Script(app).runInContext(context,{timeout:3000});
 const formatted=new vm.Script(`({missing:fmt(null),bad:fmt(Infinity),zero:fmt(0),missingDelta:fmtD(null),zeroDelta:fmtD(0),missingPct:fmtP(null),badPct:fmtP(NaN),zeroPct:fmtP(0)})`).runInContext(context);
 assert.equal(formatted.missing,'Unavailable');assert.equal(formatted.bad,'Unavailable');assert.equal(formatted.zero,'$0');assert.equal(formatted.missingDelta,'Unavailable');assert.equal(formatted.zeroDelta,'+$0');assert.equal(formatted.missingPct,'Unavailable');assert.equal(formatted.badPct,'Unavailable');assert.equal(formatted.zeroPct,'+0.00%');
 new vm.Script(`
 const syntheticRows=Array.from({length:12},(_,i)=>['SYNTHETIC '+i,1,null,null,null,null,'Synthetic','Synthetic']);
 active={id:'synthetic',label:'Synthetic fixture',data:{items:syntheticRows,totals:{curr:null,past:null,dollar:null,count:12},categories:[],rollup:[],topGainers:[],topLosers:[]}};snaps=[active];
 _inventoryAnalytics=Object.fromEntries(syntheticRows.map(row=>[row[0],{name:row[0],marketCurr:12,marketPast:10,marketUnitPrice:12,marketPriorUnitPrice:10,marketDollar:2,marketPct:20,providerPrices:{},hasPrior:true}]));
 _portfolioCurve={ok:false,points:[],error:'Synthetic raw history unavailable'};
 requestPortfolioCurve=async()=>{};loadTaxonomyAnalysis=async()=>({dimensions:[],coverage:{}});
 renderOverview(active.data);buildItemsTab(active.data);
 `).runInContext(context,{timeout:3000});
 assert.ok(chartKinds.includes('doughnut'));assert.ok(chartKinds.includes('bar'));
 assert.match(elements.get('items-container').innerHTML,/Provider benchmark/);
 assert.match(elements.get('items-tbody').innerHTML,/SYNTHETIC/);
 new vm.Script(`processText(snapshotExportText(active),'SYNTHETIC holdings.txt')`).runInContext(context,{timeout:3000});
 assert.match(elements.get('parse-preview').innerHTML,/Imported benchmark value \(not current cash\)/);
 assert.match(elements.get('parse-preview').innerHTML,/Unavailable/);
 assert.doesNotMatch(elements.get('parse-preview').innerHTML,/\$0/);
 await new vm.Script(`(async()=>{
 loadTaxonomyAnalysis=async(scope)=>{
  var data={scope:scope,coverage:{},dimensions:['collection','type','event'].map(dimension=>({dimension:dimension,leaders:[{dimension:dimension,name:'SYNTHETIC GROUP',count:1,priced:1,currentValue:12,priorValue:10,priorCoverageItems:1,weightedChangePct:20,changeDollar:2,avgChangePct:null,avgVolatilityPct:null,topMovers:[],topVolatile:[]}]}))};
  if(scope==='market')_taxonomyMarketData=data;else _taxonomyPortfolioData=data;return data;
 };
 await buildBreakdown(active.data);await buildMarketAnalysis();
 })()`).runInContext(context,{timeout:3000});
 assert.match(elements.get('bk-table-wrap').innerHTML,/SYNTHETIC GROUP/);
 assert.match(elements.get('market-bk-table-wrap').innerHTML,/SYNTHETIC GROUP/);
 assert.match(elements.get('bk-table-wrap').innerHTML,/portfolio holdings/);
 assert.match(elements.get('market-bk-table-wrap').innerHTML,/one-each basket/);
});
