/**
 * REAL browser acceptance against serve_v1_fixture.py ONLY. No browser is
 * launched unless CS2_V1_BROWSER_RELEASE=GRANTED and fixture identity verifies.
 *
 * PLAYWRIGHT_MODULE may name an installed package, module path, or file URL.
 * No npm install, mock browser, substituted Chart implementation or live-site
 * access is used. Run inside a bounded CPU/memory/process job.
 */
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';

if(process.env.CS2_V1_BROWSER_RELEASE!=='GRANTED')throw new Error('No browser/resource lease. Nothing launched.');
const started=Date.now(),budgetMs=Number(process.env.CS2_V1_BROWSER_BUDGET_MS||150000),deadline=started+budgetMs;
assert.ok(Number.isFinite(budgetMs)&&budgetMs>=10000&&budgetMs<=180000,'Browser budget must be 10..180 seconds');
const base=new URL(process.env.CS2_V1_FIXTURE_BASE||'http://127.0.0.1:8097');
assert.equal(base.origin,'http://127.0.0.1:8097','Only the invented loopback fixture on8097 is permitted');
assert.equal(base.pathname,'/');assert.equal(base.username,'');assert.equal(base.password,'');
const fixtureResponse=await fetch(new URL('/api/fixture-info',base),{signal:AbortSignal.timeout(5000)});
assert.equal(fixtureResponse.status,200);
const fixture=await fixtureResponse.json();
assert.equal(fixture.fixture,true);assert.equal(fixture.allDataInvented,true);
assert.equal(fixture.providerNetworkEnabled,false);assert.equal(fixture.ownerCanEdit,true);
const repo=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const output=path.resolve(process.env.CS2_V1_BROWSER_OUTPUT||path.join(repo,'..','cs2-04-private','browser-acceptance-'+new Date().toISOString().replace(/[:.]/g,'-')));
await fs.mkdir(output,{recursive:true});
let moduleSpecifier=process.env.PLAYWRIGHT_MODULE||(process.platform==='win32'?'file:///D:/Projects/site/node_modules/playwright/index.mjs':'playwright');
if(!moduleSpecifier.startsWith('file:')&&(path.isAbsolute(moduleSpecifier)||/^[A-Za-z]:[\\/]/.test(moduleSpecifier)))moduleSpecifier=pathToFileURL(moduleSpecifier).href;
const {chromium}=await import(moduleSpecifier);
const report={id:'CS2-04',kind:'real_browser_invented_fixture',startedAt:new Date(started).toISOString(),fixtureRunId:fixture.runId||null,frontendSha256:fixture.frontendSha256,budgetSeconds:budgetMs/1000,sandboxRequired:true,oneContext:true,allDataInvented:true,liveSiteAccess:false,cases:[],blockedRequests:[],pageErrors:[],requestEvidence:[],screenshots:[],limits:['Provider acquisition is deliberately disabled in this fixture.','Historical signal rules are exercised as existing v1 behavior, not validated as investment forecasts.','All screenshots and downloads contain invented data only.']};
const requests=[];let browser,context,page,importedLabel='',importedPortfolioSaved=false,downloadText='';
const hardStop=setTimeout(async()=>{report.budgetExceeded=true;try{await browser?.close();}catch{}},Math.max(1,deadline-Date.now()));
const chartCdn='https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js';
const cases=[];
const register=(id,title,run)=>cases.push({id,title,run});
const tab=async name=>{
 await page.locator('.tab').filter({hasText:new RegExp('^'+name+'$')}).click();
 const id={Overview:'overview',Market:'market',Breakdown:'categories',Items:'items',History:'history'}[name];
 await page.locator('#tab-'+id+'.active').waitFor({state:'visible'});
};
const ready=async()=>{
 await page.locator('#app-loading').waitFor({state:'hidden',timeout:10000});
 await page.waitForFunction(()=>active&&active.id&&active.data&&Array.isArray(active.data.items),null,{timeout:10000});
};
const inventorySettled=()=>page.waitForFunction(()=>active&&!_inventoryAnalyticsLoading&&_inventoryAnalyticsKey===active.id+':'+_globalScale,null,{timeout:10000});
const marketSettled=()=>page.waitForFunction(()=>_itemsMode!=='market'||(!_marketItemsLoading&&_marketItemsKey===marketItemsQueryKey()&&document.getElementById('items-container').getAttribute('aria-busy')!=='true'),null,{timeout:10000});
const advanced=async id=>{
 await marketSettled();
 const details=page.locator('#items-advanced-filters');
 if(await details.getAttribute('open')===null)await details.locator(':scope > summary').click();
 const control=page.locator('#'+id);await control.waitFor({state:'visible'});return control;
};
const fillAdvanced=async(id,value)=>{await(await advanced(id)).fill(value);await marketSettled();};
const selectAdvanced=async(id,value)=>{await(await advanced(id)).selectOption(value);await marketSettled();};
const clearFilters=async()=>{
 await advanced('item-min-price');
 await page.locator('#items-advanced-filters button').filter({hasText:/^Clear All$/}).click();
 await marketSettled();
};
const items=async(basis,mode)=>{
 await tab('Items');
 if(await page.locator('#items-basis').inputValue()!==basis){await page.locator('#items-basis').selectOption(basis);await marketSettled();}
 const modeButton=page.locator('#items-mode-'+mode);
 if(!((await modeButton.getAttribute('class'))||'').split(/\s+/).includes('active'))await modeButton.click();
 if(mode==='market')await marketSettled();else await inventorySettled();
 await page.locator('#items-table.'+(mode==='market'?'market':'portfolio')+'-items-table').waitFor();
};
const selectPortfolio=async id=>{await page.locator('#snap-select').selectOption(id);await page.waitForFunction(id=>active&&active.id===id,id);await inventorySettled();};
const expectText=async(selector,pattern)=>{await page.waitForFunction(({selector,pattern})=>new RegExp(pattern).test(document.querySelector(selector)?.textContent||''),{selector,pattern:pattern.source},{timeout:9000});};
const latestMarketRequest=(basis)=>[...requests].reverse().find(r=>r.path===(basis==='cash'?'/api/current-market-items':'/api/market-items'));

register('NAV-01/03,DAT-03..05,OV-02..12','Tabs, research periods, widgets and explicit job controls',async()=>{
 for(const name of ['Overview','Market','Breakdown','Items','History'])await tab(name);
 await tab('Overview');await inventorySettled();
 await page.waitForFunction(()=>typeof Chart==='function'&&!!Chart.getChart(document.getElementById('pie-chart')),null,{timeout:9000});
 assert.deepEqual(await page.locator('#global-scale option').evaluateAll(options=>options.map(o=>o.value)),['1','3','7','30','90','180','365','0']);
 for(const period of ['7','30','90','0','90']){await page.locator('#global-scale').selectOption(period);await inventorySettled();}
 for(const period of ['7','90']){await page.locator('#overview-curve-days').selectOption(period);await page.waitForFunction(()=>!_portfolioCurveLoading&&_portfolioCurve&&_portfolioCurve.points?.length>1);}
 for(const id of ['overview-action-brief','overview-risk-controls','overview-rankings'])await expectText('#'+id,/\S/);
 for(const id of ['ov-return-top','ov-return-bottom','ov-return-split'])await page.locator('#'+id).click();
 await page.locator('#data-menu > button').click();
 await page.locator('#data-task-mode').selectOption('provider_snapshots');
 await page.locator('#data-pull-mode').selectOption('gentle');
 await page.locator('#data-history-limit').fill('0');await page.locator('#data-task-cycles').fill('1');
 await page.locator('#data-retry-failures').uncheck();assert.equal(await page.locator('#data-retry-failures').isChecked(),false);
 await page.locator('#data-retry-failures').check();assert.equal(await page.locator('#data-retry-failures').isChecked(),true);
 assert.equal(await page.locator('#data-task-mode').inputValue(),'provider_snapshots');
 assert.equal(await page.locator('#data-task-start-btn').textContent(),'Start Background Fill');
 await page.locator('.logo-text h1').click();
 return {tabsActivated:5,globalPeriodsExercised:['7','30','90','0'],curvePeriodsExercised:['7','90'],returnModesExercised:3,acquisition:'controls exercised; no job requested because fixture forbids providers'};
});
register('OV-07,IO-09','Decision note survives a real page reload',async()=>{
 await tab('Overview');const note='SYNTHETIC browser acceptance note';
 await page.locator('#decision-note').fill(note);
 await page.waitForFunction(value=>localStorage.getItem('cs2dash_decision_note_v1')===value,note);
 await page.reload({waitUntil:'domcontentloaded'});await ready();
 assert.equal(await page.locator('#decision-note').inputValue(),note);return {savedAndReloaded:true};
});
register('IT-07..09','Preset add, select, rename and delete through real controls',async()=>{
 await items('research','portfolio');await clearFilters();
 const manage=page.locator('.item-preset-manage');if(await manage.getAttribute('open')===null)await manage.locator(':scope > summary').click();
 await page.locator('#item-preset-name').fill('SYNTHETIC acceptance preset');await page.locator('#item-preset-icon').selectOption('!');
 await manage.getByRole('button',{name:'Add Current Filters',exact:true}).click();
 const preset=page.locator('.item-preset-btn').filter({hasText:'SYNTHETIC acceptance preset'});await preset.click();
 await page.locator('#item-preset-name').fill('SYNTHETIC renamed preset');await page.locator('#item-preset-rename-btn').click();
 await page.locator('.item-preset-btn').filter({hasText:'SYNTHETIC renamed preset'}).waitFor();
 await page.locator('#item-preset-delete-btn').click();
 assert.equal(await page.locator('.item-preset-btn').filter({hasText:'SYNTHETIC renamed preset'}).count(),0);
 const stored=await page.evaluate(()=>JSON.parse(localStorage.getItem('cs_item_filter_presets_v1')||'[]'));
 assert.ok(!stored.some(p=>String(p.name).startsWith('SYNTHETIC acceptance')||p.name==='SYNTHETIC renamed preset'));
 return {addedSelectedRenamedDeleted:true,systemPresets:await page.locator('.item-preset-btn').count()};
});
async function groupFlow(scope){
 const market=scope==='market',prefix=market?'market-bk':'bk',section=market?'#tab-market':'#tab-categories';
 await tab(market?'Market':'Breakdown');await expectText('#'+prefix+'-table-wrap',/SYNTHETIC|Synthetic/);
 for(const lens of ['source','type','event']){
  await page.locator(section+' .bk-view-btn[onclick^="'+(market?'switchMarketBkView':'switchBkView')+'(\''+lens+'\'"]').click();
  await page.waitForFunction(({market,lens})=>(market?_marketBkView:_bkView)===lens,{market,lens});
 }
 await page.locator(section+' .bk-view-btn').first().click();
 for(const sort of ['value','dollar','pct','vol7','vol30','vol90','count','name'])await page.locator('#'+prefix+'-sort-'+sort).click();
 const groupValueWait=page.waitForResponse(response=>{const url=new URL(response.url());return url.origin===base.origin&&url.pathname==='/api/group-history'&&url.searchParams.get('scope')===scope;});
 const first=page.locator('#'+prefix+'-table-wrap .bk-row.expandable').first();await first.click();
 const groupValue=await groupValueWait;assert.ok(groupValue.ok());assert.ok((await groupValue.json()).points?.length>1);
 const detail=page.locator('#'+prefix+'-table-wrap .bk-expand-row:visible').first();await detail.waitFor({state:'visible'});
 const groupLinesWait=page.waitForResponse(response=>{const url=new URL(response.url());return url.origin===base.origin&&url.pathname==='/api/group-item-history'&&url.searchParams.get('scope')===scope&&url.searchParams.get('days')==='7';});
 const period=detail.locator('select[id$="-days"]');await period.selectOption('7');
 const groupLines=await groupLinesWait;assert.ok(groupLines.ok());assert.ok((await groupLines.json()).series?.length>0);
 await page.waitForFunction(({prefix})=>[...document.querySelectorAll('#'+prefix+'-table-wrap .bk-expand-row')].some(row=>row.style.display!=='none'&&[...row.querySelectorAll('canvas')].some(canvas=>Chart.getChart(canvas))), {prefix});
 return {scope,lenses:3,sorts:8,drilldownOpened:true,detailPeriod:'7'};
}
register('GRP-01/04..08','Portfolio Breakdown lenses, sorts and group drilldown',()=>groupFlow('portfolio'));
register('GRP-02..09','Full-market lenses, sorts and independent group drilldown',()=>groupFlow('market'));
register('IT-01/10/11/14/16','Research/cash bases in both portfolio and catalog scopes',async()=>{
 const evidence=[];
 for(const basis of ['research','cash'])for(const mode of ['portfolio','market']){
  await items(basis,mode);await clearFilters();
  const headers=await page.locator('#items-table thead').textContent();
  assert.ok(headers.includes(basis==='research'?'Provider benchmark':'Cash price'));
  if(basis==='research')assert.ok(headers.includes('Move %')&&headers.includes('CSFloat'));
  assert.ok(await page.locator('#items-tbody .item-main-row').count()>0);
  await page.locator('#items-tbody .item-main-row').first().click();
  const detail=page.locator('#items-tbody .item-detail-row:visible').first();await detail.waitFor({state:'visible'});
  if(basis==='research')assert.ok(await detail.locator('.steam-history-box').count()>0);
  else await detail.locator('[data-current-sources]').waitFor({state:'attached'});
  evidence.push({basis,mode,expandedDetail:true});
 }
 return {combinations:evidence};
});
register('IT-05/12/13','Global price bounds, true sorting and server pagination in both bases',async()=>{
 const evidence=[];
 for(const basis of ['research','cash']){
  await items(basis,'market');await clearFilters();
  await page.locator('#items-table th[data-sort-key="latestPrice"]').click();await marketSettled();
  if(await page.evaluate(()=>_sortDir)!==1){await page.locator('#items-table th[data-sort-key="latestPrice"]').click();await marketSettled();}
  await expectText('#items-tbody .item-main-row:first-child',new RegExp(fixture.cheapItemBeyondAlphabetic500));
  await fillAdvanced('item-min-price','0');await fillAdvanced('item-max-price','1');
  assert.equal(await page.locator('#items-tbody .item-main-row').count(),1);
  await expectText('#items-tbody',new RegExp(fixture.cheapItemBeyondAlphabetic500));
  const bounded=latestMarketRequest(basis);assert.equal(bounded.sort,'latestPrice');assert.equal(bounded.minimum,'0');assert.equal(bounded.maximum,'1');
  await clearFilters();await page.locator('#items-page-next').click();await marketSettled();
  const paged=latestMarketRequest(basis);assert.equal(paged.offset,'500');
  assert.ok(await page.locator('#items-tbody .item-main-row').count()>0);
  await page.locator('#items-page-previous').click();await marketSettled();assert.equal(latestMarketRequest(basis).offset,'0');
  evidence.push({basis,cheapBeyond500Found:true,boundsSent:true,pageOffsetVerified:500});
 }
 return {bases:evidence};
});
register('IT-02/04','AND search chips, inline search and collection/type/grade filters',async()=>{
 await items('research','market');await clearFilters();
 for(const term of ['Synthetic','Rifle']){await page.locator('#item-search').fill(term);await page.locator('#item-search').press('Enter');await marketSettled();}
 assert.equal(await page.evaluate(()=>_searchTerms.length),2);
 for(const [id,key]of [['item-coll-filter','collection'],['item-type-filter','type'],['item-grade-filter','grade']])await selectAdvanced(id,fixture.filterExample[key]);
 assert.equal(await page.locator('#items-tbody .item-main-row').count(),2);
 await page.locator('#item-search').fill('Alpha');await marketSettled();assert.equal(await page.locator('#items-tbody .item-main-row').count(),1);
 await clearFilters();assert.equal(await page.evaluate(()=>_searchTerms.length),0);
 return {andChips:2,metadataFilters:3,inlineNarrowingVerified:true};
});
register('IO-06/08','Null-safe actual TXT download and pasted reimport preview',async()=>{
 await selectPortfolio(fixture.initialPortfolioId);await tab('History');
 const label=fixture.portfolios.find(p=>p.id===fixture.initialPortfolioId).label;
 const card=page.locator('#snap-cards .snap-card').filter({has:page.locator('h3').filter({hasText:label})});
 const downloadWait=page.waitForEvent('download');await card.getByRole('button',{name:'Export',exact:true}).click();const download=await downloadWait;
 const file=path.join(output,'synthetic-null-holdings.txt');await download.saveAs(file);downloadText=await fs.readFile(file,'utf8');
 assert.match(downloadText,/CS2Dash holdings export v2/);assert.match(downloadText,/N\/A/);
 await page.getByRole('button',{name:'+ Import',exact:true}).click();await page.locator('#paste-input').fill(downloadText);
 await page.locator('#parse-result').waitFor({state:'visible'});
 const parsed=await page.evaluate(()=>_parsedData.items.map(row=>({name:row[IDX.n],qty:row[IDX.q]})));
 assert.deepEqual(parsed.sort((a,b)=>a.name.localeCompare(b.name)),[...fixture.heavyHoldings].sort((a,b)=>a.name.localeCompare(b.name)));
 assert.ok((await page.locator('#parse-preview').innerText()).includes('Unavailable'));
 await page.locator('.modal-close').click();return {downloaded:true,reimportPreview:true,identityQuantityPreserved:true,unknownPricesRemainUnknown:true};
});
register('IO-01..04','Synthetic CSV upload, duplicate preview and actual fixture save',async()=>{
 await page.getByRole('button',{name:'+ Import',exact:true}).click();await page.getByRole('button',{name:'Upload File',exact:true}).click();
 const rows=Array.from({length:10},(_,i)=>`"Synthetic browser import ${i}",1,N/A,N/A,N/A,N/A`);
 rows.push(rows[0]);rows.push('"Synthetic browser import 1",2,N/A,N/A,N/A,N/A');
 const csv=['Item Name,Amount,Past Price,Current Price,Return %,Return',...rows].join('\n');
 await page.locator('#file-input').setInputFiles({name:'synthetic-invented.csv',mimeType:'text/csv',buffer:Buffer.from(csv)});
 await page.locator('#parse-result').waitFor({state:'visible'});await expectText('#parse-preview',/Duplicates collapsed/);
 const parsed=await page.evaluate(()=>({count:_parsedData.items.length,duplicateCount:_parsedData.importStats.exactDuplicates,mergedCount:_parsedData.importStats.mergedRows,mergedQty:_parsedData.items.find(row=>row[0]==='Synthetic browser import 1')[1]}));
 assert.deepEqual(parsed,{count:10,duplicateCount:1,mergedCount:1,mergedQty:3});
 importedLabel='SYNTHETIC browser acceptance import '+Date.now();await page.locator('#snap-label').fill(importedLabel);
 const saved=page.waitForResponse(r=>r.url()===new URL('/api/portfolios',base).href&&r.request().method()==='POST');
 await page.locator('#parse-result').getByRole('button',{name:'Save Portfolio',exact:true}).click();assert.ok((await saved).ok());importedPortfolioSaved=true;
 await page.waitForFunction(label=>active?.label===label,importedLabel);await tab('History');await expectText('#snap-cards',new RegExp(importedLabel));
 return {...parsed,savedToInventedFixtureOnly:true};
});
register('IO-06/07/10/11,NAV-05','Snapshot View, benchmark History, real report and fixture cleanup',async()=>{
 await selectPortfolio('synthetic-light');await tab('History');
 assert.ok(await page.locator('#snap-cards .snap-card').count()>=2);
 assert.equal(await page.locator('#trend-chart').isVisible(),true);
 const heavyLabel=fixture.portfolios.find(p=>p.id===fixture.initialPortfolioId).label;
 const heavy=page.locator('#snap-cards .snap-card').filter({has:page.locator('h3').filter({hasText:heavyLabel})});await heavy.getByRole('button',{name:'View',exact:true}).click();
 await page.waitForFunction(id=>active?.id===id,fixture.initialPortfolioId);await inventorySettled();
 const popupWait=page.waitForEvent('popup');await page.getByRole('link',{name:'Report',exact:true}).click();const popup=await popupWait;await popup.waitForLoadState('domcontentloaded');
 assert.equal(new URL(popup.url()).origin,base.origin);assert.equal(new URL(popup.url()).pathname,'/report');
 for(const heading of ['Category Momentum','Full Item Appendix'])assert.ok(await popup.getByRole('heading',{name:heading,exact:true}).count()>0);
 await popup.screenshot({path:path.join(output,'synthetic-report.png'),fullPage:false});report.screenshots.push('synthetic-report.png');await popup.close();
 if(importedPortfolioSaved){await tab('History');const imported=page.locator('#snap-cards .snap-card').filter({has:page.locator('h3').filter({hasText:importedLabel})});
  const deleted=page.waitForResponse(r=>new URL(r.url()).origin===base.origin&&new URL(r.url()).pathname.startsWith('/api/portfolios/')&&r.request().method()==='DELETE');
  await imported.getByRole('button',{name:'Delete',exact:true}).click();assert.ok((await deleted).ok());await imported.waitFor({state:'detached'});
 }
 return {snapshotViewed:true,benchmarkChartVisible:true,reportSectionsVerified:2,syntheticImportDeleted:importedPortfolioSaved};
});
register('UX-01..06','320/390/768 mobile geometry, local table scroll and synthetic screenshots',async()=>{
 const geometry=[];
 for(const width of [320,390,768]){
  await page.setViewportSize({width,height:900});await tab('Overview');
  let boxes=await page.locator('.header-center,.header-right,.resp-tabs').evaluateAll(elements=>elements.map(el=>{const b=el.getBoundingClientRect();return {left:b.left,right:b.right,width:b.width};}));
  assert.ok(boxes.every(b=>b.left>=-1&&b.right<=width+1),'Header controls must fit viewport');
  await page.screenshot({path:path.join(output,`synthetic-${width}-overview.png`),fullPage:false});report.screenshots.push(`synthetic-${width}-overview.png`);
  await items('research','market');await clearFilters();
  const table=await page.locator('.item-table-wrap').evaluate(el=>{const b=el.getBoundingClientRect();el.scrollLeft=el.scrollWidth;return {left:b.left,right:b.right,client:el.clientWidth,scroll:el.scrollWidth,moved:el.scrollLeft>0,overflow:getComputedStyle(el).overflowX};});
  assert.ok(table.left>=-1&&table.right<=width+1);assert.ok(table.scroll>table.client&&table.moved&&table.overflow==='auto');
  await page.screenshot({path:path.join(output,`synthetic-${width}-items.png`),fullPage:false});report.screenshots.push(`synthetic-${width}-items.png`);geometry.push({width,headerFits:true,tableScrollReachable:true});
 }
 await page.setViewportSize({width:1280,height:900});return {viewports:geometry};
});

const selectedNumbers=(process.env.CS2_V1_BROWSER_CASES||'').split(',').filter(Boolean).map(Number);
assert.ok(selectedNumbers.every(n=>Number.isInteger(n)&&n>=1&&n<=cases.length),'Requested browser case is outside the finite plan');
assert.equal(new Set(selectedNumbers).size,selectedNumbers.length,'Duplicate browser case selection');
const plannedCases=cases.map((item,index)=>({...item,number:index+1})).filter(item=>!selectedNumbers.length||selectedNumbers.includes(item.number));
report.registeredCases=cases.length;
report.selectedCases=plannedCases.map(item=>({number:item.number,id:item.id}));
report.scope=selectedNumbers.length?'selected cases only':'complete prepared browser plan';

try{
 const launchBudget=Math.min(15000,deadline-Date.now());assert.ok(launchBudget>1000,'No browser startup budget remains');
 browser=await chromium.launch({headless:true,chromiumSandbox:true,timeout:launchBudget,args:['--renderer-process-limit=1','--js-flags=--max-old-space-size=256']});
 if(report.budgetExceeded)throw new Error('Browser startup exhausted the owned run budget');
 context=await browser.newContext({viewport:{width:1280,height:900},serviceWorkers:'block',acceptDownloads:true});context.setDefaultTimeout(7000);
 await context.route('**/*',async route=>{
  const request=route.request(),url=new URL(request.url());
  const isFixture=url.origin===base.origin,isChart=request.url()===chartCdn&&request.method()==='GET';
  if(!isFixture&&!isChart){report.blockedRequests.push({method:request.method(),origin:url.origin,path:url.pathname.slice(0,160)});return route.abort('blockedbyclient');}
  if(!isFixture&&!['GET','HEAD'].includes(request.method()))return route.abort('blockedbyclient');
  if(isFixture&&['/api/market-items','/api/current-market-items'].includes(url.pathname)){
   const q=url.searchParams,entry={path:url.pathname,sort:q.get('sort'),direction:q.get('dir'),minimum:q.get('min_price'),maximum:q.get('max_price'),offset:q.get('offset'),limit:q.get('limit')};requests.push(entry);report.requestEvidence.push(entry);
  }
  return route.continue();
 });
 context.on('page',opened=>{
  opened.on('pageerror',error=>report.pageErrors.push(String(error.message).slice(0,800)));
  opened.on('dialog',async dialog=>{if(dialog.type()==='prompt')await dialog.accept('SYNTHETIC browser response');else await dialog.accept();});
 });
 page=await context.newPage();
 await page.goto(base.href,{waitUntil:'domcontentloaded',timeout:15000});await ready();
 for(const item of plannedCases){
  if(Date.now()>deadline-3000||report.budgetExceeded){report.cases.push({number:item.number,id:item.id,title:item.title,status:'not_run_budget'});continue;}
  const tick=Date.now();try{const evidence=await item.run();report.cases.push({number:item.number,id:item.id,title:item.title,status:'passed',milliseconds:Date.now()-tick,evidence});}
  catch(error){const failure={number:item.number,id:item.id,title:item.title,status:'failed',milliseconds:Date.now()-tick,error:String(error.message).slice(0,1400)};report.cases.push(failure);
   try{const file='synthetic-failure-'+report.cases.length+'.png';await page.screenshot({path:path.join(output,file),timeout:3000});report.screenshots.push(file);}catch{}
   if(!report.budgetExceeded)try{await page.goto(base.href,{waitUntil:'domcontentloaded',timeout:7000});await ready();}catch{}
  }
  console.log(JSON.stringify({case:item.id,status:report.cases.at(-1).status,milliseconds:Date.now()-tick}));
 }
}catch(error){report.harnessError=String(error.message).slice(0,1800);}
finally{
 clearTimeout(hardStop);try{await context?.close();}catch{}
 try{await browser?.close();report.browserCloseConfirmed=true;}catch(error){report.browserCloseConfirmed=false;report.browserCloseError=String(error.message).slice(0,300);}
 report.finishedAt=new Date().toISOString();report.elapsedMilliseconds=Date.now()-started;
 report.status=report.browserCloseConfirmed&&!report.harnessError&&!report.budgetExceeded&&report.pageErrors.length===0&&report.cases.length===plannedCases.length&&report.cases.every(c=>c.status==='passed')?'passed':'incomplete_or_failed';
 await fs.writeFile(path.join(output,'results.json'),JSON.stringify(report,null,2)+'\n');
 console.log(JSON.stringify({status:report.status,cases:report.cases.length,passed:report.cases.filter(c=>c.status==='passed').length,output,elapsedMilliseconds:report.elapsedMilliseconds,browserCloseConfirmed:report.browserCloseConfirmed}));
}
if(report.status!=='passed')process.exitCode=1;
