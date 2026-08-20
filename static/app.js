let charts = {};
let streamTimer = null;
let columnsCache = [];

const $ = id => document.getElementById(id);
const $$ = selector => Array.from(document.querySelectorAll(selector));

const themeToggle = $("themeToggle");
const savedTheme = localStorage.getItem("dataguard-theme");
if(savedTheme === "light") document.body.classList.add("light-theme");
if(themeToggle){
  const updateThemeControl = () => {
    const light = document.body.classList.contains("light-theme");
    themeToggle.textContent = light ? "☾" : "☀";
    themeToggle.title = light ? "Switch to dark theme" : "Switch to light theme";
    themeToggle.setAttribute("aria-label", themeToggle.title);
  };
  updateThemeControl();
  themeToggle.addEventListener("click",()=>{
    document.body.classList.toggle("light-theme");
    localStorage.setItem("dataguard-theme", document.body.classList.contains("light-theme") ? "light" : "dark");
    updateThemeControl();
  });
}

const pageMeta = {
  overview:["Overview","Reusable batch, validation, streaming and data-quality workspace."],
  sources:["Data Sources","Bring files and future connectors into one ingestion layer."],
  explorer:["Data Explorer","Profile and visualize the structure of your dataset clearly."],
  validation:["Validation Studio","Find data-quality problems, choose fixes, preview the result and revalidate."],
  quality:["DQ Breakdown","Measure completeness, validity, uniqueness, consistency and integrity."],
  streaming:["Kafka Monitor","Watch live events, offsets, lag and validation results."],
  spark:["Spark Performance","Inspect partitions, throughput, skew and processing metrics."],
  exports:["Export Center","Download every useful layer of the pipeline in the format you need."],
  history:["Run History","Audit validation runs, duration, throughput and data-quality outcomes."],
  integration:["Integration Studio","Combine related datasets, harmonize join keys and create analytics-ready data."]
};

function toast(message){
  const el = $("toast");
  el.textContent = message;
  el.classList.add("show");
  clearTimeout(el._timer);
  el._timer = setTimeout(()=>el.classList.remove("show"),2200);
}

async function api(url, options={}){
  const res = await fetch(url, options);
  const data = await res.json().catch(()=>({}));
  if(!res.ok) throw new Error(data.error || "Request failed");
  return data;
}

function escapeHtml(value){
  return String(value ?? "")
    .replaceAll("&","&amp;")
    .replaceAll("<","&lt;")
    .replaceAll(">","&gt;")
    .replaceAll('"',"&quot;")
    .replaceAll("'","&#039;");
}

function formatBytes(bytes){
  if(!Number.isFinite(bytes)) return "";
  if(bytes < 1024) return `${bytes} B`;
  if(bytes < 1024*1024) return `${(bytes/1024).toFixed(1)} KB`;
  return `${(bytes/(1024*1024)).toFixed(1)} MB`;
}

function initDragScroll(el){
  if(!el || el.dataset.dragScrollReady === "1") return;
  el.dataset.dragScrollReady = "1";
  let down=false,startX=0,startY=0,left=0,top=0,moved=false;
  el.addEventListener("pointerdown",e=>{
    if(e.pointerType === "mouse" && e.button !== 0) return;
    if(e.target.closest("button,a,input,select,textarea")) return;
    down=true;moved=false;startX=e.clientX;startY=e.clientY;left=el.scrollLeft;top=el.scrollTop;
    el.setPointerCapture?.(e.pointerId);
  });
  el.addEventListener("pointermove",e=>{
    if(!down) return;
    const dx=e.clientX-startX,dy=e.clientY-startY;
    if(Math.abs(dx)>4 || Math.abs(dy)>4){moved=true;el.classList.add("dragging");}
    if(moved){el.scrollLeft=left-dx;el.scrollTop=top-dy;e.preventDefault();}
  });
  const end=()=>{down=false;moved=false;el.classList.remove("dragging")};
  el.addEventListener("pointerup",end);el.addEventListener("pointercancel",end);el.addEventListener("pointerleave",()=>{if(down) end();});
}

function initAllDragScroll(){
  document.querySelectorAll(".table-wrap,.affected-table-wrap,.chat-sample-wrap").forEach(initDragScroll);
}

function renderTable(targetId, rows, actionsRenderer=null){
  const target = $(targetId);
  if(!target) return;
  if(!rows || rows.length === 0){
    target.className = "table-wrap empty-state";
    target.innerHTML = "No data available.";
    return;
  }
  const cols = Object.keys(rows[0]);
  let html = "<table><thead><tr>";
  cols.forEach(c => html += `<th>${escapeHtml(c)}</th>`);
  if(actionsRenderer) html += "<th>Actions</th>";
  html += "</tr></thead><tbody>";
  rows.forEach(row => {
    html += "<tr>";
    cols.forEach(c => {
      let v = row[c];
      if(typeof v === "object" && v !== null) v = JSON.stringify(v);
      const numeric = typeof v === "number" || (typeof v === "string" && v.trim() !== "" && !Number.isNaN(Number(v)));
      html += `<td class="${numeric ? 'numeric-cell' : ''}">${escapeHtml(v)}</td>`;
    });
    if(actionsRenderer) html += `<td>${actionsRenderer(row)}</td>`;
    html += "</tr>";
  });
  html += "</tbody></table>";
  target.className = "table-wrap";
  target.innerHTML = html;
  initDragScroll(target);
}

function destroyChart(name){
  if(charts[name]){ charts[name].destroy(); charts[name] = null; }
}

function switchPage(page){
  $$(".nav-item").forEach(x => x.classList.toggle("active",x.dataset.page===page));
  $$(".page").forEach(x => x.classList.toggle("active",x.id===`page-${page}`));
  const meta = pageMeta[page] || [page,""];
  $("pageTitle").textContent = meta[0];
  $("pageSubtitle").textContent = meta[1];
  if(page === "explorer") refreshProfile();
  if(page === "integration") refreshIntegration();
  if(page === "validation") refreshValidationArea();
  if(page === "quality") refreshDQ();
  if(page === "spark") refreshPerformanceAndPartitions();
  if(page === "history") refreshHistory();
}

$$(".nav-item").forEach(btn => btn.addEventListener("click",()=>switchPage(btn.dataset.page)));
$$('[data-goto]').forEach(btn => btn.addEventListener("click",()=>switchPage(btn.dataset.goto)));

async function refreshColumns(){
  columnsCache = await api("/api/columns");
  renderColumnCheckboxes();
  ["visualColumn","partitionKey"].forEach(id => {
    const select = $(id);
    if(!select) return;
    const first = id === "partitionKey" ? '<option value="">Sequential preview</option>' : '<option value="">Select column</option>';
    select.innerHTML = first + columnsCache.map(c=>`<option value="${escapeHtml(c.name)}">${escapeHtml(c.name)} • ${escapeHtml(c.dtype)}</option>`).join("");
  });
}

async function refreshStatus(){
  const s = await api("/api/status");
  $("mRecords").textContent = s.records;
  $("mValid").textContent = s.valid;
  $("mInvalid").textContent = s.invalid;
  $("mQuarantine").textContent = s.quarantined;
  $("mScore").textContent = `${s.quality_score}%`;
  $("mRules").textContent = s.rules;
  $("sourceBadge").textContent = s.source_name === "demo_orders" ? "Current Dataset" : (s.source_name || "No source selected");
  $("ruleMini").textContent = s.rules;

  $("kafkaMini").textContent = s.kafka_connected ? "Connected" : "Disconnected";
  $("kafkaMini").className = `status-pill ${s.kafka_connected ? "on":"off"}`;
  $("sparkMini").textContent = s.spark_connected ? "Connected" : "Disconnected";
  $("sparkMini").className = `status-pill ${s.spark_connected ? "on":"off"}`;

  $("kafkaBtn").textContent = s.kafka_connected ? "Disconnect Kafka" : "Connect Kafka";
  $("sparkBtn").textContent = s.spark_connected ? "Disconnect Spark" : "Connect Spark";
  $("streamBtn").textContent = s.stream_running ? "Stop Stream" : "Start Stream";
  $("streamBadge").textContent = s.stream_running ? "LIVE" : "STOPPED";
  $("streamBadge").className = `badge ${s.stream_running ? "good":"danger"}`;

  if(s.stream_running && !streamTimer) startPolling();
  if(!s.stream_running && streamTimer) stopPolling();
}

async function refreshPreview(){
  const current = await api("/api/preview?view=current");
  renderTable("overviewTable",current.rows);
  const original = await api("/api/preview?view=original");
  renderTable("sourceTable",original.rows);
}

async function refreshSources(){
  const d = await api("/api/sources");
  const sources = d.sources || [];
  const select = $("activeSourceSelect");
  if(select){
    select.innerHTML = sources.length
      ? sources.map(src=>`<option value="${escapeHtml(src.id)}" ${src.id===d.active_source_id?'selected':''}>${escapeHtml(src.id === "demo_orders" && src.type === "Demo" ? "Current Dataset" : `${src.name} • ${src.type}`)}</option>`).join("")
      : '<option value="">No source selected</option>';
  }
  $("sourceCount").textContent = `${sources.length} source${sources.length===1?'':'s'}`;
  const box = $("sourceRegistry");
  if(!sources.length){ box.className="source-registry empty-state"; box.textContent="No sources added yet."; return; }
  box.className="source-registry";
  box.innerHTML = sources.map(src=>`
    <div class="source-registry-card ${src.active?'selected':''}">
      <div class="source-icon">${src.type==='REST API'?'🌐':src.type==='SQLite DB'?'🗄️':src.type==='Excel'?'📊':'📄'}</div>
      <div class="source-main">
        <div class="source-title-row"><strong>${escapeHtml(src.id === "demo_orders" && src.type === "Demo" ? "Current Dataset" : src.name)}</strong><span class="badge ${src.active?'good':''}">${escapeHtml(src.id === "demo_orders" && src.type === "Demo" ? "Dataset" : src.type)}</span></div>
        <p>${src.rows} rows • ${src.columns} columns${src.filename?` • ${escapeHtml(src.filename)}`:''}</p>
        ${src.details?.url?`<small>${escapeHtml(src.details.url)}</small>`:''}
      </div>
      <div class="source-actions">
        <button class="btn ${src.active?'secondary':'ghost'}" onclick="selectSource('${src.id}')">${src.active?'Selected':'Use Dataset'}</button>
        <button class="danger-link" onclick="removeSource('${src.id}')">Remove</button>
      </div>
    </div>`).join("");
}

window.selectSource = async id => {
  try{
    await api(`/api/sources/select/${encodeURIComponent(id)}`,{method:"POST"});
    toast("Dataset selected");
    await refreshDashboard(); await refreshSources(); await refreshProfile(); await refreshValidationArea();
  }catch(e){toast(e.message);}
};
window.removeSource = async id => {
  try{
    await api(`/api/sources/${encodeURIComponent(id)}`,{method:"DELETE"});
    toast("Source removed");
    await refreshDashboard(); await refreshSources(); await refreshProfile(); await refreshValidationArea();
  }catch(e){toast(e.message);}
};

$("activeSourceSelect")?.addEventListener("change", e => {
  if(e.target.value) window.selectSource(e.target.value);
});

$$('.source-tab').forEach(btn=>btn.addEventListener('click',()=>{
  $$('.source-tab').forEach(x=>x.classList.toggle('active',x===btn));
  $$('.source-tab-panel').forEach(x=>x.classList.toggle('active',x.id===`sourceTab${btn.dataset.sourceTab.charAt(0).toUpperCase()+btn.dataset.sourceTab.slice(1)}`));
}));

async function refreshDashboard(){
  await Promise.all([refreshStatus(),refreshPreview(),refreshDQMini(),refreshPerformanceMini(),refreshColumns(),refreshSources()]);
}

// FILE STAGING: show exactly what the user selected before anything is uploaded.
function fileKind(name){
  const ext=(name.split(".").pop()||"").toUpperCase();
  if(["XLSX","XLS"].includes(ext)) return "XLS";
  if(["SQLITE","SQLITE3","DB"].includes(ext)) return "DB";
  return ext.slice(0,5) || "FILE";
}

function setSelectedFiles(files){
  const input=$("fileInput");
  const dt=new DataTransfer();
  files.forEach(f=>dt.items.add(f));
  input.files=dt.files;
  renderSelectedFiles();
}

function renderSelectedFiles(){
  const input=$("fileInput"),box=$("selectedFilesList"),count=$("selectedFilesCount");
  if(!input || !box || !count) return;
  const files=Array.from(input.files||[]);
  count.textContent=`${files.length} file${files.length===1?'':'s'}`;
  if(!files.length){
    box.className="selected-files-list empty-state compact-empty";
    box.textContent="Choose files to see them here before they are added.";
    return;
  }
  box.className="selected-files-list";
  box.innerHTML=files.map((f,i)=>`<div class="staged-file">
    <span class="staged-file-icon">${escapeHtml(fileKind(f.name))}</span>
    <span><b>${escapeHtml(f.name)}</b><small>${formatBytes(f.size)} · ready to add</small></span>
    <button type="button" data-remove-file="${i}" title="Remove selected file">×</button>
  </div>`).join("");
  box.querySelectorAll("[data-remove-file]").forEach(btn=>btn.addEventListener("click",()=>{
    const keep=files.filter((_,i)=>i!==Number(btn.dataset.removeFile));
    setSelectedFiles(keep);
  }));
  const result=$("uploadResult");
  if(result){result.className="message subtle";result.textContent=`${files.length} file${files.length===1?'':'s'} selected and waiting to be added.`;}
}

$("fileInput")?.addEventListener("change",renderSelectedFiles);
$("clearSelectedFilesBtn")?.addEventListener("click",()=>setSelectedFiles([]));
const sourceDropZone=$("uploadZone");
if(sourceDropZone){
  ["dragenter","dragover"].forEach(type=>sourceDropZone.addEventListener(type,e=>{e.preventDefault();sourceDropZone.classList.add("drag-over")}));
  ["dragleave","dragend"].forEach(type=>sourceDropZone.addEventListener(type,()=>sourceDropZone.classList.remove("drag-over")));
  sourceDropZone.addEventListener("drop",e=>{
    e.preventDefault();sourceDropZone.classList.remove("drag-over");
    const files=Array.from(e.dataTransfer?.files||[]);
    if(files.length) setSelectedFiles(files);
  });
}

$("demoBtn").addEventListener("click",async()=>{
  await api("/api/demo",{method:"POST"});
  toast("Dataset loaded successfully.");
  await refreshDashboard();
  await refreshProfile();
  await refreshValidationArea();
});

$("uploadBtn").addEventListener("click",async()=>{
  const files = Array.from($("fileInput").files || []);
  if(!files.length){ toast("Choose one or more source files first"); return; }
  const form = new FormData();
  files.forEach(file=>form.append("files",file));
  if(files.length===1 && $("datasetName").value.trim()) form.append("dataset_name",$("datasetName").value.trim());
  try{
    const res = await fetch("/api/upload",{method:"POST",body:form});
    const data = await res.json();
    if(!res.ok) throw new Error(data.error || "Upload failed");
    $("uploadResult").className = "message success";
    $("uploadResult").textContent = `${files.length} source${files.length===1?'':'s'} added. ${data.rows} rows in active dataset.`;
    $("fileInput").value=""; $("datasetName").value="";
    renderSelectedFiles();
    toast(`${files.length} source${files.length===1?'':'s'} added`);
    await refreshDashboard(); await refreshProfile(); await refreshValidationArea();
  }catch(e){
    $("uploadResult").className = "message error";
    $("uploadResult").textContent = e.message;
  }
});

$("addRestBtn")?.addEventListener("click",async()=>{
  const url=$("restUrl").value.trim();
  const name=$("restName").value.trim() || "rest_api_source";
  if(!url){toast("Enter a REST JSON URL");return;}
  try{
    const d=await api("/api/source/rest",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({url,name})});
    $("restResult").className="message success";
    $("restResult").textContent=`Loaded ${d.rows} rows and ${d.columns.length} columns.`;
    $("restUrl").value=""; $("restName").value="";
    toast("REST source added");
    await refreshDashboard(); await refreshProfile(); await refreshValidationArea();
  }catch(e){$("restResult").className="message error";$("restResult").textContent=e.message;}
});

// DATA EXPLORER
async function refreshProfile(){
  const p = await api("/api/profile");
  const s = p.summary || {};
  $("profileRows").textContent = s.rows || 0;
  $("profileCols").textContent = s.columns || 0;
  $("profileMemory").textContent = `${s.memory_mb || 0} MB`;
  $("profileMissing").textContent = s.missing_cells || 0;
  renderTable("profileTable",p.columns || []);
  renderMissingChart(p.columns || []);
}

function renderMissingChart(rows){
  destroyChart("missing");
  const canvas = $("missingChart"); if(!canvas) return;
  charts.missing = new Chart(canvas,{type:"bar",data:{labels:rows.map(x=>x.column),datasets:[{data:rows.map(x=>x.missing_pct),backgroundColor:"rgba(255,111,139,.65)",borderRadius:6}]},options:chartOptions("Missing %")});
}

function chartOptions(yLabel="Value"){
  return {responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{ticks:{color:"#7f94af",maxRotation:45,minRotation:0},grid:{display:false}},y:{ticks:{color:"#7f94af"},grid:{color:"rgba(48,71,102,.35)"},title:{display:true,text:yLabel,color:"#7187a5"}}}};
}

$("visualizeBtn").addEventListener("click",async()=>{
  const col = $("visualColumn").value;
  if(!col){toast("Select a column");return;}
  const v = await api(`/api/visualize?column=${encodeURIComponent(col)}`);
  destroyChart("column");
  charts.column = new Chart($("columnChart"),{type:v.type || "bar",data:{labels:v.labels,datasets:[{data:v.values,backgroundColor:"rgba(91,124,255,.65)",borderColor:"#7088ff",borderWidth:1,borderRadius:6}]},options:chartOptions("Count")});
});

// INTERACTIVE CLEANING WORKSPACE
let qualityIssuesCache = [];
let selectedQualityIssue = null;
let cleaningActions = {};

function issueIcon(type){
  if(["NULL","REQUIRED"].includes(type)) return "⚠";
  if(type.includes("DUPLICATE") || type === "UNIQUE") return "⧉";
  if(type === "NEGATIVE_VALUE" || type === "NON_NEGATIVE") return "−";
  if(type === "ALLOWED_VALUES" || type === "DATATYPE" || type === "RANGE") return "◆";
  return "!";
}

function issueTone(severity){
  return severity === "HIGH" ? "high" : severity === "MEDIUM" ? "medium" : "low";
}

function renderQualityIssues(){
  const box = $("qualityIssues");
  if(!box) return;
  if(!qualityIssuesCache.length){
    box.className = "quality-issues empty-state";
    box.innerHTML = "✓ No quality issues detected. Your dataset is ready for validation.";
    $("scanStatus").textContent = "Clean";
    $("scanStatus").className = "badge good";
    return;
  }
  box.className = "quality-issues";
  box.innerHTML = qualityIssuesCache.map(issue=>{
    const selected = selectedQualityIssue === issue.ui_id ? "selected" : "";
    const action = cleaningActions[issue.ui_id];
    const actionLabel = action ? (issue.options.find(o=>o.id===action.operation)?.label || action.operation) : "Choose fix";
    return `<button class="quality-issue ${selected}" onclick="selectQualityIssue('${escapeHtml(issue.ui_id)}')">
      <span class="issue-icon ${issueTone(issue.severity)}">${issueIcon(issue.issue_type)}</span>
      <span class="issue-main"><b>${escapeHtml(issue.reason)}</b><small>${escapeHtml(issue.columns.join(" + "))} · ${issue.rows} row(s)</small></span>
      <span class="issue-action ${action ? 'chosen':''}">${escapeHtml(actionLabel)} ${action ? '✓' : '→'}</span>
    </button>`;
  }).join("");
  $("scanStatus").textContent = `${qualityIssuesCache.length} issue group${qualityIssuesCache.length===1?'':'s'}`;
  $("scanStatus").className = "badge danger";
}

function renderAffectedRows(issue){
  const rows = issue.affected_rows || [];
  if(!rows.length) return `<div class="affected-empty">No affected rows found for this issue.</div>`;
  const allCols = Object.keys(rows[0].data || {});
  const issueCols = (issue.columns || []).filter(c=>allCols.includes(c));
  const keyCols = allCols.filter(c=>/(^id$|id$)/i.test(c) && !issueCols.includes(c)).slice(0,3);
  const contextCols = allCols.filter(c=>!issueCols.includes(c) && !keyCols.includes(c)).slice(0,4);
  const compactCols = [...new Set([...issueCols,...keyCols,...contextCols])];
  const showToggle = allCols.length > compactCols.length;
  const renderCols = compactCols.length ? compactCols : allCols.slice(0,8);
  let html = `<div class="affected-head"><div><b>Affected rows</b><small>Showing ${Math.min(rows.length, issue.affected_rows_total || rows.length)} of ${issue.affected_rows_total || rows.length}. The problem column is highlighted.</small></div>${showToggle?`<div class="affected-head-actions"><button class="view-columns-btn" type="button" onclick="toggleAffectedColumns(this)">Show all ${allCols.length} columns</button></div>`:''}</div>`;
  html += `<div class="affected-table-wrap" data-compact-cols="${escapeHtml(JSON.stringify(renderCols))}"><table class="affected-table"><thead><tr><th>Row</th>`;
  allCols.forEach(c=>{const hidden=!renderCols.includes(c);html += `<th class="${hidden?'affected-extra-col':''}">${escapeHtml(c)}</th>`;});
  html += `</tr></thead><tbody>`;
  rows.forEach(r=>{
    html += `<tr><td class="row-num">${escapeHtml(r.row_number)}</td>`;
    allCols.forEach(c=>{
      const value = r.data[c];
      const isIssue = issueCols.includes(c) || c === r.issue_column;
      const hidden=!renderCols.includes(c);
      html += `<td class="${isIssue ? 'issue-cell ' : ''}${hidden?'affected-extra-col':''}">${isIssue ? '<span class="issue-marker">!</span>' : ''}${escapeHtml(value === null ? 'NULL' : value)}</td>`;
    });
    html += `</tr>`;
  });
  html += `</tbody></table></div>`;
  return html;
}

window.toggleAffectedColumns = btn => {
  const panel=btn.closest('.affected-rows-panel');
  if(!panel) return;
  const expanded=panel.classList.toggle('show-all-columns');
  btn.textContent=expanded?'Show fewer columns':'Show all columns';
  initAllDragScroll();
};

function renderFixPanel(){
  const panel = $("fixPanel");
  if(!panel) return;
  const issue = qualityIssuesCache.find(x=>x.ui_id===selectedQualityIssue);
  if(!issue){
    panel.className = "fix-panel-empty";
    panel.innerHTML = "Your selected issue and affected rows will appear here.";
    $("fixSubtitle").textContent = "Select an issue from the left.";
    return;
  }
  const current = cleaningActions[issue.ui_id] || {operation: issue.recommended, value:""};
  const valueNeeded = ["custom"].includes(current.operation);
  const optionCards = issue.options.map(o=>{
    const preview = o.preview !== null && o.preview !== undefined ? `<em>Suggested value: ${escapeHtml(o.preview)}</em>` : '';
    return `<button class="fix-option ${current.operation===o.id?'active':''}" onclick="chooseFix('${escapeHtml(issue.ui_id)}','${escapeHtml(o.id)}')"><span><b>${escapeHtml(o.label)}</b><small>${escapeHtml(o.help)}</small>${preview}</span>${current.operation===o.id?'<strong>✓</strong>':''}</button>`;
  }).join("");
  panel.className = "fix-panel";
  panel.innerHTML = `
    <div class="selected-issue-banner ${issueTone(issue.severity)}">
      <div><span>${issueIcon(issue.issue_type)}</span><div><b>${escapeHtml(issue.reason)}</b><small>${escapeHtml(issue.columns.join(" + "))} · ${issue.rows} affected row(s)</small></div></div>
      <span class="severity-tag">${escapeHtml(issue.severity)}</span>
    </div>
    <div class="affected-rows-panel">${renderAffectedRows(issue)}</div>
    <div class="fix-choice-title">Choose how to handle these rows</div>
    <div class="fix-options">${optionCards}</div>
    ${valueNeeded ? `<label class="fix-value-label"><span>Replacement value</span><input id="fixCustomValue" class="field" value="${escapeHtml(current.value || '')}" placeholder="Enter the value to use"></label>` : ''}
    <div class="fix-panel-footer"><span>Recommended: <b>${escapeHtml(issue.options.find(o=>o.id===issue.recommended)?.label || issue.recommended)}</b></span><button class="btn primary" onclick="saveCurrentFix()">Add Fix</button></div>`;
  $("fixSubtitle").textContent = `Inspect the affected rows, then choose the safest fix for ${issue.rows} row(s).`;
}

window.selectQualityIssue = uiId => { selectedQualityIssue = uiId; renderQualityIssues(); renderFixPanel(); };

window.chooseFix = (uiId, operation) => {
  const issue = qualityIssuesCache.find(x=>x.ui_id===uiId);
  if(!issue) return;
  cleaningActions[uiId] = {issue_id: issue.rule_id, column: issue.columns[0] || null, columns: issue.columns, operation, value: cleaningActions[uiId]?.value || ""};
  selectedQualityIssue = uiId;
  renderFixPanel();
};

window.saveCurrentFix = () => {
  const issue = qualityIssuesCache.find(x=>x.ui_id===selectedQualityIssue);
  if(!issue) return;
  const current = cleaningActions[issue.ui_id] || {issue_id:issue.rule_id, column:issue.columns[0] || null, columns:issue.columns, operation:issue.recommended, value:""};
  if($("fixCustomValue")) current.value = $("fixCustomValue").value;
  cleaningActions[issue.ui_id] = current;
  renderQualityIssues();
  renderFixPanel();
  renderCleaningSummary();
  setValidationStage(3);
  toast("Fix added to cleaning plan");
};

function getCleaningActions(){
  return Object.values(cleaningActions).filter(x=>x.operation && x.operation!=="leave");
}

function renderCleaningSummary(){
  const chosen = getCleaningActions();
  const el = $("fixSummary");
  if(!chosen.length){
    el.className = "message subtle";
    el.textContent = "Choose one or more fixes to build a cleaning plan.";
    return;
  }
  el.className = "message success";
  el.textContent = `${chosen.length} fix${chosen.length===1?'':'es'} selected. Preview them before applying anything.`;
}

function setValidationStage(stage, completed=false){
  const stages=$$(".validation-stage");
  stages.forEach((el,i)=>{
    el.classList.toggle("active",i===stage-1);
    el.classList.toggle("completed",i<stage-1 || (completed && i===stage-1));
  });
}

async function scanQuality(){
  try{
    const d = await api("/api/quality/scan");
    qualityIssuesCache = d.issues || [];
    selectedQualityIssue = qualityIssuesCache[0]?.ui_id || null;
    cleaningActions = {};
    $("cleanIssueCount").textContent = d.issue_count || 0;
    $("cleanAffectedRows").textContent = d.affected_rows || 0;
    $("cleanColumnsAffected").textContent = new Set(qualityIssuesCache.flatMap(x=>x.columns || [])).size;
    const q = await api("/api/dq");
    $("cleanDQScore").textContent = `${q.after.overall}%`;
    renderQualityIssues();
    renderFixPanel();
    renderCleaningSummary();
    setValidationStage(2);
  }catch(e){
    toast(e.message);
  }
}

async function previewCleaning(){
  const actions = getCleaningActions();
  if(!actions.length){toast("Choose at least one fix first");return;}
  try{
    const d = await api("/api/clean/preview",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({actions})});
    renderTable("changesTable",d.changes || []);
    renderTable("cleanPreviewTable",d.preview || []);
    $("fixSummary").className = "message success";
    $("fixSummary").textContent = `${d.changes.length} change group(s) previewed · ${d.before_rows} rows → ${d.after_rows} rows · ${d.quarantine_rows} rows marked for quarantine.`;
    setValidationStage(4);
    toast("Fix preview ready");
  }catch(e){toast(e.message);}
}

async function applyCleaning(){
  try{
    const d = await api("/api/clean/apply",{method:"POST"});
    const qText = d.quarantine_change > 0 ? ` Quarantine +${d.quarantine_change}.` : d.quarantine_change < 0 ? ` Quarantine ${Math.abs(d.quarantine_change)} row(s) cleared.` : " Quarantine updated.";
    setValidationStage(4,true);
    toast(`✓ ${d.fixed_rows} issue row(s) fixed.${qText}`);
    await refreshDashboard();
    await refreshValidationArea(false);
    await refreshDQ();
    await refreshHistory();
    await scanQuality();
  }catch(e){toast(e.message);}
}

async function resetCleaning(){
  await api("/api/clean/reset",{method:"POST"});
  cleaningActions = {};
  selectedQualityIssue = qualityIssuesCache[0]?.ui_id || null;
  $("changesTable").className="table-wrap empty-state";
  $("changesTable").innerHTML="No changes previewed yet.";
  $("cleanPreviewTable").className="table-wrap empty-state";
  $("cleanPreviewTable").innerHTML="Preview the selected fixes to see the result.";
  renderQualityIssues(); renderFixPanel(); renderCleaningSummary();
  setValidationStage(1);
  toast("Cleaning plan reset");
}

$("scanQualityBtn")?.addEventListener("click",scanQuality);
$("previewFixBtn")?.addEventListener("click",previewCleaning);
$("applyFixBtn")?.addEventListener("click",applyCleaning);
$("resetFixesBtn")?.addEventListener("click",resetCleaning);
$("runCombinedBtnAdvanced")?.addEventListener("click",()=>runValidation("combined"));

$$(".result-tab").forEach(btn=>btn.addEventListener("click",()=>{
  $$(".result-tab").forEach(x=>x.classList.toggle("active",x===btn));
  $$(".result-panel").forEach(x=>x.classList.toggle("active",x.id===`resultTab${btn.dataset.resultTab.charAt(0).toUpperCase()+btn.dataset.resultTab.slice(1)}`));
}));

// RULE BUILDER
const ruleText = {
  duplicate:"Choose all columns that together identify a duplicate. Example: Name + Country + District + Village.",
  required:"Choose one or more columns that must always contain a value.",
  unique:"Choose one column that must never repeat, such as orderID.",
  range:"Choose one numeric column, then enter minimum and/or maximum values.",
  allowed_values:"Choose one column and provide the only values that are allowed.",
  datatype:"Choose one column and the datatype it must follow.",
  regex:"Choose one text column and enter the pattern it must match.",
  non_negative:"Choose one or more numeric columns that must never contain a negative value."
};

function renderColumnCheckboxes(){
  const box = $("ruleColumns"); if(!box) return;
  if(!columnsCache.length){box.innerHTML='<span class="muted">Load a dataset first.</span>';return;}
  box.innerHTML = columnsCache.map((c,i)=>`<label class="check-card"><input type="checkbox" class="rule-column" value="${escapeHtml(c.name)}"><span>${escapeHtml(c.name)}</span><small>${escapeHtml(c.dtype)}</small></label>`).join("");
}

function renderRuleParams(){
  const type = $("ruleType").value;
  $("ruleExplainer").textContent = ruleText[type] || "Configure the rule.";
  $("columnHint").textContent = ["duplicate","required","non_negative"].includes(type) ? "Multiple columns allowed" : "Choose one column";
  const box = $("ruleParams");
  if(type === "range") box.innerHTML = '<label><span>Minimum</span><input id="paramMin" class="field" type="number" step="any" placeholder="Optional"></label><label><span>Maximum</span><input id="paramMax" class="field" type="number" step="any" placeholder="Optional"></label>';
  else if(type === "allowed_values") box.innerHTML = '<label style="grid-column:1/-1"><span>Allowed Values</span><input id="paramValues" class="field" placeholder="NEW, PAID, SHIPPED"></label>';
  else if(type === "datatype") box.innerHTML = '<label style="grid-column:1/-1"><span>Expected Datatype</span><select id="paramDatatype" class="field"><option value="string">String</option><option value="numeric">Numeric</option><option value="integer">Integer</option><option value="date">Date</option><option value="boolean">Boolean</option></select></label>';
  else if(type === "regex") box.innerHTML = '<label style="grid-column:1/-1"><span>Regex Pattern</span><input id="paramRegex" class="field" placeholder="^[A-Z]{2}[0-9]{4}$"></label>';
  else box.innerHTML = "";
}

$("ruleType").addEventListener("change",()=>{renderRuleParams();$$('.rule-column').forEach(x=>x.checked=false);});
renderRuleParams();

$("addRuleBtn").addEventListener("click",async()=>{
  const type = $("ruleType").value;
  const selected = $$(".rule-column:checked").map(x=>x.value);
  if(!selected.length){toast("Select at least one column");return;}
  const params = {};
  if(type === "range"){params.min=$("paramMin")?.value || null;params.max=$("paramMax")?.value || null;}
  if(type === "allowed_values") params.values=($("paramValues")?.value || "").split(",").map(x=>x.trim()).filter(Boolean);
  if(type === "datatype") params.datatype=$("paramDatatype")?.value || "string";
  if(type === "regex") params.pattern=$("paramRegex")?.value || "";
  try{
    await api("/api/rules",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({type,columns:selected,params,severity:$("ruleSeverity").value,name:$("ruleName").value})});
    $("ruleName").value=""; $$(".rule-column").forEach(x=>x.checked=false);
    toast("Validation rule added");
    await refreshRules(); await refreshStatus();
  }catch(e){toast(e.message);}
});

async function refreshRules(){
  const rows = await api("/api/rules");
  const mapped = rows.map(r=>({id:r.id,name:r.name,type:r.type,columns:r.columns.join(" + "),severity:r.severity,status:r.enabled?"Enabled":"Disabled"}));
  renderTable("rulesTable",mapped,row=>`<button class="rule-toggle ${row.status==='Enabled'?'on':''}" onclick="toggleRule('${row.id}')">${row.status}</button> <button class="danger-link" onclick="deleteRule('${row.id}')">Delete</button>`);
}

window.toggleRule = async id => {await api(`/api/rules/${id}/toggle`,{method:"POST"});toast("Rule status changed");await refreshRules();};
window.deleteRule = async id => {await api(`/api/rules/${id}`,{method:"DELETE"});toast("Rule deleted");await refreshRules();await refreshStatus();};

async function runValidation(mode){
  try{
    const d = await api(`/api/validate/${mode}`,{method:"POST"});
    $("validationSummary").className="message success";
    $("validationSummary").textContent = `${mode.replaceAll('-',' ')}: ${d.valid} valid, ${d.invalid} invalid, ${d.issues.length} issue group(s).`;
    toast("Validation finished");
    if(mode === "custom-preview") await refreshTempPreview();
    else {await refreshDashboard();await refreshIssues();await refreshAppliedPreview();await refreshDQ();await refreshHistory();}
  }catch(e){toast(e.message);}
}

$("runDefaultBtn").addEventListener("click",()=>runValidation("default"));
$("validateDefaultTop").addEventListener("click",()=>runValidation("default"));
$("previewCustomBtn").addEventListener("click",()=>runValidation("custom-preview"));
$("runCombinedBtn").addEventListener("click",()=>runValidation("combined"));
$("autoFixBtn").addEventListener("click",async()=>{
  try{
    const d=await api("/api/autofix",{method:"POST"});
    toast(`Auto fix complete. DQ ${d.dq.overall}%`);
    await refreshDashboard(); await refreshValidationArea(); await refreshDQ(); await refreshHistory();
  }catch(e){toast(e.message);}
});

async function refreshTempPreview(){
  const [v,i] = await Promise.all([api("/api/preview?view=temp_valid"),api("/api/preview?view=temp_invalid")]);
  renderTable("tempValidTable",v.rows);renderTable("tempInvalidTable",i.rows);
}
async function refreshIssues(){renderTable("issuesTable",await api("/api/issues"));}
async function refreshAppliedPreview(){
  const [v,i]=await Promise.all([api("/api/preview?view=valid"),api("/api/preview?view=invalid")]);
  renderTable("appliedValidTable",v.rows); renderTable("appliedInvalidTable",i.rows);
}
async function refreshQuarantinePreview(){const q=await api("/api/preview?view=quarantine");renderTable("quarantineTable",q.rows);}
async function refreshValidationArea(scan=true){await refreshColumns();await Promise.all([refreshRules(),refreshTempPreview(),refreshAppliedPreview(),refreshIssues(),refreshQuarantinePreview()]);if(scan) await scanQuality();}

// DQ
async function refreshDQMini(){
  const d = await api("/api/dq"); const q=d.after;
  $("miniCompleteness").textContent=`${q.completeness}%`;$("miniValidity").textContent=`${q.validity}%`;$("miniUniqueness").textContent=`${q.uniqueness}%`;$("miniConsistency").textContent=`${q.consistency}%`;$("miniIntegrity").textContent=`${q.integrity}%`;
}

async function refreshDQ(){
  const d = await api("/api/dq"); const q=d.after;
  $("dqOverall").textContent=`${q.overall}%`;
  [["Completeness","completeness"],["Validity","validity"],["Uniqueness","uniqueness"],["Consistency","consistency"],["Integrity","integrity"]].forEach(([label,key])=>{
    $(`dq${label}`).textContent=`${q[key]}%`; $(`bar${label}`).style.width=`${q[key]}%`;
  });
  $("dqTips").innerHTML=(q.tips||[]).map(t=>`<div class="tip">${escapeHtml(t)}</div>`).join("");
  destroyChart("dqCompare");
  charts.dqCompare=new Chart($("dqCompareChart"),{type:"bar",data:{labels:["Completeness","Validity","Uniqueness","Consistency","Integrity"],datasets:[{label:"Before",data:[d.before.completeness,d.before.validity,d.before.uniqueness,d.before.consistency,d.before.integrity],backgroundColor:"rgba(255,111,139,.55)",borderRadius:5},{label:"After",data:[q.completeness,q.validity,q.uniqueness,q.consistency,q.integrity],backgroundColor:"rgba(48,211,155,.62)",borderRadius:5}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:"#9db0c8"}}},scales:{x:{ticks:{color:"#8298b5"},grid:{display:false}},y:{beginAtZero:true,max:100,ticks:{color:"#8298b5"},grid:{color:"rgba(48,71,102,.35)"}}}}});
}

// STREAM
$("kafkaBtn").addEventListener("click",async()=>{await api("/api/kafka/toggle",{method:"POST"});await refreshStatus();});
$("sparkBtn").addEventListener("click",async()=>{await api("/api/spark/toggle",{method:"POST"});await refreshStatus();});
$("streamBtn").addEventListener("click",async()=>{try{await api("/api/stream/toggle",{method:"POST"});await refreshStatus();await streamTick();}catch(e){toast(e.message);}});
$("injectBtn").addEventListener("click",async()=>{try{await api("/api/stream/inject-error",{method:"POST"});toast("Bad event injected");await streamTick();}catch(e){toast(e.message);}});
$("clearStreamBtn").addEventListener("click",async()=>{await api("/api/stream/clear",{method:"POST"});await streamTick();toast("Stream cleared");});

function startPolling(){if(streamTimer)return;streamTimer=setInterval(streamTick,1000);}
function stopPolling(){if(streamTimer)clearInterval(streamTimer);streamTimer=null;}

async function streamTick(){
  const d=await api("/api/stream/tick",{method:"POST"});
  $("sProcessed").textContent=d.processed;$("sValid").textContent=d.valid;$("sInvalid").textContent=d.invalid;$("sLag").textContent=d.total_lag;$("sRate").textContent=d.rate;$("sQuality").textContent=`${d.quality}%`;
  renderTable("streamTable",d.events);renderTable("kafkaPartitionTable",d.kafka_partitions);
  destroyChart("streamQuality");
  charts.streamQuality=new Chart($("qualityChart"),{type:"line",data:{labels:d.history.map(x=>x.time),datasets:[{data:d.history.map(x=>x.quality),borderColor:"#20c7e8",backgroundColor:"rgba(32,199,232,.12)",fill:true,tension:.3,pointRadius:0}]},options:{responsive:true,maintainAspectRatio:false,animation:false,plugins:{legend:{display:false}},scales:{x:{ticks:{color:"#7890af",maxTicksLimit:7},grid:{display:false}},y:{beginAtZero:true,max:100,ticks:{color:"#7890af"},grid:{color:"rgba(48,71,102,.35)"}}}}});
  await Promise.all([refreshStatus(),refreshPerformanceMini()]);
}

// SPARK/PERFORMANCE
async function refreshPerformanceMini(){
  const p=await api("/api/performance");
  $("miniRps").textContent=p.rows_per_sec;$("miniValidationMs").textContent=`${p.validation_ms} ms`;$("miniPartitions").textContent=p.spark.partitions;$("miniKafkaLag").textContent=p.kafka.total_lag;
}

async function refreshPerformanceAndPartitions(){
  const p=await api("/api/performance");
  $("pIngestion").textContent=`${p.ingestion_ms} ms`;$("pValidation").textContent=`${p.validation_ms} ms`;$("pRps").textContent=p.rows_per_sec;$("pPartitions").textContent=p.spark.partitions;$("pSkew").textContent=p.spark.skew_ratio;$("pMemory").textContent=`${p.memory_mb} MB`;
  await refreshPartitions();
}

async function refreshPartitions(){
  const d=await api("/api/partitions");
  $("partitionNote").textContent=d.note.replace(/\bdemo\b/gi,"the current configuration") + ` Current skew ratio: ${d.skew_ratio}.`;
  renderTable("partitionTable",d.partitions);
  destroyChart("partition");
  charts.partition=new Chart($("partitionChart"),{type:"bar",data:{labels:d.partitions.map(x=>`P${x.partition}`),datasets:[{data:d.partitions.map(x=>x.rows),backgroundColor:"rgba(139,92,246,.62)",borderRadius:6}]},options:chartOptions("Rows")});
}

$("partitionBtn").addEventListener("click",async()=>{
  await api("/api/partitions",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({count:Number($("partitionCount").value)||4,key:$("partitionKey").value||null})});
  toast("Partition preview updated");await refreshPerformanceAndPartitions();
});

// HISTORY
async function refreshHistory(){renderTable("historyTable",await api("/api/runs"));}

// CHAT
function openChat(){$("chatPanel").classList.add("open");setTimeout(()=>$("chatInput").focus(),50);}
function closeChat(){$("chatPanel").classList.remove("open");}
$("chatFab").addEventListener("click",openChat);$("openChatTop").addEventListener("click",openChat);$("closeChat").addEventListener("click",closeChat);
$$(".chat-suggestions button").forEach(b=>b.addEventListener("click",()=>{$("chatInput").value=b.textContent;sendChat();}));

function addBubble(type,text,sql=null,evidence=[]){
  const box=$("chatMessages");
  const div=document.createElement("div");div.className=`bubble ${type}`;
  div.innerHTML=`<p>${escapeHtml(text)}</p>${sql?`<div class="sql">${escapeHtml(sql)}</div>`:""}${evidence?.length?`<div class="evidence">Sources: ${escapeHtml(evidence.join(" • "))}</div>`:""}`;
  box.appendChild(div);box.scrollTop=box.scrollHeight;
}

async function sendChat(){
  const text=$("chatInput").value.trim();if(!text)return;
  addBubble("user",text);$("chatInput").value="";
  try{const d=await api("/api/chat",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({message:text})});addBubble("bot",d.answer,d.sql,d.evidence);}catch(e){addBubble("bot",e.message);}
}
$("sendChat").addEventListener("click",sendChat);$("chatInput").addEventListener("keydown",e=>{if(e.key==="Enter"&&!e.shiftKey){e.preventDefault();sendChat();}});

// RESET
$("resetBtn").addEventListener("click",async()=>{await api("/api/reset",{method:"POST"});stopPolling();toast("Dataset state reset; custom rules kept");await refreshDashboard();await refreshValidationArea();await refreshProfile();await refreshDQ();await streamTick();});

// Initial load
(async function init(){
  try{
    await refreshDashboard();
    await refreshValidationArea();
    await refreshProfile();
    await refreshDQ();
    await refreshHistory();
    await streamTick();
    renderSelectedFiles();
    initAllDragScroll();
  }catch(e){console.error(e);toast("UI loaded. Add a dataset to begin.");}
})();

// INTEGRATION STUDIO
const integrationState = { selected: new Set(), candidateList: [] };

async function refreshIntegration(){
  const d = await api('/api/integration/state');
  const sources = (await api('/api/sources')).sources || [];
  const picker = $('integrationSourcePicker');
  if(!sources.length){
    picker.className='integration-source-picker empty-state';
    picker.textContent='Add sources from Data Sources first.';
    return;
  }
  const existing = new Set(d.source_ids || []);
  integrationState.selected = existing;
  picker.className='integration-source-picker';
  picker.innerHTML = sources.map(src=>`
    <label class="integration-source-option ${existing.has(src.id)?'selected':''}">
      <input type="checkbox" value="${escapeHtml(src.id)}" ${existing.has(src.id)?'checked':''}>
      <span class="integration-source-icon">${src.type==='REST API'?'🌐':src.type==='SQLite DB'?'🗄️':src.type==='SQL Table'?'🧮':src.type==='Excel'?'📊':'📄'}</span>
      <span><b>${escapeHtml(src.name)}</b><small>${escapeHtml(src.type)} • ${src.rows} rows • ${src.columns} cols</small></span>
    </label>`).join('');
  const syncSourceSelectors=()=>{
    const selectedIds=[...integrationState.selected];
    const base=$('integrationBaseSource');
    const previousBase=base.value;
    base.innerHTML=selectedIds.map(id=>{const src=sources.find(x=>x.id===id);return src?`<option value="${escapeHtml(id)}">${escapeHtml(src.name)}</option>`:''}).join('');
    if(selectedIds.includes(previousBase)) base.value=previousBase;
    const baseId=base.value;
    const right=$('integrationRightSource');
    const previousRight=right.value;
    right.innerHTML=selectedIds.filter(id=>id!==baseId).map(id=>{const src=sources.find(x=>x.id===id);return src?`<option value="${escapeHtml(id)}">${escapeHtml(src.name)}</option>`:''}).join('');
    if(selectedIds.includes(previousRight) && previousRight!==baseId) right.value=previousRight;
  };
  picker.querySelectorAll('input').forEach(input=>input.addEventListener('change',async()=>{
    if(input.checked) integrationState.selected.add(input.value); else integrationState.selected.delete(input.value);
    input.closest('.integration-source-option')?.classList.toggle('selected',input.checked);
    $('integrationSourceCount').textContent=`${integrationState.selected.size} selected`;
    syncSourceSelectors();
    await loadIntegrationCandidates();
  }));
  $('integrationSourceCount').textContent=`${integrationState.selected.size} selected`;
  $('integrationProjectName').value=d.project_name || '';
  $('integrationProjectBadge').textContent=d.project_name || 'No project';
  const base=$('integrationBaseSource');
  base.innerHTML=[...integrationState.selected].map(id=>{const src=sources.find(x=>x.id===id);return src?`<option value="${escapeHtml(id)}">${escapeHtml(src.name)}</option>`:''}).join('');
  if(d.base_source_id && integrationState.selected.has(d.base_source_id)) base.value=d.base_source_id;
  const right=$('integrationRightSource');
  const baseId=base.value;
  right.innerHTML=[...integrationState.selected].filter(id=>id!==baseId).map(id=>{const src=sources.find(x=>x.id===id);return src?`<option value="${escapeHtml(id)}">${escapeHtml(src.name)}</option>`:''}).join('');
  $('integrationStepBadge').textContent=`${(d.steps||[]).length} join${(d.steps||[]).length===1?'':'s'}`;
  renderIntegrationSteps(d.steps||[]);
  renderIntegrationReport(d.report||{});
  renderTable('integrationPreview',d.combined_preview||[]);
  $('combinedDatasetBadge').textContent=d.has_combined?'Ready':'Not built';
  if(d.steps?.length && d.combined_preview?.length) {
    $('integrationLeftLabel').textContent='Current Combined Result';
    $('integrationLeftRows').textContent=`${d.report?.final_rows||d.combined_preview.length} rows after joins`;
  } else if(baseId) {
    const src=sources.find(x=>x.id===baseId); $('integrationLeftLabel').textContent=src?.name||'Base dataset'; $('integrationLeftRows').textContent=src?`${src.rows} rows`:'—';
  }
  updateIntegrationFlow(d);
  await loadIntegrationCandidates();
}

function updateIntegrationFlow(d){
  const steps=$$(".flow-step");
  const project=Boolean(d.project_name);
  const selected=(d.source_ids||[]).length>=2;
  const connected=(d.steps||[]).length>0 || Boolean(d.report?.joins?.length);
  const built=Boolean(d.has_combined);
  const states=[project,selected,connected,built];
  steps.forEach((el,i)=>{
    el.classList.toggle("completed",states[i]);
    const firstPending=states.findIndex(v=>!v);
    el.classList.toggle("active",firstPending===i || (firstPending===-1 && i===steps.length-1));
  });
}

function renderIntegrationSteps(steps){
  const el=$('integrationSteps');
  if(!steps.length){el.className='integration-steps empty-state';el.textContent='No joins configured yet.';return;}
  el.className='integration-steps';
  el.innerHTML=steps.map((s,i)=>`<div class="integration-step"><div class="step-number">${i+1}</div><div><b>${escapeHtml(s.left_name)} → ${escapeHtml(s.right_name)}</b><small>${escapeHtml(s.left_key)} ${escapeHtml(s.how).toUpperCase()} JOIN ${escapeHtml(s.right_key)} • ${s.match_rate}% match • ${s.unmatched} unmatched${s.type_conversion?' • normalized':''}</small></div><span class="badge ${s.unmatched?'warning':'good'}">${s.unmatched?'⚠ '+s.unmatched:'✓ matched'}</span></div>`).join('');
}

function renderIntegrationReport(report){
  const vals=[report.sources||0,report.joins?.length||0,report.final_rows||0,report.final_columns||0,report.validation?.dq_score!=null?`${report.validation.dq_score}%`:'—'];
  const box=$('integrationReportMetrics');
  box.querySelectorAll('div').forEach((el,i)=>el.querySelector('b').textContent=vals[i]);
  const vs=$('integrationValidationSummary');
  vs.innerHTML=report.validation ? `<b>Combined dataset validation</b><span>✓ ${report.validation.valid} valid</span><span>⚠ ${report.validation.invalid} invalid</span><span>🔒 ${report.validation.quarantine} quarantined</span><span>${report.validation.issue_groups} issue group(s)</span>` : '';
  const jr=$('integrationJoinReport');
  if(!report.joins?.length){jr.className='integration-join-report empty-state';jr.textContent='Build a join to generate the integration report.';}
  else {jr.className='integration-join-report';jr.innerHTML=report.joins.map(j=>`<div class="integration-join-row"><div><b>${escapeHtml(j.left)} → ${escapeHtml(j.right)}</b><small>${escapeHtml(j.left_key)} ↔ ${escapeHtml(j.right_key)} • ${escapeHtml(j.how)}</small></div><strong>${j.match_rate}%</strong><span>${j.unmatched?`${j.unmatched} unmatched`:'All left rows matched'}</span></div>`).join('');}
  const issues=$('integrationIssues');
  issues.innerHTML=report.issues?.length ? report.issues.map(i=>`<div class="integration-issue"><b>⚠ ${escapeHtml(i.type.replaceAll('_',' '))}</b><span>${escapeHtml(i.reason)}</span></div>`).join('') : (report.joins?.length?'<div class="integration-success">✓ No join-level referential integrity issues detected.</div>':'');
}

async function loadIntegrationCandidates(){
  const baseId=$('integrationBaseSource').value;
  const rightId=$('integrationRightSource').value;
  const d=await api('/api/integration/state');
  const leftId=(d.steps||[]).length ? '__combined__' : baseId;
  if(!leftId || !rightId || leftId===rightId){ $('integrationCandidateMessage').textContent='Select a right dataset to inspect possible join keys.'; $('integrationCandidates').innerHTML=''; return; }
  try{
    const c=await api(`/api/integration/candidates?left=${encodeURIComponent(leftId)}&right=${encodeURIComponent(rightId)}`);
    integrationState.candidateList=c.candidates||[];
    const lk=$('integrationLeftKey'), rk=$('integrationRightKey');
    lk.innerHTML=(c.left_columns||[]).map(x=>`<option value="${escapeHtml(x.name)}">${escapeHtml(x.name)} • ${escapeHtml(x.dtype)}</option>`).join('');
    rk.innerHTML=(c.right_columns||[]).map(x=>`<option value="${escapeHtml(x.name)}">${escapeHtml(x.name)} • ${escapeHtml(x.dtype)}</option>`).join('');
    if(c.candidates?.length){
      lk.value=c.candidates[0].left; rk.value=c.candidates[0].right;
      $('integrationCandidateMessage').textContent=`💡 Suggested relationship: ${c.candidates[0].left} ↔ ${c.candidates[0].right} (${c.candidates[0].reason}).`;
      $('integrationCandidates').innerHTML=c.candidates.slice(0,6).map((x,i)=>`<button type="button" class="candidate-chip ${i===0?'active':''}" data-left="${escapeHtml(x.left)}" data-right="${escapeHtml(x.right)}">${escapeHtml(x.left)} ↔ ${escapeHtml(x.right)}</button>`).join('');
      $('integrationCandidates').querySelectorAll('.candidate-chip').forEach(btn=>btn.addEventListener('click',()=>{
        lk.value=btn.dataset.left; rk.value=btn.dataset.right;
        $('integrationCandidates').querySelectorAll('.candidate-chip').forEach(x=>x.classList.toggle('active',x===btn));
      }));
    } else {
      $('integrationCandidateMessage').textContent='No obvious matching key found. You can still choose columns manually.';
      $('integrationCandidates').innerHTML='';
    }
  }catch(e){ $('integrationCandidateMessage').textContent=e.message; }
}

async function runSmartIntegration(analyzeOnly=false){
  const ids=[...integrationState.selected];
  if(ids.length<2){toast('Select at least two datasets first');return;}
  const msg=$('smartIntegrationMessage'), rel=$('smartIntegrationRelationships');
  msg.textContent='⏳ DataGuard is profiling schemas and discovering relationships...';
  try{
    const d=await api('/api/integration/smart/analyze',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({source_ids:ids})});
    const auto=d.auto||[], review=d.review||[];
    rel.className='integration-steps';
    rel.innerHTML=(auto.length||review.length)?[
      ...auto.map((x,i)=>`<div class="integration-step"><div class="step-number">✓</div><div><b>${escapeHtml(x.left_name)}.${escapeHtml(x.left_key)} ↔ ${escapeHtml(x.right_name)}.${escapeHtml(x.right_key)}</b><small>High confidence • ${x.confidence}% • DataGuard will use a LEFT JOIN automatically</small></div><span class="badge good">AUTO</span></div>`),
      ...review.map(x=>`<div class="integration-step"><div class="step-number">?</div><div><b>${escapeHtml(x.left_name)}.${escapeHtml(x.left_key)} ↔ ${escapeHtml(x.right_name)}.${escapeHtml(x.right_key)}</b><small>Confidence ${x.confidence}% • review recommended</small></div><span class="badge warning">REVIEW</span></div>`)
    ].join(''):'<div class="empty-state">No confident relationships found. Use Advanced Integration to choose keys manually.</div>';
    msg.innerHTML=`✓ Found <b>${d.relationships.length}</b> likely relationship(s): <b>${auto.length}</b> high-confidence automatic, <b>${review.length}</b> needing review.`;
    if(!analyzeOnly && auto.length){ await buildSmartIntegration(); }
  }catch(e){msg.textContent='❌ '+e.message;}
}
async function buildSmartIntegration(){
  const ids=[...integrationState.selected];
  const name=($('smartIntegrationProjectName')?.value||$('integrationProjectName')?.value||'Sales Analysis').trim()||'Sales Analysis';
  $('smartIntegrationMessage').textContent='⏳ Building combined dataset and validating it...';
  try{
    const d=await api('/api/integration/smart/build',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({source_ids:ids,project_name:name})});
    $('smartIntegrationMessage').innerHTML=`✓ <b>${escapeHtml(name)}</b> created automatically. ${d.rows} rows × ${d.columns.length} columns. ${d.unconnected?.length?`⚠ ${d.unconnected.length} source(s) could not be connected.`:''}`;
    toast(`✓ ${name} integrated automatically • ${d.rows} rows`);
    await refreshDashboard(); await refreshIntegration(); await refreshProfile(); await refreshValidationArea();
  }catch(e){$('smartIntegrationMessage').textContent='❌ '+e.message;toast(e.message);}
}
$('smartAnalyzeBtn')?.addEventListener('click',()=>runSmartIntegration(true));
$('smartBuildBtn')?.addEventListener('click',()=>runSmartIntegration(false));
$('smartIntegrationBtn')?.addEventListener('click',()=>runSmartIntegration(false));

$('newIntegrationBtn')?.addEventListener('click',async()=>{
  await api('/api/integration/reset',{method:'POST'}); integrationState.selected=new Set(); toast('Integration project reset'); await refreshIntegration();
});
$('createIntegrationBtn')?.addEventListener('click',async()=>{
  const name=$('integrationProjectName').value.trim(); if(!name){toast('Enter a project name');return;}
  try{await api('/api/integration/project',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name})});toast(`${name} project created`);await refreshIntegration();}catch(e){toast(e.message);}
});
$('saveIntegrationSourcesBtn')?.addEventListener('click',async()=>{
  const ids=[...integrationState.selected]; if(ids.length<2){toast('Select at least two datasets');return;}
  try{await api('/api/integration/sources',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({source_ids:ids,base_source_id:$('integrationBaseSource').value||ids[0]})});toast('Datasets added to integration project');await refreshIntegration();}catch(e){toast(e.message);}
});
$('integrationBaseSource')?.addEventListener('change',async()=>{await loadIntegrationCandidates();});
$('integrationRightSource')?.addEventListener('change',async()=>{await loadIntegrationCandidates();});
$('addJoinBtn')?.addEventListener('click',async()=>{
  const right=$('integrationRightSource').value, left=$('integrationLeftKey').value, rkey=$('integrationRightKey').value;
  if(!right||!left||!rkey){toast('Choose both join columns');return;}
  try{
    const d=await api('/api/integration/join',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({right_source_id:right,left_key:left,right_key:rkey,how:$('integrationJoinType').value,normalize:$('integrationNormalize').checked})});
    toast(`Join added • ${d.report?.joins?.at(-1)?.match_rate ?? 0}% match`);await refreshIntegration();
  }catch(e){toast(e.message);}
});
$('buildIntegrationBtn')?.addEventListener('click',async()=>{
  try{const d=await api('/api/integration/build',{method:'POST'});toast(`✓ ${d.rows} rows combined. Dataset is ready for validation.`);await refreshDashboard();await refreshIntegration();await refreshProfile();await refreshValidationArea();}catch(e){toast(e.message);}
});
