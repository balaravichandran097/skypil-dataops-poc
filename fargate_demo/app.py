import csv, io, threading, time, uuid
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import HTMLResponse, Response
from pydantic import BaseModel, Field

app = FastAPI(title="SkyPIL Fargate Mock Demo")
lock = threading.Lock()
active = {"id": None}
jobs, inputs, outputs = {}, {}, {}
MAX_SIZE = 10 * 1024 * 1024

def field(name, kind="string", required=False, default=None, choices=None):
    return {"name":name,"kind":kind,"required":required,"default":default,"choices":choices or [],"extensions":[".csv"] if kind=="file" else [],"minimum":1 if kind=="integer" else None,"maximum":10000 if kind=="integer" else None}
env=field("environment",required=True,choices=["dev","test"])
partner=field("partner",required=True)
filefield=field("input_file","file",True)
dry=field("dry_run","boolean",default=True)
maximum=field("max_records","integer",default=1000)
tasks={
 "account-device-associations":{"category":"extract","description":"Read-only account/device associations CSV (mock data)","readOnly":True,"requiresInputFile":False,"requiresApproval":False,"supportsDryRun":False,"parameters":[env],"outputs":["account-device-associations.csv"]},
 "delete-accounts":{"category":"delete","description":"Account deletion CSV preview (NEVER deletes data)","readOnly":False,"requiresInputFile":True,"requiresApproval":True,"supportsDryRun":True,"parameters":[env,partner,filefield,dry,maximum],"outputs":["delete-preview.csv"]},
 "purchase-balance-reset":{"category":"update","description":"Purchase balance reset preview (NEVER updates data)","readOnly":False,"requiresInputFile":True,"requiresApproval":True,"supportsDryRun":True,"parameters":[env,partner,filefield,dry,maximum],"outputs":["balance-reset-preview.csv"]}
}
class Job(BaseModel):
 task:str
 environment:str
 parameters:dict=Field(default_factory=dict)

def check(req):
 if req.task not in tasks: raise HTTPException(400,"Task not approved")
 spec=tasks[req.task]
 if req.environment not in ("dev","test"): raise HTTPException(400,"Only dev/test allowed")
 if set(req.parameters)-{p["name"] for p in spec["parameters"]}: raise HTTPException(400,"Unknown parameters")
 p=dict(req.parameters)
 if p.get("environment")!=req.environment: raise HTTPException(400,"Environment mismatch")
 if not spec["readOnly"]:
  if p.get("dry_run",True) is not True: raise HTTPException(400,"Live mutation is prohibited")
  if not isinstance(p.get("partner"),str) or not p["partner"].strip(): raise HTTPException(400,"Partner required")
  if p.get("input_file") not in inputs: raise HTTPException(400,"CSV upload required")
  n=p.get("max_records",1000)
  if type(n) is not int or not 1<=n<=10000: raise HTTPException(400,"Invalid maximum record count")
 if spec["readOnly"] and set(p)!={"environment"}: raise HTTPException(400,"Unknown extract parameter")
 return spec,p

@app.get("/health")
def health(): return {"status":"ok","mode":"mock","awsExecution":False}
@app.get("/tasks")
def listing(): return {"tasks":[{"name":name,**spec,"environments":["dev","test"],"maxRecords":10000} for name,spec in tasks.items()],"activeJobId":active["id"]}
@app.get("/tasks/{name}")
def detail(name:str):
 if name not in tasks: raise HTTPException(404,"Unknown task")
 return {"name":name,**tasks[name]}
@app.post("/demo/upload")
async def upload(file:UploadFile=File(...)):
 name=file.filename or ""
 if not name.lower().endswith(".csv") or "/" in name or chr(92) in name: raise HTTPException(400,"CSV only")
 data=await file.read(MAX_SIZE+1)
 if len(data)>MAX_SIZE: raise HTTPException(413,"Maximum 10 MB")
 key=uuid.uuid4().hex+".csv"
 inputs[key]=data
 return {"inputFile":key,"filename":name,"size":len(data)}
@app.post("/jobs/validate")
def validation(req:Job):
 check(req)
 return {"valid":True,"mode":"mock","warning":"No AWS or production data is touched"}

def generate(jid,params):
 try:
  jobs[jid]["status"]="RUNNING"
  time.sleep(1)
  name=jobs[jid]["task"]
  if name=="account-device-associations":
   rows=[{"accountId":"demo-account","deviceId":"demo-device","environment":params["environment"]}]
  else:
   reader=csv.DictReader(io.StringIO(inputs[params["input_file"]].decode("utf-8-sig")))
   if not reader.fieldnames or "accountId" not in reader.fieldnames: raise ValueError("CSV requires accountId column")
   rows=[]
   for record in reader:
    if len(rows)>=params.get("max_records",1000): raise ValueError("Maximum record limit exceeded")
    aid=(record.get("accountId") or "").strip()
    if not aid: raise ValueError("Blank accountId")
    rows.append({"accountId":aid,"partner":params["partner"],"operation":"DELETE_PREVIEW" if name=="delete-accounts" else "RESET_PREVIEW","status":"NOT_EXECUTED"})
  result=io.StringIO()
  writer=csv.DictWriter(result,fieldnames=list(rows[0]) if rows else ["accountId","status"])
  writer.writeheader()
  writer.writerows(rows)
  outputs[jid]=result.getvalue().encode()
  jobs[jid].update(status="SUCCEEDED",processedCount=len(rows),affectedCount=0 if name=="account-device-associations" else len(rows),preview=rows[:100],outputFiles=[{"path":"preview.csv","url":"/jobs/"+jid+"/download"}])
 except Exception as e: jobs[jid].update(status="FAILED",errorMessage=str(e),errorCount=1)
 finally:
  with lock:
   if active["id"]==jid: active["id"]=None

@app.post("/jobs")
def create(req:Job):
 spec,params=check(req)
 with lock:
  if active["id"]:
   prev=active["id"]
   raise HTTPException(409,{"error":"JOB_ALREADY_RUNNING","message":"Another SkyPIL demo job is running","activeJobId":prev,"activeTask":jobs[prev]["task"]})
  jid="demo-"+uuid.uuid4().hex[:12]
  active["id"]=jid
  jobs[jid]={"jobId":jid,"task":req.task,"environment":req.environment,"status":"SUBMITTED","dryRun":not spec["readOnly"],"mode":"mock","userId":"demo-user","processedCount":0,"affectedCount":0,"errorCount":0,"outputFiles":[]}
 threading.Thread(target=generate,args=(jid,params),daemon=True).start()
 return {"jobId":jid,"status":"SUBMITTED","task":req.task,"environment":req.environment,"dryRun":not spec["readOnly"],"mode":"mock"}
@app.get("/jobs/{jid}")
def status(jid:str):
 if jid not in jobs: raise HTTPException(404,"Job not found")
 return jobs[jid]
@app.get("/jobs/{jid}/outputs")
def artifacts(jid:str):
 if jid not in jobs: raise HTTPException(404,"Job not found")
 return {"files":jobs[jid]["outputFiles"]}
@app.get("/jobs/{jid}/download")
def download(jid:str):
 if jid not in outputs: raise HTTPException(404,"No output")
 return Response(outputs[jid],media_type="text/csv",headers={"Content-Disposition":"attachment; filename=skypil-preview.csv"})
@app.post("/jobs/{jid}/cancel")
def cancel(jid:str): raise HTTPException(409,"Mock cancellation unavailable")

@app.get("/",response_class=HTMLResponse)
def home():
 return """<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>SkyPIL Operations Demo</title><style>
*{box-sizing:border-box}body{font:15px system-ui;background:#f2f6fa;color:#203348;margin:0}main{max-width:1040px;margin:32px auto;padding:20px}h1{font-size:30px;margin-bottom:4px}.notice{background:#fff4d7;border:1px solid #e1c778;padding:16px;border-radius:9px;margin:20px 0}.grid{display:grid;grid-template-columns:300px 1fr;gap:20px}.card{background:white;border:1px solid #dae3eb;padding:24px;margin-bottom:20px;border-radius:12px}h2{margin-top:0;font-size:19px}.task{display:block;width:100%;margin:8px 0;background:#dceafa;color:#204d77;text-align:left}.selected{background:#b8d9fc}button{border:0;border-radius:7px;padding:11px 16px;background:#176ba6;color:white;cursor:pointer;font-weight:600}button:disabled{opacity:.4}label{display:block;margin:14px 0;font-weight:600}input,select{display:block;width:100%;padding:10px;margin-top:6px;border-radius:6px;border:1px solid #bbcbd7}input[type=checkbox]{display:inline;width:auto}pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#f3f6f9;padding:15px;border-radius:8px;max-height:300px;overflow:auto}.warning{color:#a04d17;font-weight:600}a{display:block;color:#1467aa}@media(max-width:650px){.grid{grid-template-columns:1fr}}
</style></head><body><main><h1>SkyPIL Operations</h1><p>Three approved tasks · CSV preview · One active demo job</p><div class="notice"><b>MOCK DEMO ONLY:</b> This Render app never calls AWS and never changes real data. Uploaded CSV files stay temporarily in process memory. Do not upload sensitive production data.</div><div class="grid"><section class="card"><h2>Approved tasks</h2><div id="tasks"></div><p id="active"></p></section><section class="card"><h2>Run task</h2><p id="description">Select a task</p><form id="fields"></form><p id="warning" class="warning"></p><button id="run" disabled>Validate &amp; submit</button><p id="upload"></p></section></div><section class="card"><h2>Job status &amp; results</h2><button id="refresh" disabled>Refresh</button><p id="status">No active job</p><pre id="result">Results will appear here</pre><div id="outputs"></div></section></main><script>
let selected=null,jid=null;const E=id=>document.getElementById(id);async function api(url,opt={}){let r=await fetch(url,opt),b=await r.json();if(!r.ok)throw Error(JSON.stringify(b.detail||b));return b}
async function list(){let b=await api('/tasks');E('tasks').replaceChildren();for(let t of b.tasks){let btn=document.createElement('button');btn.className='task';btn.textContent=t.name+' — '+(t.readOnly?'extract':'dry-run');btn.onclick=()=>choose(t,btn);E('tasks').append(btn)}E('active').textContent=b.activeJobId?'Active job: '+b.activeJobId:'No running job'}
function choose(t,b){selected=t;document.querySelectorAll('.task').forEach(x=>x.classList.remove('selected'));b.classList.add('selected');E('fields').replaceChildren();E('description').textContent=t.description;for(let p of t.parameters){let label=document.createElement('label');label.textContent=p.name+(p.required?' *':'');let x;if(p.choices.length){x=document.createElement('select');for(let c of p.choices){let o=document.createElement('option');o.value=c;o.textContent=c;x.append(o)}}else{x=document.createElement('input');x.type=p.kind==='boolean'?'checkbox':p.kind==='integer'?'number':p.kind==='file'?'file':'text';if(p.kind==='integer'){x.min=1;x.max=10000}if(p.kind==='file')x.accept='.csv'}x.id='p_'+p.name;if(p.kind==='boolean'){x.checked=true;x.disabled=true}else if(p.default!==null&&p.kind!=='file')x.value=p.default;label.append(x);E('fields').append(label)}E('warning').textContent=t.readOnly?'':'WARNING: preview only; no live mutations';E('run').disabled=false}
E('run').onclick=async()=>{E('run').disabled=true;try{let p={};for(let f of selected.parameters){let x=E('p_'+f.name);p[f.name]=f.kind==='boolean'?x.checked:f.kind==='integer'?Number(x.value):f.kind==='file'?null:x.value}if(selected.requiresInputFile){let f=E('p_input_file').files[0];if(!f)throw Error('Choose a CSV file');let data=new FormData();data.append('file',f);E('upload').textContent='Uploading preview file…';let u=await api('/demo/upload',{method:'POST',body:data});p.input_file=u.inputFile;E('upload').textContent='Uploaded '+u.filename}let req={task:selected.name,environment:p.environment,parameters:p},opt={method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(req)};await api('/jobs/validate',opt);let j=await api('/jobs',opt);jid=j.jobId;E('refresh').disabled=false;await refresh()}catch(e){E('status').textContent='Error';E('result').textContent=e.message}finally{E('run').disabled=false;list()}}
async function refresh(){if(!jid)return;let j=await api('/jobs/'+jid);E('status').textContent=j.status+' · '+jid;E('result').textContent=JSON.stringify(j,null,2);E('outputs').replaceChildren();for(let f of j.outputFiles){let a=document.createElement('a');a.href=f.url;a.textContent='Download '+f.path;E('outputs').append(a)}if(j.status==='RUNNING'||j.status==='SUBMITTED')setTimeout(refresh,800);list()}E('refresh').onclick=refresh;list();
</script></body></html>"""
