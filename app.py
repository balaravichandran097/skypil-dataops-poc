import os, io, csv, json, sqlite3, threading, time, uuid, secrets, base64, html
from pathlib import Path
from contextlib import contextmanager
from fastapi import FastAPI, HTTPException, UploadFile, File, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

ROOT=Path(os.getenv("SKYPIL_DATA_DIR","/tmp/skypil-dataops"))
ROOT.mkdir(parents=True,exist_ok=True)
DB=ROOT/"jobs.db"
app=FastAPI(title="SkyPIL DataOps Live POC",version="0.2")
mutex=threading.Lock()
def conn():
    c=sqlite3.connect(DB,timeout=30,check_same_thread=False)
    c.row_factory=sqlite3.Row
    return c
def setup():
    with conn() as c:
        c.execute("CREATE TABLE IF NOT EXISTS jobs(id TEXT PRIMARY KEY, task TEXT, mode TEXT, environment TEXT, partner TEXT, max_records INTEGER, upload_id TEXT, preview_id TEXT, status TEXT, created REAL, started REAL, completed REAL, result TEXT, approved_by TEXT)")
        c.execute("CREATE TABLE IF NOT EXISTS uploads(id TEXT PRIMARY KEY, filename TEXT, data TEXT)")
        c.execute("CREATE TABLE IF NOT EXISTS audit(id INTEGER PRIMARY KEY AUTOINCREMENT,job_id TEXT,event TEXT,at REAL)")
        c.execute("CREATE TABLE IF NOT EXISTS records(partner TEXT, account_id TEXT, name TEXT, PRIMARY KEY(partner,account_id))")
        if c.execute("SELECT COUNT(*) FROM records").fetchone()[0]==0:
            c.executemany("INSERT INTO records VALUES (?,?,?)",[("demo",f"A{n:03d}",f"Demo Account {n}") for n in range(1,21)])
setup()
@app.middleware("http")
async def basic_auth(request:Request,call_next):
    if request.url.path=="/health": return await call_next(request)
    pw=os.getenv("SKYPIL_DEMO_PASSWORD","")
    if not pw: return JSONResponse({"detail":"Demo password is not configured"},status_code=503)
    try:
        auth=request.headers.get("authorization","")
        raw=base64.b64decode(auth[6:],validate=True).decode() if auth.startswith("Basic ") else ""
        user,sep,value=raw.partition(":")
        okay=sep==":" and secrets.compare_digest(user,"demo") and secrets.compare_digest(value,pw)
    except Exception: okay=False
    if not okay:
        return JSONResponse({"detail":"Authentication required"},status_code=401,headers={"WWW-Authenticate":'Basic realm="SkyPIL Live Demo"'})
    return await call_next(request)
@app.get("/health")
def health(): return {"status":"ok"}
TASKS=[
 {"name":"delete-accounts","category":"delete","description":"Preview and delete demo accounts using a CSV upload","requires_approval":True,"parameters":[{"name":"partner","type":"string","required":True},{"name":"input_file","type":"file","required":True,"extensions":[".csv"]},{"name":"max_records","type":"integer","default":1000,"minimum":1,"maximum":10000}]},
 {"name":"extract-accounts","category":"extract","description":"Export matching demo accounts as CSV","requires_approval":False,"parameters":[{"name":"partner","type":"string","required":True},{"name":"max_records","type":"integer","default":1000,"minimum":1,"maximum":10000}]}]
def row(r):
    d=dict(r)
    if d.get("result"): d["result"]=json.loads(d["result"])
    return d
def event(c,jid,name): c.execute("INSERT INTO audit(job_id,event,at) VALUES(?,?,?)",(jid,name,time.time()))
@app.get("/api/tasks")
def tasks(): return TASKS
@app.get("/api/status")
def status():
    with conn() as c:
        return {"running":c.execute("SELECT COUNT(*) FROM jobs WHERE status='RUNNING'").fetchone()[0],"queued":c.execute("SELECT COUNT(*) FROM jobs WHERE status='QUEUED'").fetchone()[0],"total":c.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]}
@app.get("/api/jobs")
def jobs():
    with conn() as c:return [row(x) for x in c.execute("SELECT * FROM jobs ORDER BY created DESC LIMIT 100")]
@app.get("/api/jobs/{jid}")
def job(jid:str):
    with conn() as c:
        r=c.execute("SELECT * FROM jobs WHERE id=?",(jid,)).fetchone()
        if not r:raise HTTPException(404,"Job not found")
        return row(r)
@app.get("/api/jobs/{jid}/audit")
def audit(jid:str):
    with conn() as c:return [dict(x) for x in c.execute("SELECT event,at FROM audit WHERE job_id=? ORDER BY id",(jid,))]
@app.post("/api/uploads")
async def upload(file:UploadFile=File(...)):
    if not file.filename or not file.filename.lower().endswith(".csv"):raise HTTPException(400,"CSV required")
    content=await file.read(131073)
    if len(content)>131072:raise HTTPException(413,"Demo max upload 128 KB")
    try:
        decoded=content.decode("utf-8-sig")
        data=list(csv.DictReader(io.StringIO(decoded)))
        if not data or "account_id" not in (data[0].keys()):raise ValueError("CSV must have an account_id column")
        if len(data)>5000:raise ValueError("Maximum 5000 rows")
    except (UnicodeDecodeError,ValueError,csv.Error) as e:raise HTTPException(400,str(e))
    uid=str(uuid.uuid4())
    with conn() as c:c.execute("INSERT INTO uploads VALUES(?,?,?)",(uid,Path(file.filename).name,json.dumps(data)))
    return {"upload_id":uid,"filename":Path(file.filename).name,"rows":len(data)}
class Submit(BaseModel):
    task:str
    mode:str="PREVIEW"
    environment:str="dev"
    parameters:dict=Field(default_factory=dict)
    preview_id:str|None=None
@app.post("/api/jobs",status_code=201)
def submit(req:Submit):
    if req.task not in [t["name"] for t in TASKS]:raise HTTPException(400,"Unknown task")
    if req.mode not in ("PREVIEW","APPLY"):raise HTTPException(400,"Invalid mode")
    if req.environment not in ("dev","test","204","207","prod"):raise HTTPException(400,"Invalid environment")
    partner=str(req.parameters.get("partner","")).strip()
    if not partner or len(partner)>100:raise HTTPException(400,"Partner required")
    try:maxrec=int(req.parameters.get("max_records",1000))
    except (ValueError,TypeError):raise HTTPException(400,"Invalid max_records")
    if not 1<=maxrec<=10000:raise HTTPException(400,"max_records must be between 1 and 10000")
    upload_id=str(req.parameters.get("input_file","")) if req.task=="delete-accounts" else None
    with mutex,conn() as c:
        if upload_id and not c.execute("SELECT 1 FROM uploads WHERE id=?",(upload_id,)).fetchone():raise HTTPException(400,"Upload missing")
        if req.task=="delete-accounts" and not upload_id:raise HTTPException(400,"Upload required")
        if req.task=="delete-accounts" and req.mode=="APPLY":
            old=c.execute("SELECT * FROM jobs WHERE id=?",(req.preview_id,)).fetchone()
            if not old or old["status"]!="SUCCEEDED" or old["mode"]!="PREVIEW" or old["task"]!=req.task or old["partner"]!=partner or old["upload_id"]!=upload_id or old["max_records"]!=maxrec or old["environment"]!=req.environment:
                raise HTTPException(400,"Matching successful preview required")
        jid=str(uuid.uuid4())
        state="WAITING_APPROVAL" if req.task=="delete-accounts" and req.mode=="APPLY" else "QUEUED"
        c.execute("INSERT INTO jobs(id,task,mode,environment,partner,max_records,upload_id,preview_id,status,created) VALUES(?,?,?,?,?,?,?,?,?,?)",(jid,req.task,req.mode,req.environment,partner,maxrec,upload_id,req.preview_id,state,time.time()))
        event(c,jid,"SUBMITTED")
        if state=="WAITING_APPROVAL":event(c,jid,"APPROVAL_REQUIRED")
        return {"id":jid,"status":state}
class Approval(BaseModel):approver:str
@app.post("/api/jobs/{jid}/approve")
def approve(jid:str,body:Approval):
    if not body.approver.strip():raise HTTPException(400,"Approver required")
    with mutex,conn() as c:
        r=c.execute("UPDATE jobs SET status='QUEUED', approved_by=? WHERE id=? AND status='WAITING_APPROVAL'",(body.approver.strip(),jid))
        if r.rowcount!=1:raise HTTPException(409,"Job cannot be approved")
        event(c,jid,"APPROVED")
        return {"id":jid,"status":"QUEUED"}
@app.post("/api/jobs/{jid}/cancel")
def cancel(jid:str):
    with mutex,conn() as c:
        r=c.execute("UPDATE jobs SET status='CANCELLED', completed=? WHERE id=? AND status IN ('QUEUED','WAITING_APPROVAL')",(time.time(),jid))
        if r.rowcount!=1:raise HTTPException(409,"Only queued jobs can be cancelled in this demo")
        event(c,jid,"CANCELLED")
        return {"id":jid,"status":"CANCELLED"}
def work():
    while True:
        jid=None
        try:
            with mutex,conn() as c:
                c.execute("BEGIN IMMEDIATE")
                if not c.execute("SELECT 1 FROM jobs WHERE status='RUNNING'").fetchone():
                    r=c.execute("SELECT * FROM jobs WHERE status='QUEUED' ORDER BY created,id LIMIT 1").fetchone()
                    if r:
                        jid=r["id"]
                        c.execute("UPDATE jobs SET status='RUNNING',started=? WHERE id=?",(time.time(),jid))
                        event(c,jid,"STARTED")
            if jid:
                with conn() as c:
                    j=c.execute("SELECT * FROM jobs WHERE id=?",(jid,)).fetchone()
                    if j["task"]=="delete-accounts":
                        upload=c.execute("SELECT data FROM uploads WHERE id=?",(j["upload_id"],)).fetchone()
                        ids=list(dict.fromkeys(str(x.get("account_id","")).strip() for x in json.loads(upload["data"])))
                        ids=[x for x in ids if x][:j["max_records"]]
                        matches=[x["account_id"] for x in c.execute("SELECT account_id FROM records WHERE partner=?",(j["partner"],)) if x["account_id"] in ids]
                        if j["mode"]=="APPLY":
                            for aid in matches:c.execute("DELETE FROM records WHERE partner=? AND account_id=?",(j["partner"],aid))
                        result={"processed":len(ids),"matched":len(matches),"would_delete":len(matches) if j["mode"]=="PREVIEW" else 0,"deleted":len(matches) if j["mode"]=="APPLY" else 0,"accounts":matches}
                    else:
                        records=[dict(x) for x in c.execute("SELECT partner,account_id,name FROM records WHERE partner=? LIMIT ?",(j["partner"],j["max_records"]))]
                        result={"count":len(records),"rows":records}
                    c.execute("UPDATE jobs SET status='SUCCEEDED',completed=?,result=? WHERE id=?",(time.time(),json.dumps(result),jid))
                    event(c,jid,"SUCCEEDED")
        except Exception as exc:
            if jid:
                with conn() as c:
                    c.execute("UPDATE jobs SET status='FAILED',completed=?,result=? WHERE id=?",(time.time(),json.dumps({"error":str(exc)}),jid))
                    event(c,jid,"FAILED")
        time.sleep(0.7)
threading.Thread(target=work,name="single-worker",daemon=True).start()
@app.get("/api/jobs/{jid}/download")
def download(jid:str):
    with conn() as c:
        j=c.execute("SELECT * FROM jobs WHERE id=?",(jid,)).fetchone()
        if not j or j["task"]!="extract-accounts" or j["status"]!="SUCCEEDED":raise HTTPException(404,"Report not found")
        data=json.loads(j["result"])["rows"]
    f=io.StringIO();w=csv.DictWriter(f,fieldnames=["partner","account_id","name"]);w.writeheader();w.writerows(data)
    return StreamingResponse(iter([f.getvalue()]),media_type="text/csv",headers={"Content-Disposition":f'attachment; filename="extract-{jid}.csv"'})
@app.get("/",response_class=HTMLResponse)
def home():return HTML
HTML=r"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>SkyPIL DataOps</title><style>
*{box-sizing:border-box}body{margin:0;background:#f1f5fb;color:#18243c;font:14px system-ui,Arial}.nav{background:#122543;color:white;padding:19px 5%;display:flex;justify-content:space-between;align-items:center}.nav strong{font-size:23px}.nav span{color:#82bfff}.wrap{max-width:1180px;margin:24px auto;padding:0 14px}.metrics{display:grid;grid-template-columns:repeat(3,1fr);gap:14px}.metric,.panel{background:white;border:1px solid #dce5f0;border-radius:12px;padding:21px;box-shadow:0 3px 14px #2233550a}.metric b{font-size:32px;display:block;margin-top:8px}.muted{color:#64748b;font-size:12px}.cols{display:grid;grid-template-columns:1fr 1.25fr;gap:18px;margin-top:18px}label{display:block;font-weight:600;margin:15px 0 6px}input,select{width:100%;border:1px solid #ccd7e5;background:white;border-radius:8px;padding:11px;font-size:14px}button{background:#1768da;color:white;border:0;border-radius:8px;padding:11px 17px;font-weight:700;cursor:pointer}button.secondary{background:#eff4fb;color:#172b49;border:1px solid #cbd5e1}.buttons{display:flex;gap:8px;margin-top:18px;flex-wrap:wrap}.job{border:1px solid #d9e2ef;border-radius:9px;padding:12px;margin-top:9px;cursor:pointer}.job.active{border-color:#2563eb;background:#f8fbff}.pill{display:inline-block;padding:4px 8px;border-radius:20px;background:#e6efff;color:#1852a0;font-size:11px;font-weight:700}pre{white-space:pre-wrap;word-break:break-word;background:#14243f;color:#dceaff;padding:14px;border-radius:8px;max-height:250px;overflow:auto}.top{display:flex;justify-content:space-between;align-items:center;gap:10px}h2{font-size:18px;margin:0 0 16px}.notice{margin-top:12px;color:#245ea1;min-height:18px}#detail{border-top:1px solid #dbe5ee;margin-top:15px;padding-top:15px}.footer{margin-top:20px;font-size:12px;color:#64748b}@media(max-width:750px){.cols{grid-template-columns:1fr}.metric,.panel{padding:14px}}
</style></head><body><div class="nav"><strong>SkyPIL <span>DataOps</span></strong><small>LIVE POC · DEMO DATA ONLY</small></div><main class="wrap"><div class="metrics"><div class="metric"><span class="muted">Active execution</span><b id="running">0</b></div><div class="metric"><span class="muted">FIFO waiting</span><b id="queued">0</b></div><div class="metric"><span class="muted">Total jobs</span><b id="total">0</b></div></div><div class="cols"><div class="panel"><h2>Create execution</h2><label>Approved task</label><select id="task"></select><p class="muted" id="description"></p><label>Environment (simulated)</label><select id="env"><option>dev</option><option>test</option><option>204</option><option>207</option><option>prod</option></select><label>Partner</label><input id="partner" value="demo"><div id="fileBox"><label>CSV input (account_id column)</label><input type="file" id="file" accept=".csv"><p class="muted" id="uploadinfo">Upload a CSV with account_id column.</p></div><label>Maximum records</label><input id="max" type="number" min="1" max="10000" value="1000"><label>Mode</label><select id="mode"><option value="PREVIEW">Dry-run / Preview</option><option value="APPLY">Apply to demo data</option></select><div class="buttons"><button id="submit">Submit job</button><button class="secondary" id="refresh">Refresh</button></div><div id="msg" class="notice"></div></div><div class="panel"><div class="top"><h2>Execution history</h2><span class="pill">ONE WORKER</span></div><div id="jobs">No jobs submitted</div><div id="detail" hidden></div></div></div><p class="footer">Hosted demo uses a simulated account dataset. No AWS, Oracle, or production resources are connected. Jobs and files can reset when the hosting container restarts.</p></main><script>
let tasks=[],jobs=[],uploadId=null,previewId=null,selected=null;const $=x=>document.getElementById(x);const esc=s=>String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));async function api(p,o={}){let r=await fetch(p,o),data=await r.json();if(!r.ok)throw Error(data.detail||r.status);return data}function msg(m){$('msg').textContent=m}function taskChange(){let t=tasks.find(t=>t.name===$('task').value);$('description').textContent=t?.description||'';$('fileBox').hidden=t?.name!=='delete-accounts';previewId=null}async function load(){try{let s=await api('/api/status');$('running').textContent=s.running;$('queued').textContent=s.queued;$('total').textContent=s.total;jobs=await api('/api/jobs');$('jobs').innerHTML=jobs.length?jobs.map(j=>'<div class="job '+(j.id===selected?'active':'')+'" data-id="'+j.id+'"><div class="top"><b>'+esc(j.task)+'</b><span class="pill">'+esc(j.status)+'</span></div><div class="muted">'+esc(j.mode)+' · '+esc(j.environment)+' · '+j.id.slice(0,8)+'</div></div>').join(''):'<p class="muted">No jobs submitted yet.</p>';document.querySelectorAll('.job').forEach(x=>x.onclick=()=>{selected=x.dataset.id;load()});let j=jobs.find(j=>j.id===selected);let d=$('detail');d.hidden=!j;if(j){d.innerHTML='<h2>Job '+j.id.slice(0,8)+'</h2><p>Status: <b>'+esc(j.status)+'</b></p><pre>'+esc(JSON.stringify(j.result||{message:'Awaiting execution'},null,2))+'</pre><div class="buttons">'+(j.status==='WAITING_APPROVAL'?'<button id="approve">Approve</button>':'')+(j.status==='QUEUED'?'<button class="secondary" id="cancel">Cancel</button>':'')+(j.task==='extract-accounts'&&j.status==='SUCCEEDED'?'<button id="download">Download CSV</button>':'')+'</div><p class="muted" id="auditevents"></p>';let ap=$('approve');if(ap)ap.onclick=async()=>{let approver=prompt('Demo approver name (not production identity verification):');if(!approver)return;try{await api('/api/jobs/'+j.id+'/approve',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({approver})});load()}catch(e){msg(e.message)}};let cancel=$('cancel');if(cancel)cancel.onclick=async()=>{await api('/api/jobs/'+j.id+'/cancel',{method:'POST'});load()};let dl=$('download');if(dl)dl.onclick=()=>location.href='/api/jobs/'+j.id+'/download';api('/api/jobs/'+j.id+'/audit').then(e=>{let x=$('auditevents');if(x)x.textContent='Audit: '+e.map(a=>a.event).join(' → ')});if(j.task==='delete-accounts'&&j.mode==='PREVIEW'&&j.status==='SUCCEEDED'&&j.id===selected)previewId=j.id}}catch(e){msg(e.message)}}
$('file').onchange=async()=>{let file=$('file').files[0];if(!file)return;let f=new FormData();f.append('file',file);try{let x=await api('/api/uploads',{method:'POST',body:f});uploadId=x.upload_id;previewId=null;$('uploadinfo').textContent=x.filename+' · '+x.rows+' rows uploaded'}catch(e){msg(e.message)}};
$('submit').onclick=async()=>{let task=$('task').value,mode=$('mode').value,params={partner:$('partner').value,max_records:Number($('max').value)};if(task==='delete-accounts'){if(!uploadId){msg('Please upload a CSV first.');return}params.input_file=uploadId;if(mode==='APPLY'&&!previewId){msg('Run and inspect a matching successful preview first.');return}}try{let x=await api('/api/jobs',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({task,mode,environment:$('env').value,parameters:params,preview_id:previewId})});selected=x.id;msg('Job submitted: '+x.status);load()}catch(e){msg(e.message)}};$('refresh').onclick=load;
(async()=>{tasks=await api('/api/tasks');$('task').innerHTML=tasks.map(x=>'<option value="'+esc(x.name)+'">'+esc(x.name)+'</option>').join('');$('task').onchange=taskChange;taskChange();load();setInterval(load,4000)})()
</script></body></html>""";
