"""SkyPIL DataOps v2: local demo engines, not AWS production adapters."""
import csv
import io
import json
import os
import secrets
import sqlite3
import threading
import time
import uuid
import base64
from pathlib import Path
from typing import Any
from fastapi import FastAPI, UploadFile, File, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

DATA_DIR=Path(os.environ.get("SKYPIL_DATA_DIR","/tmp/skypil-dataops"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB=DATA_DIR/"platform-v2.db"
app=FastAPI(title="SkyPIL DataOps Platform v2",version="2.0.0")
guard=threading.Lock()

def connect():
    c=sqlite3.connect(DB,timeout=30,check_same_thread=False)
    c.row_factory=sqlite3.Row
    c.execute("PRAGMA busy_timeout=30000")
    return c

def initialize():
    with connect() as db:
        db.executescript("""
        CREATE TABLE IF NOT EXISTS records(
          partner TEXT NOT NULL, account_id TEXT NOT NULL,
          status TEXT NOT NULL, entitlement TEXT NOT NULL,
          PRIMARY KEY(partner, account_id));
        CREATE TABLE IF NOT EXISTS uploads(
          id TEXT PRIMARY KEY, filename TEXT, rows_json TEXT NOT NULL,
          checksum TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS jobs(
          id TEXT PRIMARY KEY, engine TEXT NOT NULL, task TEXT NOT NULL,
          environment TEXT NOT NULL, mode TEXT NOT NULL,
          parameters TEXT NOT NULL, input_id TEXT, input_hash TEXT,
          preview_id TEXT, status TEXT NOT NULL, created REAL NOT NULL,
          started REAL, completed REAL, result TEXT, approved_by TEXT);
        CREATE TABLE IF NOT EXISTS audit(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          job_id TEXT NOT NULL, event TEXT NOT NULL, created REAL NOT NULL);
        """)
        if not db.execute("SELECT 1 FROM records LIMIT 1").fetchone():
            db.executemany("INSERT INTO records VALUES(?,?,?,?)",
                [("demo",f"A{i:03d}","ACTIVE","BASIC") for i in range(1,31)])
initialize()

@app.middleware("http")
async def auth(req:Request,call_next):
    if req.url.path=="/health":return await call_next(req)
    password=os.environ.get("SKYPIL_DEMO_PASSWORD")
    if not password:return JSONResponse({"detail":"Set SKYPIL_DEMO_PASSWORD"},status_code=503)
    try:
        raw=req.headers.get("authorization","")
        decoded=base64.b64decode(raw[6:],validate=True).decode() if raw.startswith("Basic ") else ""
        u,sep,p=decoded.partition(":")
        ok=sep==":" and secrets.compare_digest(u,"demo") and secrets.compare_digest(p,password)
    except Exception:ok=False
    if not ok:return JSONResponse({"detail":"Sign in required"},status_code=401,headers={"WWW-Authenticate":'Basic realm="SkyPIL DataOps Demo"'})
    return await call_next(req)

@app.get("/health")
def health():return {"status":"ok","version":"2.0.0"}

# Approved templates are a finite allow-list, not arbitrary table/attribute access.
TEMPLATES={
 "account-status-update":{"name":"Account status update","operation":"UPDATE","requires_approval":True,"file_required":True,"columns":["account_id","status"],"description":"Update a demo account's status from a CSV"},
 "account-entitlement-update":{"name":"Account entitlement update","operation":"UPDATE","requires_approval":True,"file_required":True,"columns":["account_id","entitlement"],"description":"Update demo entitlements from a CSV"},
 "account-delete":{"name":"Delete selected accounts","operation":"DELETE","requires_approval":True,"file_required":True,"columns":["account_id"],"description":"Delete a reviewed set of demo accounts"},
 "account-insert":{"name":"Insert accounts","operation":"INSERT","requires_approval":True,"file_required":True,"columns":["account_id","status","entitlement"],"description":"Create demo accounts if absent"},
 "account-extract":{"name":"Extract accounts","operation":"EXTRACT","requires_approval":False,"file_required":False,"columns":[],"description":"Export demo accounts to CSV"}
}
VALID_STATUSES={"ACTIVE","INACTIVE","SUSPENDED"}
VALID_ENTITLEMENTS={"BASIC","PREMIUM","SPORTS"}

# SDK: developers register reviewed, versioned business functions.
PLUGINS={}
def task(*,name,description,parameters,requires_approval=True):
    def decorator(fn):
        if name in PLUGINS:raise ValueError("duplicate plugin")
        PLUGINS[name]={"name":name,"description":description,"parameters":parameters,
                       "requires_approval":requires_approval,"handler":fn}
        return fn
    return decorator

class TaskContext:
    def __init__(self,db,mode,partner,limit):
        self.db=db
        self.dry_run=mode=="PREVIEW"
        self.partner=partner
        self.limit=limit
    def accounts(self):
        return [dict(x) for x in self.db.execute("SELECT * FROM records WHERE partner=? ORDER BY account_id LIMIT ?",(self.partner,self.limit))]
    def set_status(self,account_id,status):
        if status not in VALID_STATUSES:raise ValueError("invalid status")
        rec=self.db.execute("SELECT * FROM records WHERE partner=? AND account_id=?",(self.partner,account_id)).fetchone()
        if rec is None:return {"account_id":account_id,"action":"SKIP_MISSING"}
        if rec["status"]==status:return {"account_id":account_id,"action":"UNCHANGED"}
        if not self.dry_run:
            self.db.execute("UPDATE records SET status=? WHERE partner=? AND account_id=?",(status,self.partner,account_id))
        return {"account_id":account_id,"before":rec["status"],"after":status,"action":"WOULD_UPDATE" if self.dry_run else "UPDATED"}

@task(name="suspend-basic-accounts",description="Custom Python rule: suspend ACTIVE accounts with BASIC entitlement",
      parameters=[{"name":"partner","type":"string","required":True},{"name":"max_records","type":"integer","default":100}],
      requires_approval=True)
def suspend_basic(ctx:TaskContext,parameters:dict):
    changes=[]
    for rec in ctx.accounts():
        if rec["status"]=="ACTIVE" and rec["entitlement"]=="BASIC":
            changes.append(ctx.set_status(rec["account_id"],"SUSPENDED"))
    return {"actions":changes,"summary":{"matched":len(changes)}}

@task(name="normalize-inactive",description="Custom Python rule: normalize suspended demo accounts to INACTIVE",
      parameters=[{"name":"partner","type":"string","required":True},{"name":"max_records","type":"integer","default":100}],
      requires_approval=True)
def normalize_inactive(ctx:TaskContext,parameters:dict):
    actions=[]
    for rec in ctx.accounts():
        if rec["status"]=="SUSPENDED":actions.append(ctx.set_status(rec["account_id"],"INACTIVE"))
    return {"actions":actions,"summary":{"matched":len(actions)}}

# Reviewed workflow definitions are explicit activities; no Python eval, arbitrary HTTP or code upload.
WORKFLOWS={
 "reconcile-and-suspend":{
   "name":"Reconcile & suspend BASIC accounts",
   "description":"Validate sample records, generate a comparison, then suspend matching accounts",
   "requires_approval":True,
   "steps":[{"id":"validate","type":"validate_partner"},
            {"id":"compare","type":"compare_basic"},
            {"id":"update","type":"run_plugin","plugin":"suspend-basic-accounts"},
            {"id":"report","type":"summary"}]},
 "normalize-and-report":{
   "name":"Normalize & report suspended accounts",
   "description":"Validate, normalize suspended statuses, and generate a final report",
   "requires_approval":True,
   "steps":[{"id":"validate","type":"validate_partner"},
            {"id":"update","type":"run_plugin","plugin":"normalize-inactive"},
            {"id":"report","type":"summary"}]}
}
def record_event(db,jid,event):
    db.execute("INSERT INTO audit(job_id,event,created) VALUES(?,?,?)",(jid,event,time.time()))
def as_job(r):
    d=dict(r)
    for k in ("parameters","result"):
        if d.get(k):d[k]=json.loads(d[k])
    return d

@app.get("/api/catalog")
def catalog():
    return {"templates":TEMPLATES,
            "plugins":{k:{key:value for key,value in v.items() if key!="handler"} for k,v in PLUGINS.items()},
            "workflows":WORKFLOWS,
            "environments":["dev","test","204","207","prod"]}
@app.get("/api/status")
def platform_status():
    with connect() as db:
        counts={s:db.execute("SELECT COUNT(*) FROM jobs WHERE status=?",(s,)).fetchone()[0]
                for s in ("RUNNING","QUEUED","WAITING_APPROVAL","SUCCEEDED","FAILED")}
        counts["total"]=db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
        return counts
@app.get("/api/jobs")
def list_jobs():
    with connect() as db:return [as_job(x) for x in db.execute("SELECT * FROM jobs ORDER BY created DESC LIMIT 100")]
@app.get("/api/jobs/{job_id}")
def job_detail(job_id:str):
    with connect() as db:
        r=db.execute("SELECT * FROM jobs WHERE id=?",(job_id,)).fetchone()
        if r is None:raise HTTPException(404,"Job not found")
        return as_job(r)
@app.get("/api/jobs/{job_id}/audit")
def job_audit(job_id:str):
    with connect() as db:return [dict(x) for x in db.execute("SELECT event,created FROM audit WHERE job_id=? ORDER BY id",(job_id,))]
@app.get("/api/data")
def demo_data(partner:str="demo"):
    with connect() as db:return [dict(x) for x in db.execute("SELECT * FROM records WHERE partner=? ORDER BY account_id LIMIT 250",(partner,))]
@app.post("/api/uploads")
async def upload(file:UploadFile=File(...)):
    if not file.filename or not file.filename.lower().endswith(".csv"):raise HTTPException(400,"Upload a CSV file")
    binary=await file.read(262145)
    if len(binary)>262144:raise HTTPException(413,"Maximum demo upload size is 256 KB")
    try:
        rows=list(csv.DictReader(io.StringIO(binary.decode("utf-8-sig"))))
        if not rows or not rows[0] or "account_id" not in rows[0]:raise ValueError("CSV must include account_id header and rows")
        if len(rows)>5000:raise ValueError("Maximum 5,000 data rows")
    except (ValueError,UnicodeDecodeError,csv.Error) as exc:raise HTTPException(400,str(exc))
    import hashlib
    digest=hashlib.sha256(binary).hexdigest()
    uid=str(uuid.uuid4())
    with connect() as db:db.execute("INSERT INTO uploads VALUES(?,?,?,?)",(uid,Path(file.filename).name,json.dumps(rows),digest))
    return {"upload_id":uid,"filename":Path(file.filename).name,"rows":len(rows),"sha256":digest}

class JobRequest(BaseModel):
    engine:str
    task:str
    environment:str="dev"
    mode:str="PREVIEW"
    parameters:dict[str,Any]=Field(default_factory=dict)
    input_id:str|None=None
    preview_id:str|None=None

def validate_request(db,req):
    if req.engine not in ("generic","plugin","workflow"):raise HTTPException(400,"Unknown engine")
    registry={"generic":TEMPLATES,"plugin":PLUGINS,"workflow":WORKFLOWS}[req.engine]
    if req.task not in registry:raise HTTPException(400,"Task is not approved")
    if req.environment not in ("dev","test","204","207","prod"):raise HTTPException(400,"Unknown environment")
    if req.mode not in ("PREVIEW","APPLY"):raise HTTPException(400,"Unknown execution mode")
    partner=str(req.parameters.get("partner","")).strip()
    if not partner or len(partner)>60:raise HTTPException(400,"Partner required")
    try:limit=int(req.parameters.get("max_records",100))
    except (TypeError,ValueError):raise HTTPException(400,"Invalid max records")
    if not 1<=limit<=1000:raise HTTPException(400,"max_records must be 1–1000 in the demo")
    upload=None
    if req.input_id:
        upload=db.execute("SELECT * FROM uploads WHERE id=?",(req.input_id,)).fetchone()
        if upload is None:raise HTTPException(400,"Uploaded file not found")
    if req.engine=="generic" and TEMPLATES[req.task]["file_required"] and not upload:raise HTTPException(400,"This template requires CSV input")
    if req.engine=="generic" and upload:
        cols=TEMPLATES[req.task]["columns"]
        rows=json.loads(upload["rows_json"])
        for col in cols:
            if col not in rows[0]:raise HTTPException(400,f"CSV missing required column: {col}")
    # Preview replay is bound to the task, environment, exact parameter JSON and immutable uploaded content.
    canonical=json.dumps(req.parameters,sort_keys=True)
    if req.mode=="APPLY":
        preview=db.execute("SELECT * FROM jobs WHERE id=?",(req.preview_id,)).fetchone()
        if (not preview or preview["status"]!="SUCCEEDED" or preview["engine"]!=req.engine or
            preview["task"]!=req.task or preview["mode"]!="PREVIEW" or preview["environment"]!=req.environment or
            preview["parameters"]!=canonical or preview["input_hash"]!=(upload["checksum"] if upload else None)):
            raise HTTPException(400,"A matching completed preview is required")
    return canonical,upload

@app.post("/api/jobs",status_code=201)
def submit(req:JobRequest):
    with guard,connect() as db:
        canonical,upload=validate_request(db,req)
        jid=str(uuid.uuid4())
        registry={"generic":TEMPLATES,"plugin":PLUGINS,"workflow":WORKFLOWS}[req.engine]
        approval=req.mode=="APPLY" and registry[req.task]["requires_approval"]
        state="WAITING_APPROVAL" if approval else "QUEUED"
        db.execute("""INSERT INTO jobs(id,engine,task,environment,mode,parameters,input_id,input_hash,preview_id,status,created)
                  VALUES(?,?,?,?,?,?,?,?,?,?,?)""",(jid,req.engine,req.task,req.environment,req.mode,canonical,
                    req.input_id,upload["checksum"] if upload else None,req.preview_id,state,time.time()))
        record_event(db,jid,"SUBMITTED")
        record_event(db,jid,"APPROVAL_REQUIRED" if approval else "QUEUED")
        return {"id":jid,"status":state}

class Approval(BaseModel):approver:str
@app.post("/api/jobs/{jid}/approve")
def approve(jid:str,body:Approval):
    if not body.approver.strip():raise HTTPException(400,"Enter a demo approver name")
    with guard,connect() as db:
        r=db.execute("UPDATE jobs SET status='QUEUED',approved_by=? WHERE id=? AND status='WAITING_APPROVAL'",
                     (body.approver.strip(),jid))
        if r.rowcount!=1:raise HTTPException(409,"Job is not awaiting approval")
        record_event(db,jid,"APPROVED_BY:"+body.approver.strip()[:80])
        return {"id":jid,"status":"QUEUED"}

@app.post("/api/jobs/{jid}/cancel")
def cancel(jid:str):
    with guard,connect() as db:
        r=db.execute("UPDATE jobs SET status='CANCELLED',completed=? WHERE id=? AND status IN ('QUEUED','WAITING_APPROVAL')",
                     (time.time(),jid))
        if r.rowcount!=1:raise HTTPException(409,"Only pending jobs can be cancelled")
        record_event(db,jid,"CANCELLED")
        return {"id":jid,"status":"CANCELLED"}

def execute_generic(ctx,task,rows):
    definition=TEMPLATES[task]
    operation=definition["operation"]
    actions=[]
    if operation=="EXTRACT":return {"summary":{"exported":len(ctx.accounts())},"records":ctx.accounts()}
    for index,rec in enumerate(rows[:ctx.limit],1):
        aid=str(rec.get("account_id","")).strip()
        if not aid:actions.append({"row":index,"action":"SKIP_INVALID"});continue
        old=ctx.db.execute("SELECT * FROM records WHERE partner=? AND account_id=?",(ctx.partner,aid)).fetchone()
        action={"row":index,"account_id":aid}
        if operation=="DELETE":
            action["action"]="SKIP_MISSING" if not old else ("WOULD_DELETE" if ctx.dry_run else "DELETED")
            if old and not ctx.dry_run:ctx.db.execute("DELETE FROM records WHERE partner=? AND account_id=?",(ctx.partner,aid))
        elif operation=="INSERT":
            status=str(rec.get("status","")).upper().strip()
            entitlement=str(rec.get("entitlement","")).upper().strip()
            if status not in VALID_STATUSES or entitlement not in VALID_ENTITLEMENTS:
                action["action"]="INVALID_VALUE"
            elif old:action["action"]="ALREADY_EXISTS"
            else:
                action["action"]="WOULD_INSERT" if ctx.dry_run else "INSERTED"
                if not ctx.dry_run:ctx.db.execute("INSERT INTO records VALUES(?,?,?,?)",(ctx.partner,aid,status,entitlement))
        elif operation=="UPDATE":
            column="status" if task=="account-status-update" else "entitlement"
            value=str(rec.get(column,"")).upper().strip()
            allowed=VALID_STATUSES if column=="status" else VALID_ENTITLEMENTS
            if value not in allowed:action["action"]="INVALID_VALUE"
            elif not old:action["action"]="SKIP_MISSING"
            elif old[column]==value:action["action"]="UNCHANGED"
            else:
                action.update(before=old[column],after=value)
                action["action"]="WOULD_UPDATE" if ctx.dry_run else "UPDATED"
                if not ctx.dry_run:ctx.db.execute(f"UPDATE records SET {column}=? WHERE partner=? AND account_id=?",(value,ctx.partner,aid))
        actions.append(action)
    return {"summary":{"processed":len(actions),"changed":sum(x["action"] in ("WOULD_DELETE","DELETED","WOULD_UPDATE","UPDATED","WOULD_INSERT","INSERTED") for x in actions)},"actions":actions[:1000]}

def execute_workflow(ctx,name,params):
    workflow=WORKFLOWS[name]
    steps=[]
    for step in workflow["steps"]:
        t=step["type"]
        if t=="validate_partner":
            count=len(ctx.accounts())
            steps.append({"id":step["id"],"type":t,"status":"SUCCEEDED","found":count})
        elif t=="compare_basic":
            matched=sum(x["status"]=="ACTIVE" and x["entitlement"]=="BASIC" for x in ctx.accounts())
            steps.append({"id":step["id"],"type":t,"status":"SUCCEEDED","matched":matched})
        elif t=="run_plugin":
            plugin=PLUGINS[step["plugin"]]
            out=plugin["handler"](ctx,params)
            steps.append({"id":step["id"],"type":t,"status":"SUCCEEDED","task":step["plugin"],"result":out})
        elif t=="summary":
            steps.append({"id":step["id"],"type":t,"status":"SUCCEEDED","message":"Workflow reconciliation completed"})
        else:raise ValueError("Unknown step type")
    return {"steps":steps,"summary":{"completed_steps":len(steps)}}

def execute(db,job):
    params=json.loads(job["parameters"])
    ctx=TaskContext(db,job["mode"],str(params["partner"]),int(params.get("max_records",100)))
    if job["engine"]=="generic":
        upload=db.execute("SELECT rows_json FROM uploads WHERE id=?",(job["input_id"],)).fetchone() if job["input_id"] else None
        rows=json.loads(upload["rows_json"]) if upload else []
        return execute_generic(ctx,job["task"],rows)
    if job["engine"]=="plugin":
        return PLUGINS[job["task"]]["handler"](ctx,params)
    return execute_workflow(ctx,job["task"],params)

def worker():
    while True:
        jid=None
        try:
            with guard,connect() as db:
                db.execute("BEGIN IMMEDIATE")
                if not db.execute("SELECT 1 FROM jobs WHERE status='RUNNING'").fetchone():
                    item=db.execute("SELECT id FROM jobs WHERE status='QUEUED' ORDER BY created,id LIMIT 1").fetchone()
                    if item:
                        jid=item["id"]
                        db.execute("UPDATE jobs SET status='RUNNING',started=? WHERE id=?",(time.time(),jid))
                        record_event(db,jid,"STARTED")
            if jid:
                with connect() as db:
                    # A single transaction keeps demo mutations and job result atomic.
                    db.execute("BEGIN IMMEDIATE")
                    job=db.execute("SELECT * FROM jobs WHERE id=?",(jid,)).fetchone()
                    result=execute(db,job)
                    db.execute("UPDATE jobs SET status='SUCCEEDED',result=?,completed=? WHERE id=?",
                               (json.dumps(result),time.time(),jid))
                    record_event(db,jid,"SUCCEEDED")
        except Exception as exc:
            if jid:
                with connect() as db:
                    db.execute("UPDATE jobs SET status='FAILED',result=?,completed=? WHERE id=?",
                               (json.dumps({"error":str(exc)}),time.time(),jid))
                    record_event(db,jid,"FAILED")
        time.sleep(0.5)

threading.Thread(target=worker,name="demo-serial-worker",daemon=True).start()

@app.get("/api/jobs/{jid}/download")
def download(jid:str):
    with connect() as db:
        r=db.execute("SELECT * FROM jobs WHERE id=?",(jid,)).fetchone()
        if not r or r["status"]!="SUCCEEDED" or r["engine"]!="generic" or r["task"]!="account-extract":
            raise HTTPException(404,"Report unavailable")
        rows=json.loads(r["result"])["records"]
    buf=io.StringIO()
    writer=csv.DictWriter(buf,fieldnames=["partner","account_id","status","entitlement"])
    writer.writeheader();writer.writerows(rows)
    return StreamingResponse(iter([buf.getvalue()]),media_type="text/csv",
                             headers={"Content-Disposition":f'attachment; filename="skypil-{jid}.csv"'})

@app.get("/",response_class=HTMLResponse)
def homepage():
    return (Path(__file__).parent/"dashboard.html").read_text(encoding="utf-8")
