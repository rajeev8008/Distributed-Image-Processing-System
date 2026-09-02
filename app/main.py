from fastapi import Depends, FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.api.jobs import router as jobs_router
from app.db.session import get_db

app = FastAPI(title="Distributed Image Processing System")
app.include_router(jobs_router)
templates = Jinja2Templates(directory="app/templates")


@app.get("/", response_class=HTMLResponse)
def upload_page(request: Request):
    return templates.TemplateResponse(request, "upload.html")


@app.get("/jobs/{job_id}/view", response_class=HTMLResponse)
def job_page(request: Request, job_id: str):
    return templates.TemplateResponse(request, "job.html", {"job_id": job_id})


@app.get("/health")
def health(db: Session = Depends(get_db)):
    db.execute(text("SELECT 1"))
    return {"status": "ok"}
