from uuid import UUID

from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from starlette.background import BackgroundTasks

from app import report
from app.database import SessionLocal
from app.schemas import TriggerReportResponse, RetrieveReportResponse

app = FastAPI()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/trigger_report", response_model=TriggerReportResponse)
async def trigger_report(background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """ Trigger a report generation """
    report_id = report.create_report_entry(db)
    background_tasks.add_task(report.run_report, report_id)
    return TriggerReportResponse(report_id=report_id)


@app.get("/get_report/{report_id}", response_model=RetrieveReportResponse)
async def retrieve_report(report_id: str, db: Session = Depends(get_db)):
    """ Retrieve a given report """
    # report id's are UUIDs, validate id before passing to postgres to avoid errors
    try:
        report_id = UUID(report_id)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Report Id '{report_id}' is invalid")

    status = report.check_report_status(db, report_id)
    if status is None:
        raise HTTPException(status_code=404, detail=f"Report '{report_id}' not found")
    return RetrieveReportResponse(status=status.value)
