from typing import Optional
from uuid import UUID

from sqlalchemy.orm import Session

from app.models import Report, ReportStatus


def create_report_entry(db: Session) -> str:
    """ Create a new report entry in the database """
    report = Report(status=ReportStatus.pending)
    db.add(report)
    db.commit()
    db.refresh(report)
    return str(report.id)


def check_report_status(db: Session, report_id: UUID) -> Optional[ReportStatus]:
    """ Check the current status of the report with the given report_id. """
    report = db.query(Report).filter(Report.id == report_id).first()
    return report.status if report else None


def run_report(report_id: str):
    pass
