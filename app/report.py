import logging
from typing import Optional
from uuid import UUID

import pandas as pd
from pandas import DataFrame
from sqlalchemy import text
from sqlalchemy.orm import Session

from app import uptime
from app.database import engine
from app.models import Report, ReportStatus


def create_entry(db: Session) -> str:
    """ Create a new report entry in the database """
    report = Report(status=ReportStatus.pending)
    db.add(report)
    db.commit()
    db.refresh(report)
    return str(report.id)


def check_status(db: Session, report_id: UUID) -> Optional[ReportStatus]:
    """ Check the current status of the report with the given report_id. """
    report = db.query(Report).filter(Report.id == report_id).first()
    return report.status if report else None


def run(report_id: UUID):
    """ Run the uptime computation report. """
    conn = engine.connect()
    try:
        reference_ts = uptime.get_max_timestamp(conn)
        uptime.compute(conn, report_id, reference_ts)

        conn.execute(text("""
            UPDATE report
               SET status = 'completed'
                 , completed = clock_timestamp()
             WHERE id = :report_id
        """), {"report_id": report_id})
    except Exception as e:
        logging.exception(e, exc_info=True)
        conn.rollback()
    else:
        conn.commit()
    finally:
        conn.close()


def retrieve(db: Session, report_id: UUID) -> DataFrame:
    """ Retrieve the given report from the database """
    return pd\
        .read_sql_query(
            text("""
                SELECT store_id
                     , EXTRACT(EPOCH FROM uptime_last_hour)::INT / 60 AS "uptime_last_hour(in hours)"
                     , EXTRACT(EPOCH FROM uptime_last_day)::INT / 3600 AS "uptime_last_day(in hours)"
                     , EXTRACT(EPOCH FROM uptime_last_week)::INT / 3600 AS "uptime_last_week(in hours)"
                     , EXTRACT(EPOCH FROM downtime_last_hour)::INT / 60 AS "downtime_last_hour(in minutes)"
                     , EXTRACT(EPOCH FROM downtime_last_day)::INT / 3600 AS "downtime_last_day(in hours)"
                     , EXTRACT(EPOCH FROM downtime_last_week)::INT / 3600 AS "downtime_last_week(in hours)"
                  FROM report_item
                 WHERE report_id = :report_id
            """),
            db.connection(),
            params={"report_id": str(report_id)}
        )\
        .astype("Int64")
