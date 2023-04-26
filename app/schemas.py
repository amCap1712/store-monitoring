from pydantic import BaseModel


class TriggerReportResponse(BaseModel):
    report_id: str


class RetrieveReportResponse(BaseModel):
    status: str
