import enum
from datetime import datetime, timedelta, time
from typing import Optional
from uuid import UUID

from sqlalchemy import func, ForeignKey, Identity, text, BigInteger, Text, UniqueConstraint
from sqlalchemy.orm import mapped_column, Mapped, relationship

from app.database import Base


class StoreStatus(enum.Enum):
    active = "active"
    inactive = "inactive"


class ReportStatus(enum.Enum):
    completed = "completed"
    pending = "pending"


class Store(Base):
    __tablename__ = "store"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    timezone_str: Mapped[str] = mapped_column(Text)


class StoreTimings(Base):
    __tablename__ = "store_timing"

    id: Mapped[int] = mapped_column(Identity(), primary_key=True)
    store_id: Mapped[int] = mapped_column(BigInteger)
    day: Mapped[int]
    start_time_local: Mapped[time]
    end_time_local: Mapped[time]


class StoreObservation(Base):
    __tablename__ = "store_observation"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    store_id: Mapped[int] = mapped_column(BigInteger)
    timestamp_utc: Mapped[datetime]
    status: Mapped[StoreStatus]


class Report(Base):
    __tablename__ = "report"

    id: Mapped[UUID] = mapped_column(primary_key=True, server_default=text("gen_random_uuid()"))
    status: Mapped[ReportStatus]
    started: Mapped[datetime] = mapped_column(server_default=func.now())
    completed: Mapped[Optional[datetime]]
    items: Mapped[list["ReportItem"]] = relationship("ReportItem", back_populates="report")


class ReportItem(Base):
    __tablename__ = "report_item"

    id: Mapped[int] = mapped_column(Identity(), primary_key=True)
    report_id: Mapped[UUID] = mapped_column(ForeignKey("report.id"))
    store_id: Mapped[int] = mapped_column(BigInteger)
    uptime_last_hour: Mapped[Optional[timedelta]]
    uptime_last_day: Mapped[Optional[timedelta]]
    uptime_last_week: Mapped[Optional[timedelta]]
    downtime_last_hour: Mapped[Optional[timedelta]]
    downtime_last_day: Mapped[Optional[timedelta]]
    downtime_last_week: Mapped[Optional[timedelta]]

    report: Mapped[Report] = relationship("Report", back_populates="items")

    __table_args__ = (UniqueConstraint(report_id, store_id),)

