from sqlalchemy import Column, Integer, Float, Date, DateTime, Boolean, String
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()


class RepoActivityForecast(Base):
    __tablename__ = "repo_activity_forecast"

    id = Column(Integer, primary_key=True, autoincrement=True)

    repo_id = Column(Integer, nullable=False, index=True)
    activity_date = Column(Date, nullable=False, index=True)

    actual_commits = Column(Integer, nullable=False)
    predicted_commits = Column(Float, nullable=False)
    residual = Column(Float, nullable=False)
    z_score = Column(Float)
    is_anomaly = Column(Boolean, default=False)

    model_type = Column(String(50), default="holt_winters")
    seasonal_periods = Column(Integer, default=7)
    created_at = Column(DateTime, default=datetime.utcnow)
