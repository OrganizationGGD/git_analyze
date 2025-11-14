from sqlalchemy import Column, Integer, Float, Date, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()


class RepoActivityForecast(Base):
    __tablename__ = 'repo_activity_forecast'

    id = Column(Integer, primary_key=True, autoincrement=True)
    repo_id = Column(Integer, index=True, nullable=False)
    activity_date = Column(Date, index=True, nullable=False)

    actual_commits = Column(Integer, nullable=False)
    predicted_commits = Column(Float, nullable=False)
    residual = Column(Float, nullable=False)        # actual - predicted
    z_score = Column(Float)                         # (actual - mean) / std
    is_anomaly = Column(Boolean, default=False)

    window_size = Column(Integer, nullable=False)   # размер окна для скользящей статистики
    created_at = Column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return (f"<RepoActivityForecast(repo_id={self.repo_id}, "
                f"date={self.activity_date}, actual={self.actual_commits}, "
                f"pred={self.predicted_commits:.2f}, anomaly={self.is_anomaly})>")
