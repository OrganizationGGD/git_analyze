from sqlalchemy import Column, Integer, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()


class CommitCorrelationResult(Base):
    __tablename__ = "commit_correlation_result"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Global correlations for commits
    commit_corr_week_of_year = Column(Float)   # corr(week_of_year, commit_count)
    commit_corr_week_index = Column(Float)     # corr(week_index, commit_count)

    # Global correlations for PR -> commit lead time
    pr_corr_week_of_year = Column(Float)       # corr(week_of_year, avg_lead_time_hours)
    pr_corr_week_index = Column(Float)         # corr(week_index, avg_lead_time_hours)

    computed_at = Column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return (
            f"<CommitCorrelationResult("
            f"commit_corr_week_index={self.commit_corr_week_index}, "
            f"pr_corr_week_index={self.pr_corr_week_index}"
            f")>"
        )
