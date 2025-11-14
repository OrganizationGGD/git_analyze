# src/analysis/correlation/models/models.py
from sqlalchemy import Column, Integer, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()


class CommitCorrelationGlobal(Base):
    # Глобальная корреляция между номером недели и числом коммитов
    __tablename__ = 'commit_correlation_global'

    id = Column(Integer, primary_key=True, autoincrement=True)
    correlation = Column(Float, nullable=False)
    repo_count = Column(Integer, nullable=False)   # сколько репозиториев участвовало
    created_at = Column(DateTime, default=datetime.utcnow)


class CommitCorrelationByRepo(Base):
    # Корреляция для каждого репозитория.
    __tablename__ = 'commit_correlation_by_repo'

    id = Column(Integer, primary_key=True, autoincrement=True)
    repo_id = Column(Integer, index=True, nullable=False)
    correlation = Column(Float, nullable=False)
    points = Column(Integer, nullable=True)        # количество weekly-точек для этого репо
    created_at = Column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<CommitCorrelationByRepo(repo_id={self.repo_id}, corr={self.correlation:.4f})>"
