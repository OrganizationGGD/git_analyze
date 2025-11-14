from contextlib import contextmanager
from sqlalchemy import text
from src.storage.unit_of_work import UnitOfWork
from src.analysis.correlation.models.models import (
    CommitCorrelationGlobal,
    CommitCorrelationByRepo,
)
import pandas as pd


class CorrelationRepository:
    def __init__(self, database_url: str | None = None):
        self.database_url = database_url

    @contextmanager
    def session_scope(self):
        uow = UnitOfWork(self.database_url)
        session = uow.get_session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def load_weekly_commit_data(self) -> pd.DataFrame:
        with self.session_scope() as session:
            query = text("""
                SELECT
                    repo_id,
                    date_trunc('week', commit_date) AS week_start,
                    COUNT(*) AS commit_count
                FROM commits
                GROUP BY repo_id, week_start
                ORDER BY week_start
            """)

            df = pd.read_sql(query, session.connection())
            print(f"Loaded {len(df)} weekly commit rows")
            return df

    def save_correlation_results(
        self,
        global_corr: float,
        corr_by_repo: pd.Series,
        weekly_df: pd.DataFrame,
    ) -> None:
        with self.session_scope() as session:
            session.query(CommitCorrelationGlobal).delete()
            session.query(CommitCorrelationByRepo).delete()

            global_row = CommitCorrelationGlobal(
                correlation=global_corr,
                repo_count=len(corr_by_repo),
            )
            session.add(global_row)

            points_per_repo = weekly_df.groupby("repo_id").size()

            rows = []
            for repo_id, corr in corr_by_repo.items():
                rows.append(
                    CommitCorrelationByRepo(
                        repo_id=int(repo_id),
                        correlation=float(corr),
                        points=int(points_per_repo.get(repo_id, 0)),
                    )
                )

            session.add_all(rows)
            print(f"Saved {len(rows)} per-repo correlation rows to database")
