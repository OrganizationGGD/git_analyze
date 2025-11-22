from contextlib import contextmanager
from sqlalchemy import text
import pandas as pd

from src.storage.unit_of_work import UnitOfWork
from src.analysis.correlation.models.models import CommitCorrelationResult


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

    # ----------- LOADERS -----------

    def load_commits(self) -> pd.DataFrame:
        """
        Load minimal commit info: repo_id, commit_date, message.
        """
        with self.session_scope() as session:
            query = text("""
                SELECT
                    repo_id,
                    commit_date,
                    message
                FROM commits
                WHERE commit_date IS NOT NULL
            """)

            df = pd.read_sql(query, session.connection())
            print(f"[CorrelationRepository] Loaded {len(df)} commits")
            return df

    def load_pull_requests(self) -> pd.DataFrame:
        """
        Load minimal PR info needed for lead time:
          - id
          - repo_id
          - number
          - created_at (for start of lead time)
          - merged_at (optional; we’ll use commit_date as end)
        """
        with self.session_scope() as session:
            query = text("""
                SELECT
                    id AS pr_id,
                    repo_id,
                    number AS pr_number,
                    author_id AS pr_author_id,
                    created_at AS pr_created_at,
                    merged_at AS pr_merged_at
                FROM pull_requests
                WHERE created_at IS NOT NULL
            """)

            df = pd.read_sql(query, session.connection())
            print(f"[CorrelationRepository] Loaded {len(df)} pull_requests")
            return df

    # ----------- SAVER -----------

    def save_correlation_result(self, result_dict: dict) -> None:
        """
        Save a single global correlation result row.
        We KEEP history; no deletes.
        """
        with self.session_scope() as session:
            row = CommitCorrelationResult(
                commit_corr_week_of_year=result_dict["commit_corr_week_of_year"],
                commit_corr_week_index=result_dict["commit_corr_week_index"],
                pr_corr_week_of_year=result_dict["pr_corr_week_of_year"],
                pr_corr_week_index=result_dict["pr_corr_week_index"],
            )
            session.add(row)
            print("[CorrelationRepository] Saved correlation result to database")
