from contextlib import contextmanager
from sqlalchemy import text
from src.storage.unit_of_work import UnitOfWork
from src.analysis.activity.models.models import RepoActivityForecast
import pandas as pd


class ActivityRepository:
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

    def load_daily_commit_data(self) -> pd.DataFrame:
        with self.session_scope() as session:
            query = text("""
                SELECT
                    repo_id,
                    date_trunc('day', commit_date)::date AS activity_date,
                    COUNT(*) AS commit_count
                FROM commits
                GROUP BY repo_id, activity_date
                ORDER BY repo_id, activity_date
            """)

            df = pd.read_sql(query, session.connection())
            print(f"[ActivityRepository] Loaded {len(df)} daily activity rows")
            return df

    def save_activity_forecast(self, forecast_df: pd.DataFrame) -> None:
        with self.session_scope() as session:
            session.query(RepoActivityForecast).delete()

            rows = []
            for _, row in forecast_df.iterrows():
                rows.append(
                    RepoActivityForecast(
                        repo_id=int(row["repo_id"]),
                        activity_date=row["activity_date"],
                        actual_commits=int(row["actual_commits"]),
                        predicted_commits=float(row["predicted_commits"]),
                        residual=float(row["residual"]),
                        z_score=float(row["z_score"]) if row["z_score"] is not None else None,
                        is_anomaly=bool(row["is_anomaly"]),
                        window_size=int(row["window_size"]),
                    )
                )

            session.add_all(rows)
            print(f"[ActivityRepository] Saved {len(rows)} forecast rows to database")
