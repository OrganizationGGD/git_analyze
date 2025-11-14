import os
from typing import Dict, Any

import numpy as np
import pandas as pd

from src.analysis.activity.repo.repo import ActivityRepository
from src.storage.unit_of_work import UnitOfWork


class RepoActivityCore:

    # агрегация по дням
    # скользящее среднее / std как прогноз
    # z-score и аномалии

    def __init__(self, data_repo: ActivityRepository, window_size: int = 7, z_threshold: float = 3.0):
        self.data_repo = data_repo
        self.window_size = window_size
        self.z_threshold = z_threshold

    def run_analysis(self) -> Dict[str, Any]:
        df = self.data_repo.load_daily_commit_data()
        if df.empty:
            return {"error": "No daily activity data"}

        df["activity_date"] = pd.to_datetime(df["activity_date"]).dt.date
        df = df.sort_values(["repo_id", "activity_date"])

        results = []

        for repo_id, group in df.groupby("repo_id"):
            g = group.copy()
            g = g.sort_values("activity_date")

            g["actual_commits"] = g["commit_count"]

            g["rolling_mean"] = (
                g["actual_commits"]
                .rolling(window=self.window_size, min_periods=1)
                .mean()
                .shift(1)
            )
            g["rolling_std"] = (
                g["actual_commits"]
                .rolling(window=self.window_size, min_periods=2)
                .std()
                .shift(1)
            )

            g["predicted_commits"] = g["rolling_mean"].fillna(g["actual_commits"].mean())
            g["residual"] = g["actual_commits"] - g["predicted_commits"]

            def compute_z(row):
                std = row["rolling_std"]
                if std is None or np.isnan(std) or std == 0:
                    return np.nan
                return row["residual"] / std

            g["z_score"] = g.apply(compute_z, axis=1)

            g["is_anomaly"] = g["z_score"].abs() > self.z_threshold

            g["window_size"] = self.window_size

            results.append(
                g[
                    [
                        "repo_id",
                        "activity_date",
                        "actual_commits",
                        "predicted_commits",
                        "residual",
                        "z_score",
                        "is_anomaly",
                        "window_size",
                    ]
                ]
            )

        forecast_df = pd.concat(results, ignore_index=True)

        os.makedirs("artifacts", exist_ok=True)
        forecast_df.to_csv("artifacts/repo_activity_forecast.csv", index=False)

        anomalies_df = forecast_df[forecast_df["is_anomaly"] == True]
        anomalies_df.to_csv("artifacts/repo_activity_anomalies.csv", index=False)

        print("\nREPOSITORY ACTIVITY FORECAST & ANOMALIES")
        print("=======================================")
        print(f"Window size: {self.window_size} days, z-threshold: {self.z_threshold}")
        print(f"Total rows: {len(forecast_df)}, anomalies: {len(anomalies_df)}")

        print("\nSample anomalies:")
        print(
            anomalies_df.head(10)[
                ["repo_id", "activity_date", "actual_commits", "predicted_commits", "z_score"]
            ]
        )

        self.data_repo.save_activity_forecast(forecast_df)

        anomalies_by_repo = (
            anomalies_df.groupby("repo_id")
            .size()
            .sort_values(ascending=False)
            .to_dict()
        )

        return {
            "total_points": int(len(forecast_df)),
            "total_anomalies": int(len(anomalies_df)),
            "repos_with_anomalies": len(anomalies_by_repo),
            "anomalies_by_repo": anomalies_by_repo,
        }


class RepoActivityAnalyzer:

    def __init__(self, database_url: str, n_workers: int | None = None,
                 window_size: int = 7, z_threshold: float = 3.0):
        self.database_url = database_url
        self.n_workers = n_workers

        uow = UnitOfWork(database_url)
        uow.create_activity_tables()

        data_repo = ActivityRepository(database_url)
        self.core = RepoActivityCore(data_repo, window_size=window_size, z_threshold=z_threshold)

    def analyze(self) -> Dict[str, Any]:
        return self.core.run_analysis()
