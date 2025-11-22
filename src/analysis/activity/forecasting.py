import os
from typing import Dict, Any

import numpy as np
import pandas as pd
from statsmodels.tsa.holtwinters import ExponentialSmoothing

from src.analysis.activity.repo.repo import ActivityRepository
from src.storage.unit_of_work import UnitOfWork


class HoltWintersActivityCore:
    """
    Forecast daily commit activity per repo using Holt–Winters
    with a weekly seasonal component. If a repo has too little data,
    or Holt–Winters fails, we fall back to a simple rolling mean model.
    """

    def __init__(self, repo: ActivityRepository,
                 seasonal_periods: int = 7,
                 z_threshold: float = 3.0):
        self.repo = repo
        self.seasonal_periods = seasonal_periods
        self.z_threshold = z_threshold

    def _fallback_predict(self, series: pd.Series) -> pd.Series:
        """
        Simple fallback model: rolling mean over last 7 days (shifted),
        with global mean as initial fill.
        """
        rolling_mean = (
            series.rolling(window=7, min_periods=1)
            .mean()
            .shift(1)
        )
        return rolling_mean.fillna(series.mean())

    def run(self) -> Dict[str, Any]:
        df = self.repo.load_daily_commit_data()
        if df.empty:
            print("[HoltWintersActivityCore] No daily activity data found.")
            return {"error": "No daily activity data"}

        df["activity_date"] = pd.to_datetime(df["activity_date"])

        results = []

        for repo_id, g in df.groupby("repo_id"):
            g = g.sort_values("activity_date")

            # fill missing days with zeros
            g = g.set_index("activity_date").asfreq("D", fill_value=0)
            g["repo_id"] = repo_id

            series = g["commit_count"].astype(float)

            use_hw = series.notna().sum() >= 2 * self.seasonal_periods

            if use_hw:
                try:
                    model = ExponentialSmoothing(
                        series,
                        trend="add",
                        seasonal="add",
                        seasonal_periods=self.seasonal_periods,
                    ).fit()
                    predicted = model.fittedvalues
                except Exception as e:
                    print(f"[HoltWinters] Repo {repo_id}: HW failed ({e}), using fallback model.")
                    predicted = self._fallback_predict(series)
            else:
                # Not enough data for HW → fallback
                print(f"[HoltWinters] Repo {repo_id}: not enough data for HW "
                      f"({len(series)} points), using fallback model.")
                predicted = self._fallback_predict(series)

            g["actual"] = series
            g["predicted"] = predicted
            g["residual"] = g["actual"] - g["predicted"]

            # rolling std on residuals for anomaly detection
            std = g["residual"].rolling(7, min_periods=2).std()
            z = g["residual"] / std.replace(0, np.nan)
            g["z_score"] = z
            g["is_anomaly"] = z.abs() > self.z_threshold

            results.append(g.reset_index())

        result_df = pd.concat(results, ignore_index=True)

        # save artifacts to CSV
        os.makedirs("artifacts", exist_ok=True)
        result_df.to_csv("artifacts/holt_winters_activity.csv", index=False)

        # save to DB
        self.repo.save_forecasts(result_df)

        anomaly_count = int(result_df["is_anomaly"].sum())
        repos_with_anomalies = int(result_df[result_df["is_anomaly"]].repo_id.nunique())

        print("\nHOLT–WINTERS ACTIVITY FORECAST")
        print("======================================")
        print(f"Seasonal period: {self.seasonal_periods}")
        print(f"z-threshold: {self.z_threshold}")
        print(f"Total points: {len(result_df)}")
        print(f"Total anomalies: {anomaly_count}")
        print(f"Repos with anomalies: {repos_with_anomalies}")

        return {
            "total_points": int(len(result_df)),
            "total_anomalies": anomaly_count,
            "repos_with_anomalies": repos_with_anomalies,
        }

class HoltWintersActivityAnalyzer:

    def __init__(self, database_url: str,
                 seasonal_periods: int = 7,
                 z_threshold: float = 3.0):
        self.repo = ActivityRepository(database_url)
        self.core = HoltWintersActivityCore(
            self.repo,
            seasonal_periods=seasonal_periods,
            z_threshold=z_threshold
        )

        uow = UnitOfWork(database_url)
        uow.create_activity_tables()

    def analyze(self):
        return self.core.run()
