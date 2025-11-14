# src/analysis/correlation/correlation.py
import os
from typing import Dict, Any

import pandas as pd

from src.analysis.correlation.repo.repo import CorrelationRepository
from src.storage.unit_of_work import UnitOfWork


class CommitCorrelationCore:
    """Чистая логика корреляции."""

    def __init__(self, data_repo: CorrelationRepository):
        self.data_repo = data_repo

    def run_analysis(self) -> Dict[str, Any]:
        df = self.data_repo.load_weekly_commit_data()

        if df.empty:
            return {"error": "No weekly commit data"}

        # Добавляем номер недели
        df["week_start"] = pd.to_datetime(df["week_start"])
        iso = df["week_start"].dt.isocalendar()
        df["week_number"] = iso.week.astype(int)

        global_corr = float(df["week_number"].corr(df["commit_count"]))

        def repo_corr(group: pd.DataFrame):
            if group.shape[0] < 2:
                return None
            return group["week_number"].corr(group["commit_count"])

        corr_by_repo = (
            df.groupby("repo_id")
            .apply(repo_corr)
            .dropna()
            .rename("correlation")
            .sort_values(ascending=False)
        )

        # Сохраняем в артефакты
        os.makedirs("artifacts", exist_ok=True)
        df.to_csv("artifacts/weekly_commits_with_week.csv", index=False)
        corr_by_repo.to_csv("artifacts/repo_week_commit_correlation.csv", header=True)

        print("\nCOMMIT WEEKLY CORRELATION")
        print("========================")
        print(f"Global corr(week_number, commit_count) = {global_corr:.4f}")

        print("\nTop repos with POSITIVE correlation:")
        print(corr_by_repo.head(10))

        print("\nTop repos with NEGATIVE correlation:")
        print(corr_by_repo.tail(10))

        self.data_repo.save_correlation_results(global_corr, corr_by_repo, df)

        return {
            "global_correlation": global_corr,
            "repo_correlation_count": int(len(corr_by_repo)),
        }


class CommitCorrelationAnalyzer:

    def __init__(self, database_url: str, n_workers: int | None = None):
        self.database_url = database_url
        self.n_workers = n_workers

        # Создаем таблицы корреляции
        uow = UnitOfWork(database_url)
        uow.create_correlation_tables()

        data_repo = CorrelationRepository(database_url)
        self.core = CommitCorrelationCore(data_repo)

    def analyze(self) -> Dict[str, Any]:
        return self.core.run_analysis()
