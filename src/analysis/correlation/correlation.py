#!/usr/bin/env python3
"""
Correlation analysis WITHOUT Spark:

- Корреляция между неделей и частотой коммитов
- Корреляция между неделей и временем от ПР до коммита

Usage from main.py:

    from src.analysis.correlation.correlation import CommitCorrelationAnalyzer

    analyzer = CommitCorrelationAnalyzer(database_url, workers)
    results = analyzer.analyze()
"""

from typing import Dict, Any
import numpy as np
import pandas as pd

from src.analysis.correlation.repo.repo import CorrelationRepository
from src.storage.unit_of_work import UnitOfWork


class CommitCorrelationCore:
    def __init__(self, repo: CorrelationRepository):
        self.repo = repo

    # ---------- helpers ----------

    @staticmethod
    def _corr_safe(a: pd.Series, b: pd.Series) -> float | None:
        """
        Safe Pearson correlation: returns None if not enough data.
        """
        if len(a) < 2 or len(b) < 2:
            return None
        try:
            return float(a.corr(b))
        except Exception:
            return None

    # ---------- 1) WEEK vs COMMIT FREQUENCY ----------

    def _commit_frequency_correlation(self, commits: pd.DataFrame) -> Dict[str, Any]:
        if commits.empty:
            print("[CommitCorrelationCore] No commit data found.")
            return {
                "commit_corr_week_of_year": None,
                "commit_corr_week_index": None,
            }

        commits["commit_date"] = pd.to_datetime(commits["commit_date"])
        iso = commits["commit_date"].dt.isocalendar()
        commits["year"] = iso.year
        commits["week_of_year"] = iso.week

        weekly = (
            commits.groupby(["year", "week_of_year"], as_index=False)
            .size()
            .rename(columns={"size": "commit_count"})
            .sort_values(["year", "week_of_year"])
        )

        weekly["week_index"] = np.arange(1, len(weekly) + 1)

        corr_week_of_year = self._corr_safe(
            weekly["week_of_year"], weekly["commit_count"]
        )
        corr_week_index = self._corr_safe(
            weekly["week_index"], weekly["commit_count"]
        )

        print("\n=== COMMIT FREQUENCY CORRELATION ===")
        print("corr(week_of_year, commit_count):      ", corr_week_of_year)
        print("corr(week_index,  commit_count):       ", corr_week_index)

        return {
            "commit_corr_week_of_year": corr_week_of_year,
            "commit_corr_week_index": corr_week_index,
        }

    # ---------- 2) WEEK vs PR -> COMMIT LEAD TIME ----------

    def _pr_to_commit_correlation(
        self, commits: pd.DataFrame, prs: pd.DataFrame
    ) -> Dict[str, Any]:
        if commits.empty or prs.empty:
            print("[CommitCorrelationCore] No data for PR->commit analysis.")
            return {
                "pr_corr_week_of_year": None,
                "pr_corr_week_index": None,
            }

        # Extract PR number from merge commit message
        # Message pattern: "Merge pull request #123 ..."
        commits["pr_number"] = (
            commits["message"]
            .str.extract(r"Merge pull request #(\d+)", expand=False)
            .astype("Int64")
        )

        merges = commits.dropna(subset=["pr_number"])

        if merges.empty:
            print("[CommitCorrelationCore] No merge commits with PR numbers.")
            return {
                "pr_corr_week_of_year": None,
                "pr_corr_week_index": None,
            }

        # Join on (repo_id, pr_number)
        merged = merges.merge(
            prs,
            how="inner",
            on=["repo_id", "pr_number"],
            suffixes=("_commit", "_pr"),
        )

        if merged.empty:
            print("[CommitCorrelationCore] No matching PRs and merge commits.")
            return {
                "pr_corr_week_of_year": None,
                "pr_corr_week_index": None,
            }

        merged["commit_date"] = pd.to_datetime(merged["commit_date"])
        merged["pr_created_at"] = pd.to_datetime(merged["pr_created_at"])

        merged["lead_time_hours"] = (
            merged["commit_date"] - merged["pr_created_at"]
        ).dt.total_seconds() / 3600.0

        merged = merged.dropna(subset=["lead_time_hours"])

        if merged.empty:
            print("[CommitCorrelationCore] No valid lead_time_hours.")
            return {
                "pr_corr_week_of_year": None,
                "pr_corr_week_index": None,
            }

        iso = merged["commit_date"].dt.isocalendar()
        merged["year"] = iso.year
        merged["week_of_year"] = iso.week

        weekly = (
            merged.groupby(["year", "week_of_year"], as_index=False)
            .agg(avg_lead_time_hours=("lead_time_hours", "mean"))
            .sort_values(["year", "week_of_year"])
        )

        weekly["week_index"] = np.arange(1, len(weekly) + 1)

        corr_week_of_year = self._corr_safe(
            weekly["week_of_year"], weekly["avg_lead_time_hours"]
        )
        corr_week_index = self._corr_safe(
            weekly["week_index"], weekly["avg_lead_time_hours"]
        )

        print("\n=== PR → COMMIT LEAD TIME CORRELATION ===")
        print("corr(week_of_year, avg_lead_time_hours):", corr_week_of_year)
        print("corr(week_index,   avg_lead_time_hours):", corr_week_index)

        return {
            "pr_corr_week_of_year": corr_week_of_year,
            "pr_corr_week_index": corr_week_index,
        }

    # ---------- PUBLIC API ----------

    def run_analysis(self) -> Dict[str, Any]:
        commits = self.repo.load_commits()
        prs = self.repo.load_pull_requests()

        commit_corr = self._commit_frequency_correlation(commits)
        pr_corr = self._pr_to_commit_correlation(commits, prs)

        result = {**commit_corr, **pr_corr}

        print("\n=== CORRELATION SUMMARY ===")
        print(result)

        # Save one global result row into DB
        self.repo.save_correlation_result(result)

        return result


class CommitCorrelationAnalyzer:
    """
    High-level class used by main.py:

        commit_corr_analyzer = CommitCorrelationAnalyzer(database_url, workers)
        corr_results = commit_corr_analyzer.analyze()
    """

    def __init__(self, database_url: str, n_workers: int | None = None):
        self.database_url = database_url
        self.n_workers = n_workers

        # create correlation tables (commit_correlation_result)
        uow = UnitOfWork(database_url)
        uow.create_correlation_tables()

        repo = CorrelationRepository(database_url)
        self.core = CommitCorrelationCore(repo)

    def analyze(self) -> Dict[str, Any]:
        return self.core.run_analysis()
