#!/usr/bin/env python3
import argparse
import os
import sys


def setup_imports():
    src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)


def main():
    setup_imports()

    from src.analysis import RepositoryAnalyzer

    parser = argparse.ArgumentParser(
        description='Analyze GitHub repositories using MapReduce text clustering'
    )

    parser.add_argument(
        '--database-url',
        default='postgresql://postgres:password@localhost/github_analysis',
        help='PostgreSQL database URL'
    )

    parser.add_argument(
        '--clusters',
        type=int,
        default=3,
        help='Number of clusters (default: 3)'
    )

    parser.add_argument(
        '--workers',
        type=int,
        help='Number of parallel workers (default: CPU count)'
    )

    parser.add_argument(
        '--chunk-size',
        type=int,
        default=1000,
        help='Chunk size for processing (default: 1000)'
    )

    args = parser.parse_args()

    try:
        print("Starting MapReduce Repository Analysis")
        print("=" * 40)

        analyzer = RepositoryAnalyzer(args.database_url, args.workers)
        results = analyzer.analyze(n_clusters=args.clusters)

        if 'error' in results:
            print(f"Error: {results['error']}")
            sys.exit(1)

        print("Analysis completed successfully. Results saved to database.")

    except Exception as e:
        print(f"Analysis failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()