#!/usr/bin/env python3
import argparse
import os
import sys
import time

from data.github.github_collector import GitHubDatasetCollector


def setup_imports():
    """Setup proper imports"""
    src_dir = os.path.dirname(__file__)
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)


def main():
    """Main entry point"""
    setup_imports()

    parser = argparse.ArgumentParser(
        description='GitHub Dataset Tool - Collect GitHub repository data using multiprocessing',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic collection
  python main.py --token ghp_yourtoken123 --repos 10 --workers 4
        """
    )

    parser.add_argument(
        '--token',
        help='GitHub API token (or use GITHUB_TOKEN env variable)',
        default=os.getenv('GITHUB_TOKEN')
    )

    parser.add_argument(
        '--repos',
        type=int,
        default=50,
        help='Maximum repositories to collect (default: 50)'
    )

    parser.add_argument(
        '--workers', '-p',
        type=int,
        default=10,
        help='Number of workers'
    )

    parser.add_argument(
        '--database-url',
        help='PostgreSQL database URL',
        default='postgresql://postgres:password@localhost/github_analysis'
    )

    args = parser.parse_args()

    try:
        run_collection(args)

    except KeyboardInterrupt:
        print("\nOperation interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


def run_collection(args):
    """Run multiprocessing collection"""
    if not args.token:
        print("Error: GitHub token is required for data collection.")
        print("Use --token argument or set GITHUB_TOKEN environment variable.")
        print("You can get a token from: https://github.com/settings/tokens")
        sys.exit(1)

    print("Starting GitHub Data Collection (Multiprocessing)")
    print("=" * 50)
    print(f"Repositories to collect: {args.repos}")
    print(f"Workers: {args.workers or 'CPU count'}")
    print()

    collector = GitHubDatasetCollector(
        token=args.token,
        max_workers=args.workers,
        max_repos=args.repos,
        database_url=args.database_url
    )


    try:
        results = collector.collect_repos()

        print("=" * 50)
        print("Collection Completed!")
        print(f"Processed {len(results)} repositories successfully")

    except Exception as e:
        print("=" * 50)
        print("Collection Failed!")
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()