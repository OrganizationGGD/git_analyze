import multiprocessing as mp
from multiprocessing import Pool
import os

from .github_client import GitHubClient
from src.utils.utils import format_date, clean_message
from src.storage.repo.repo import RepositoryRepository, ContributorRepository, CommitRepository
from src.storage.unit_of_work import UnitOfWork


class GitHubDatasetCollector:
    def __init__(self, token=None, max_workers=None, max_repos=30, database_url=None):
        self.token = token or os.getenv('GITHUB_TOKEN')
        self.max_workers = max_workers or mp.cpu_count()
        self.max_repos = max_repos
        self.database_url = database_url

        if database_url:
            UnitOfWork(database_url).create_tables()

    def collect_repos(self):
        """Main method for data collection"""
        print(f"Starting MULTIPROCESSING data collection...")
        print(f"Processes: {self.max_workers}, Repositories: {self.max_repos}")

        repositories = self.__get_popular_repositories(self.max_repos)
        print(f"Found {len(repositories)} repositories")

        chunk_size = max(1, len(repositories) // self.max_workers)
        repo_chunks = [repositories[i:i + chunk_size] for i in range(0, len(repositories), chunk_size)]

        print(f"Split into {len(repo_chunks)} chunks for parallel processing")

        with Pool(processes=self.max_workers) as pool:
            tasks = []
            for i, chunk in enumerate(repo_chunks):
                task = {
                    'repositories': chunk,
                    'database_url': self.database_url,
                    'token': self.token,
                    'worker_id': i
                }
                tasks.append(task)

            results = pool.map(self._process_repository_chunk, tasks)

        total_repos = 0
        total_contributors = 0
        total_commits = 0

        for result in results:
            if result:
                total_repos += result['repos_count']
                total_contributors += result['contributors_count']
                total_commits += result['commits_count']

        print(f"FINISHED: {total_repos} repos, {total_contributors} contributors, {total_commits} commits")

        return {
            'total_repos': total_repos,
            'total_contributors': total_contributors,
            'total_commits': total_commits
        }

    @staticmethod
    def _process_repository_chunk(task):
        """Worker processes a chunk"""
        worker_id = task['worker_id']
        repositories = task['repositories']
        database_url = task['database_url']
        token = task['token']

        print(f"Worker {worker_id} started processing {len(repositories)} repositories")

        client = GitHubClient(token)

        repo_repo = RepositoryRepository(database_url)
        contrib_repo = ContributorRepository(database_url)
        commit_repo = CommitRepository(database_url)

        worker_repos_count = 0
        worker_contributors_count = 0
        worker_commits_count = 0

        for repo in repositories:
            try:
                print(f"Worker {worker_id}: Processing {repo['full_name']}")

                repo_repo.upsert_repository({
                    'id': repo['id'],
                    'name': repo['name'],
                    'full_name': repo['full_name'],
                    'description': repo.get('description'),
                    'language': repo.get('language'),
                    'stargazers_count': repo.get('stargazers_count', 0),
                    'forks_count': repo.get('forks_count', 0),
                    'watchers_count': repo.get('watchers_count', 0),
                    'open_issues_count': repo.get('open_issues_count', 0),
                    'size': repo.get('size', 0),
                    'default_branch': repo.get('default_branch'),
                    'created_at': format_date(repo.get('created_at')),
                    'updated_at': format_date(repo.get('updated_at')),
                    'pushed_at': format_date(repo.get('pushed_at')),
                    'homepage': repo.get('homepage'),
                    'topics': repo.get('topics', []),
                    'owner_id': repo['owner']['id'],
                    'owner_login': repo['owner']['login'],
                    'owner_type': repo['owner']['type'],
                    'is_fork': repo.get('fork', False),
                    'has_issues': repo.get('has_issues', False),
                    'has_projects': repo.get('has_projects', False),
                    'has_downloads': repo.get('has_downloads', False),
                    'has_wiki': repo.get('has_wiki', False),
                    'has_pages': repo.get('has_pages', False),
                    'archived': repo.get('archived', False),
                    'disabled': repo.get('disabled', False)
                })

                contributors = GitHubDatasetCollector._get_contributors(
                    client, repo['owner']['login'], repo['name']
                )

                print(f"Worker {worker_id}: Found {len(contributors)} contributors for {repo['full_name']}")

                for contributor in contributors:
                    result = GitHubDatasetCollector._process_contributor(
                        client, contributor, repo['id'],
                        repo['owner']['login'], repo['name'],
                        contrib_repo, commit_repo
                    )
                    if result:
                        worker_contributors_count += 1
                        worker_commits_count += result['commits_count']

                worker_repos_count += 1

            except Exception as e:
                print(f"Worker {worker_id} error processing {repo['full_name']}: {e}")
                continue

        print(f"Worker {worker_id} finished: {worker_repos_count} repos, {worker_contributors_count} contributors, {worker_commits_count} commits")

        return {
            'repos_count': worker_repos_count,
            'contributors_count': worker_contributors_count,
            'commits_count': worker_commits_count
        }

    @staticmethod
    def _process_contributor(client, contributor, repo_id, owner, repo_name, contrib_repo, commit_repo):
        """Worker for processing contributor"""
        username = contributor.get('login')
        if not username:
            return None

        if contrib_repo.exists(contributor.get('id')):
            print(f"Contributor {username} already exists, skipping...")
            return None

        user_info = client.get_user_info(username)
        if not user_info:
            return None

        contrib_repo.upsert_contributor(
            user_info,
            repo_id,
            contributor.get('contributions', 0)
        )

        commits = GitHubDatasetCollector._get_commits(client, owner, repo_name, username)

        if commits:
            for commit in commits:
                commit_repo.upsert_commit(commit, repo_id, user_info.get('id'))

        print(f"Saved {len(commits)} commits for {username}")

        return {
            'username': username,
            'commits_count': len(commits)
        }

    @staticmethod
    def _get_commits(client, owner, repo, username):
        """Get all commits"""
        commits = []
        page = 1

        while True:
            url = f"{client.base_url}/repos/{owner}/{repo}/commits"
            params = {
                "author": username,
                "page": page,
                "per_page": 1000
            }

            response = client.make_request(url, params=params)
            if not response or response.status_code != 200:
                break

            page_commits = response.json()
            if not page_commits:
                break

            for commit_data in page_commits:
                if isinstance(commit_data, dict):
                    commit = GitHubDatasetCollector._extract_commit_details(
                        commit_data, owner, repo, username
                    )
                    if commit:
                        commits.append(commit)

            if len(page_commits) < 100:
                break

            page += 1

        return commits

    @staticmethod
    def _get_contributors(client, owner, repo):
        """Worker for getting contributors"""
        contributors = []
        page = 1

        while True:
            url = f"{client.base_url}/repos/{owner}/{repo}/contributors"
            params = {
                "page": page,
                "per_page": 1000,
                "anon": "0"
            }

            response = client.make_request(url, params=params)
            if not response or response.status_code != 200:
                break

            page_contributors = response.json()
            if not page_contributors:
                break

            contributors.extend(page_contributors)

            if len(page_contributors) < 100:
                break

            page += 1
        return contributors

    @staticmethod
    def _extract_commit_details(commit_data, owner, repo, username):
        """Extract commit details from GitHub API response"""
        try:
            commit_info = commit_data.get('commit', {})
            author_info = commit_info.get('author', {})
            committer_info = commit_info.get('committer', {})
            stats = commit_data.get('stats', {})
            files = [f.get('filename', '') for f in commit_data.get('files', [])]

            verification = commit_info.get('verification', {})

            return {
                'sha': commit_data.get('sha', ''),
                'repo_owner': owner,
                'repo_name': repo,
                'author_login': username,
                'author_name': author_info.get('name', ''),
                'author_email': author_info.get('email', ''),
                'author_date': format_date(author_info.get('date', '')),
                'committer_name': committer_info.get('name', ''),
                'committer_email': committer_info.get('email', ''),
                'commit_date': format_date(committer_info.get('date', '')),
                'message': clean_message(commit_info.get('message', '')),
                'comment_count': commit_info.get('comment_count', 0),
                'verification_verified': verification.get('verified', False),
                'verification_reason': verification.get('reason', ''),
                'additions': stats.get('additions', 0),
                'deletions': stats.get('deletions', 0),
                'total_changes': stats.get('total', 0),
                'files_changed': files,
            }
        except Exception as e:
            print(f"Error extracting commit: {e}")
            return None

    def __get_popular_repositories(self, count=30):
        """Get popular repositories from GitHub"""
        repos = []
        page = 1
        per_page = 100

        client = GitHubClient(self.token)

        while len(repos) < count:
            url = f"{client.base_url}/search/repositories"
            params = {
                "q": "stars:>100",
                "sort": "stars",
                "order": "desc",
                "page": page,
                "per_page": per_page
            }

            response = client.make_request(url, params=params, is_search=True)
            if not response or response.status_code != 200:
                break

            data = response.json()
            if "items" not in data:
                break

            for repo in data["items"]:
                repos.append(repo)
                print(f"Added repository: {repo['full_name']} (stars: {repo.get('stargazers_count', 0)})")

                if len(repos) >= count:
                    break

            if len(data["items"]) < per_page:
                break

            page += 1

        return repos[:count]