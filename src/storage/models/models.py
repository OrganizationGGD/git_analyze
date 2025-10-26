from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import ARRAY as PG_ARRAY

Base = declarative_base()


class Repository(Base):
    __tablename__ = 'repositories'

    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    full_name = Column(String(255))
    description = Column(Text)
    language = Column(String(100))
    stargazers_count = Column(Integer)
    forks_count = Column(Integer)
    watchers_count = Column(Integer)
    open_issues_count = Column(Integer)
    size = Column(Integer)
    default_branch = Column(String(100))
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    pushed_at = Column(DateTime)
    homepage = Column(String(255))
    topics = Column(PG_ARRAY(String))
    owner_id = Column(Integer)
    owner_login = Column(String(255))
    owner_type = Column(String(50))
    is_fork = Column(Boolean)
    has_issues = Column(Boolean)
    has_projects = Column(Boolean)
    has_downloads = Column(Boolean)
    has_wiki = Column(Boolean)
    has_pages = Column(Boolean)
    archived = Column(Boolean)
    disabled = Column(Boolean)

    # Relationships
    contributors = relationship("RepositoryContributor", back_populates="repository")
    commits = relationship("Commit", back_populates="repository")


class Contributor(Base):
    __tablename__ = 'contributors'

    id = Column(Integer, primary_key=True)
    login = Column(String(255))
    type = Column(String(50))
    site_admin = Column(Boolean)
    name = Column(String(255))
    company = Column(String(255))
    blog = Column(String(255))
    location = Column(String(255))
    email = Column(String(255))
    hireable = Column(Boolean)
    bio = Column(Text)
    twitter_username = Column(String(255))
    public_repos = Column(Integer)
    public_gists = Column(Integer)
    followers = Column(Integer)
    following = Column(Integer)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)

    # Relationships
    repositories = relationship("RepositoryContributor", back_populates="contributor")
    commits = relationship("Commit", back_populates="author")


class Commit(Base):
    __tablename__ = 'commits'

    sha = Column(String(100), primary_key=True)
    repo_id = Column(Integer, ForeignKey('repositories.id'))
    author_id = Column(Integer, ForeignKey('contributors.id'))
    author_login = Column(String(255))
    committer_id = Column(Integer)
    committer_login = Column(String(255))
    message = Column(Text)
    author_date = Column(DateTime)
    commit_date = Column(DateTime)
    url = Column(String(255))
    comment_count = Column(Integer)
    verification_verified = Column(Boolean)
    verification_reason = Column(String(100))
    additions = Column(Integer)
    deletions = Column(Integer)
    total_changes = Column(Integer)
    files_changed = Column(PG_ARRAY(String))
    parent_shas = Column(PG_ARRAY(String))

    # Relationships
    repository = relationship("Repository", back_populates="commits")
    author = relationship("Contributor", back_populates="commits")


class RepositoryContributor(Base):
    __tablename__ = 'repository_contributors'

    repo_id = Column(Integer, ForeignKey('repositories.id'), primary_key=True)
    contributor_id = Column(Integer, ForeignKey('contributors.id'), primary_key=True)
    contributions = Column(Integer)

    # Relationships
    repository = relationship("Repository", back_populates="contributors")
    contributor = relationship("Contributor", back_populates="repositories")