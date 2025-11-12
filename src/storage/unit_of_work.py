from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class UnitOfWork:
    def __init__(self, database_url: str = None):

        self.engine = create_engine(
            database_url,
            pool_size=50,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=3600
        )
        self.session_factory = sessionmaker(bind=self.engine)

    def get_session(self):
        return self.session_factory()

    def create_analysis_tables(self):
        from src.analysis.models.models import Base
        Base.metadata.create_all(self.engine)

    def create_tables(self):
        from src.data.models.models import Base
        Base.metadata.create_all(self.engine)