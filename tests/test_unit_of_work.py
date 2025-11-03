from src.storage.unit_of_work import UnitOfWork
from unittest.mock import patch

def test_get_session_creates_session():
    with patch("src.storage.unit_of_work.create_engine") as engine_mock:
        engine_mock.return_value = "engine"
        uow = UnitOfWork("sqlite:///:memory:")
        session = uow.get_session()
        assert session is not None
