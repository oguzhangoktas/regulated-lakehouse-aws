import pytest

from dataplatform.lakehouse.session import local_session


@pytest.fixture(scope="session")
def spark():
    session = local_session("tests")
    yield session
    session.stop()
