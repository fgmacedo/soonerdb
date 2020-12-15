import pytest


@pytest.fixture
def db(tmpdir):
    from soonerdb import SoonerDB
    return SoonerDB(tmpdir)
