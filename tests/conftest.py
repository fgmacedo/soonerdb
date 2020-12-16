import pytest


@pytest.fixture
def db(tmpdir):
    from soonerdb import SoonerDB
    return SoonerDB(tmpdir)


@pytest.fixture
def SoonerDB(tmpdir):
    from soonerdb import SoonerDB
    return SoonerDB

