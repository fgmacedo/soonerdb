========
soonerdb
========


.. image:: https://img.shields.io/pypi/v/soonerdb.svg
        :target: https://pypi.python.org/pypi/soonerdb

.. image:: https://img.shields.io/travis/fgmacedo/soonerdb.svg
        :target: https://travis-ci.com/fgmacedo/soonerdb

.. image:: https://codecov.io/gh/fgmacedo/soonerdb/branch/main/graph/badge.svg
        :target: https://codecov.io/gh/fgmacedo/soonerdb
        :alt: Coverage report

.. image:: https://readthedocs.org/projects/soonerdb/badge/?version=latest
        :target: https://soonerdb.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status




A LSM-Tree key/value database in Python.


* Free software: MIT license
* Documentation: https://soonerdb.readthedocs.io.

This project started as a learning tool when studing the excelent book
"Designing data-intensive applications" by Martin Kleppmann.

.. note::

    This is a toy project and is not yet tested on production environments.
    Use at your own risk.

Features
--------

- Pure Python fast LSM-Tree based key/value database.
- Embedded and zero-conf.
- Support in-order traversal of all stored keys.
- On-disk database persistence.
- Data is durable in the face of application or power failure.
- Background merge of segment files.
- **Python 3.6+**.


📝 TODO List
-------------
- [ ] Deletion of keys.


Installation
------------

You can install using ``pip``:

.. code-block:: console

    pip install soonerdb

Or from source:

.. code-block:: console

    git clone https://github.com/fgmacedo/soonerdb
    cd soonerdb
    python setup.py install


Quick intro
-----------

``SoonerDB`` has a dict-like API.

Showtime:

.. code-block:: pycon

    In [1]: from soonerdb import SoonerDB

    In [2]: db = SoonerDB('./tmp')

    In [3]: db["my key"] = "A value"

    In [4]: db["my key"]
    Out[4]: 'A value'

    In [5]: "my key" in db
    Out[5]: True

    In [6]: "other key" in db
    Out[6]: False

    In [7]: db["other key"]
    ---------------------------------------------------------------------------
    KeyError                                  Traceback (most recent call last)
    <ipython-input-7-bc114493f395> in <module>
    ----> 1 db["other key"]
    KeyError: "Key 'other key' not found."

    In [8]: db.get("other key", "default value")
    Out[8]: 'default value'

    In [9]: db.set("another", "value")

    In [10]: list(db)
    Out[10]: [('another', 'value'), ('my key', 'A value')]
