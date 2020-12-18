========
soonerdb
========


.. image:: https://img.shields.io/pypi/v/soonerdb.svg
        :target: https://pypi.python.org/pypi/soonerdb

.. image:: https://img.shields.io/travis/fgmacedo/soonerdb.svg
        :target: https://travis-ci.com/fgmacedo/soonerdb-python

.. image:: https://readthedocs.org/projects/soonerdb/badge/?version=latest
        :target: https://soonerdb.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status




A LSM-Tree key/value datastore in Python.


* Free software: MIT license
* Documentation: https://soonerdb.readthedocs.io.

This project started as a learning tool when studing the excelent book
"Designing data-intensive applications" by Martin Kleppmann.

.. note::

    This project still in development and is not yet tested on production environments.
    Use at your own risk.

Features
--------

- Pure Python fast LSM-Tree based key/value database.
- Embedded and zero-conf.
- Support in-order traversal of all stored keys.
- On-disk database persistence.
- Data is durable in the face of application or power failure.
- Thread-safe.
- Python 3.5+.


📝 TODO List
-------------
- [ ] Deletion of keys.
- [ ] Background merge of segment files (segment files are merged at load time).


Installation
------------

The project is hosted at https://github.com/fgmacedo/soonerdb-python and can be installed from source:

.. code-block:: console

    git clone https://github.com/fgmacedo/soonerdb-python
    cd soonerdb-python
    python setup.py install

