.. _installation:

Installation
============

-------------
Prerequisites
-------------

Operating System
----------------

Niamoto had been developed and tested to run under **Linux** systems. However,
it had been developed with open-source and cross-platform technologies and it
should then be possible to set up a Niamoto instance on **Mac** or
**Windows**. If you succeeded to do so, feel free to contribute to this doc
(see :ref:`contributing`).


Installing and setting up PostgreSQL / PostGIS
----------------------------------------------

Niamoto uses PostgreSQL (>= 9.5) with its spatial extension PostGIS (>= 2.1)
as a backend for constituting the data warehouse and storing the data. It
implies that your Niamoto instance must be granted access to a
PostgreSQL / PostGIS instance, whether it is distant or on the same system.

1. Install PostgreSQL / PostGIS
...............................

.. important::
    PostgreSQL version must be **at least** 9.5, since Niamoto uses JSONB
    capabilities that had been released with PostgreSQL 9.5.


The installation procedure can change according to your system. Please refer
to https://www.postgresql.org/download/ and http://postgis.net/install/ .


2. Create Database and Database User
....................................

First, change the current Linux user to ``postgres``:

.. code-block:: bash

    sudo su postgres

Then, log into PostgreSQL:

.. code-block:: bash

    psql

Create the Niamoto database (default name is ``niamoto``, see
:ref:`contributing` for more details):

.. code-block:: sql

    CREATE DATABASE niamoto;

Then, create the Niamoto user and grant full access to Niamoto database to it
(to ensure a secure instance, you must change at least the default user
password see :ref:`contributing` for more details):

.. code-block:: sql

    CREATE USER niamoto WITH PASSWORD niamoto;
    GRANT ALL PRIVILEGES ON DATABASE niamoto TO niamoto;

Finally, logout with ``\q``.

3. Create PostGIS extension and niamoto schema
..............................................

Log into PostgreSQL, with ``postgres`` user and ``niamoto`` database:

.. code-block:: bash

    psql -d niamoto

Create the PostGIS extension:

.. code-block:: sql

    CREATE EXTENSION POSTGIS;

Logout with ``\q``.


4. Create Database Schema
.........................

Log into PostgreSQL, with ``niamoto`` user and ``niamoto`` database:

.. code-block:: bash

    psql -U niamoto -d niamoto

Create the ``niamoto`` schema (see :ref:`contributing` for more details
and options):

.. code-block:: sql

    CREATE SCHEMA niamoto;

Logout with ``\q``.


Installing gdal-bin and libgdal
-------------------------------

Niamoto dependencies require that ``libgdal`` and ``gdal-bin`` are installed in
your system. The installation is straightforward:

.. code-block:: bash

    sudo apt-get install -y libgdal-dev gdal-bin
