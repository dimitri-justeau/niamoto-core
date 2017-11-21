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

Python 3
--------

Niamoto is written in Python 3. It is tested with Python 3.4, 3.5 and 3.6.
You must have one of the following installed in your system, in addition to
``pip``.

PostgreSQL / PostGIS
--------------------

Niamoto uses PostgreSQL (>= 9.5) with its spatial extension PostGIS (>= 2.1)
as a backend for constituting the data warehouse and storing the data. It
implies that your Niamoto instance must be granted access to a
PostgreSQL / PostGIS instance, whether it is distant or on the same system.

.. important::
    PostgreSQL version must be **at least** 9.5, since Niamoto uses JSONB
    capabilities that had been released with PostgreSQL 9.5.


The installation procedure can change according to your system. Please refer
to https://www.postgresql.org/download/ and http://postgis.net/install/ .

gdal-bin and libgdal-dev
------------------------

Niamoto dependencies require that ``libgdal-dev`` and ``gdal-bin`` are installed in
your system. The installation is straightforward:

.. code-block:: bash

    sudo apt-get install -y libgdal-dev gdal-bin

Git
---

In order to clone the Niamoto repository, Git must be installed in your system:

.. code-block:: bash

    sudo apt-get install -y git


---------------
Install Niamoto
---------------

First, clone the Niamoto repository in your system:

.. code-block:: bash

    git clone https://github.com/dimitri-justeau/niamoto-core.git

Then, move into the project directory and install Niamoto using pip:

.. code-block:: bash

    pip install .


----------------------------------
Setting up the Niamoto environment
----------------------------------

Setting up the Niamoto database
-------------------------------

.. note::
    For more options with the Niamoto database, please refer to
    :ref:`configuration`.

1. Create Database and Database User
....................................

First, change the current Linux user to ``postgres``:

.. code-block:: bash

    sudo su postgres

Then, log into PostgreSQL:

.. code-block:: bash

    psql

Create the Niamoto database (default name is ``niamoto``, see
:ref:`configuration` for more details):

.. code-block:: sql

    CREATE DATABASE niamoto;

Then, create the Niamoto user and grant full access to Niamoto database to it
(to ensure a secure instance, you must change at least the default user
password see :ref:`configuration` for more details):

.. code-block:: sql

    CREATE USER niamoto WITH PASSWORD niamoto;
    GRANT ALL PRIVILEGES ON DATABASE niamoto TO niamoto;

Finally, logout with ``\q``.

2. Create PostGIS extension and niamoto schema
..............................................

Log into PostgreSQL, with ``postgres`` user and ``niamoto`` database:

.. code-block:: bash

    psql -d niamoto

Create the PostGIS extension:

.. code-block:: sql

    CREATE EXTENSION POSTGIS;

Logout with ``\q``.


3. Create Database Schemas
..........................

Log into PostgreSQL, with ``niamoto`` user and ``niamoto`` database:

.. code-block:: bash

    psql -U niamoto -d niamoto

Create the ``niamoto``, ``niamoto_raster``, ``niamoto_vector``, ``niamoto_dimensions``, ``niamoto_fact_tables`` schemas
(see :ref:`configuration` for more details
and options):

.. code-block:: sql

    CREATE SCHEMA niamoto;
    CREATE SCHEMA niamoto_raster;
    CREATE SCHEMA niamoto_vector;
    CREATE SCHEMA niamoto_dimensions;
    CREATE SCHEMA niamoto_fact_tables;

Logout with ``\q``.


Initializing the Niamoto home directory
---------------------------------------

.. note::
    For more options with the Niamoto home directory, please refer to
    :ref:`configuration`.

Niamoto home is the place where configuration files, scripts and plugins will
be stored. Niamoto comes with a handy command for initializing it:

.. code-block:: bash

    niamoto init_niamoto_home


Initializing the Niamoto database
---------------------------------

Initializing the Niamoto database means creating the tables, indexes, constraints and initializing basic data. The procedure is straightforward:

.. code-block:: bash

    niamoto init_db


What's next?
------------

At this point, you should have a working Niamoto environment. If you are ready
to play, you can go to the :ref:`quickstart` of the :ref:`tutorial`!
