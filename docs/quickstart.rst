.. _quickstart:

Quickstart
==========


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

Create the ``niamoto``, ``niamoto_raster``, ``niamoto_vector`` schemas
(see :ref:`configuration` for more details
and options):

.. code-block:: sql

    CREATE SCHEMA niamoto;
    CREATE SCHEMA niamoto_raster;
    CREATE SCHEMA niamoto_vector;

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

Initializing the Niamoto database means creating the tables,
populating the taxonomic referential, and registering the available data
provider types. The procedure is straightforward:

.. code-block:: bash

    niamoto init_db

.. note::
    For the moment, the taxonomic referential used corresponds
    to the New Caledonian trees. Since Niamoto is being developed within the
    New Caledonian rainsforests context, it is ok for us, but it might be
    problematic for someone trying to test Niamoto for another context. We
    plan to provide options for defining different referential for different
    contexts, but it is not our priority since, at our knowledge, no one else
    than us is using Niamoto. If you want to try Niamoto in your context and
    need those functionalities, let us know, it will be a sufficient reason
    for us to adjust our priorities.


What's next?
------------

At this point, you should have a working Niamoto environment. If you are ready
to play, you can go to the :ref:`tutorial`!

