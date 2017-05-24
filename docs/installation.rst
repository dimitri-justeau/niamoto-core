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

In order to clone the Niamoto repository, Git must be install in your system:

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
