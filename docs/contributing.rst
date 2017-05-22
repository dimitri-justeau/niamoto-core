.. _contributing:

Contributing
============

Setting up a developpment environment
-------------------------------------

Prerequesite: You need to have ``PostgreSQL`` and ``PostGIS`` installed and
running in your system, or have access to a distant instance of it. Check the
test settings in the ``tests/test_data/test_niamoto_home/settings.py`` file.

It is recommended to use ``virtualenv`` to setup a development environment with
python 3.4, 3.5 or 3.6. Please refer to https://virtualenv.pypa.io/en/stable/

First, clone the repository in your system using ``git``:

.. code-block:: bash

    git clone https://github.com/dimitri-justeau/niamoto-core.git

Move in the cloned repository and install the dependencies using ``pip``:

.. code-block:: bash

    pip install -r requirements.txt

Finally download the test data:

.. code-block:: bash

    sh tests/download_test_data.sh

You can check the tests using:

.. code-block:: bash

    python run_tests.py

