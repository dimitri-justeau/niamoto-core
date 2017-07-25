.. _quickstart:

Quickstart
==========

Setting the taxonomy
--------------------

The Niamoto's taxonomic referential is set using the
``set_taxonomy`` command:

.. code-block:: shell-session

    $ niamoto set_taxonomy taxonomy.csv
    Setting the taxonomy...
    The taxonomy had been successfully set!
        4 taxa inserted
        2 synonyms inserted: {'taxref', 'gbif'}


Adding data providers and importing data
----------------------------------------

Plots and occurrences data is imported registered data providers and syncing
with them.

.. code-block:: shell-session

    $ niamoto add_provider csv_gbif CSV gbif
      Registering the data provider in database...
      The data provider had been successfully registered to Niamoto!

It is possible to see the registered providers using the
``niamoto providers`` command:

.. code-block:: shell-session

    $ niamoto providers
               name provider_type synonym_key
    id
    1      csv_gbif           CSV        gbif


Importing data using the csv data provider is done with three csv files:

 - The **occurrences** csv file, containing the occurrence data.
 - The **plots** csv file, containing the plot data.
 - The **plots/occurrences** csv file, mapping plots with occurrences.

All of them are optional, you can import only occurrences, only plots or only
map existing plots with existing occurrences. The command for importing data
from a provider is ``niamoto sync PROVIDER_NAME [PROVIDER_ARGS]``. With the
csv data provider, three arguments are needed, corresponding to the csv files
paths:

.. code-block:: shell-session

    $ niamoto sync <csv_data_provider_name> <occurrences.csv> <plots.csv> <plots_occurrences.csv>

Using ``0`` instead of a path means that no data is to be imported. For
instance, importing only plot data can be achieved using:

.. code-block:: shell-session

    $ niamoto sync <csv_data_provider_name> 0 <plots.csv> 0

Now let's import some data:

.. code-block:: shell-session

    $ niamoto sync csv_gbif csv_niamoto_gbif_occurrences.csv csv_gbif_plots.csv csv_gbif_plots_occurrences.csv
    Syncing the Niamoto database with 'csv_gbif'...
    [INFO] *** Data sync starting ('csv_gbif' - CSV)...
    [INFO] ** Occurrence sync starting ('csv_gbif' - CSV)...
    [INFO] ** Occurrence sync with 'csv_gbif' done (0.08 s)!
    [INFO] ** Plot sync starting ('csv_gbif' - CSV)...
    [INFO] ** Plot sync with 'csv_gbif' done (0.06 s)!
    [INFO] *** Data sync with 'csv_gbif' done (total time: 0.08 s)!
    The Niamoto database had been successfully synced with 'csv_gbif'!
    Bellow is a summary of what had been done:
        Occurrences:
            432 inserted
            0 updated
            0 deleted
        Plots:
            34 inserted
            0 updated
            0 deleted
        Plots / Occurrences:
            432 inserted
            0 updated
            0 deleted

We can check the Niamoto database status with the ``niamoto status`` command:

.. code-block:: shell-session

    $ niamoto status
        1 data providers are registered.
        123 taxa are stored.
        3 taxon synonym keys are registered.
        432 occurrences are stored.
        34 plots are stored.
        432 plots/occurrences are stored.
        0 rasters are stored.
        0 vectors are stored.


Importing rasters and vectors
-----------------------------


Processing and publishing data
------------------------------

