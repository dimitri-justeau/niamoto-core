.. _tutorial:

Tutorial
========

Setting the taxonomy
--------------------

Niamoto does not make any assumption on the taxonomic referential to be used,
and therefore let you define it. Since the choice of a taxonomic referential
should not be a blocking decision either a point of no return, Niamoto always
store two taxon identifier values for an occurrence: the data provider's one,
and it's correspondence in the Niamoto's referential. In order to be able to
map a provider's taxon identifier with an internal taxon identifier, Niamoto
needs a set of correspondences for each taxon. Those correspondences are called
synonyms. When no synonym is known for a provider's taxon identifier, the
Niamoto taxon identifier will be set null.

The definition of the Niamoto referential is done using a csv file. This csv
file must have a header defining at least the following columns:

- **id**: The unique identifier of the taxon, in the provider's referential.
- **parent_id**: The parent's id of the taxon. If the taxon is a root, let the value blank.
- **rank**: The rank of the taxon, can be a value among: 'REGNUM', 'PHYLUM', 'CLASSIS', 'ORDO', 'FAMILIA', 'GENUS', 'SPECIES', 'INFRASPECIES'.
- **full_name**: The full name of the taxon.
- **rank_name**: The rank name of the taxon.

All the additional columns will be considered as synonyms.

Let's consider the following example:

==== ========= ======= ========= ========= ==== ======
id   parent_id rank    full_name rank_name gbif taxref
==== ========= ======= ========= ========= ==== ======
0              FAMILIA A         A         10   1
1    0         GENUS   A a       a         20   2
2    1         SPECIES A a 1     1         30   3
3    1         SPECIES A a 2     2         40   4
==== ========= ======= ========= ========= ==== ======

We set this table as the Niamoto's taxonomic referential using the
``set_taxonomy`` command:

.. code-block:: shell-session

    $ niamoto set_taxonomy taxonomy.csv
    Setting the taxonomy...
    The taxonomy had been successfully set!
        4 taxa inserted
        2 synonyms inserted: {'taxref', 'gbif'}

We can see that Niamoto found the two following synonym keys: **'taxref'** and
**'gbif'**. Those keys are the one that we will use later to tell Niamoto how
to map the data provider's taxon identifier. Note that there is also a special
synonym key, **'niamoto'**, that is used when a data provider uses the same
taxon identifiers as Niamoto.


Managing data providers
-----------------------

Now that we have set the taxonomic referential, we would like to import some
data within Niamoto. But before being able to do so, we need to define data
providers.

Using the command ``niamoto providers``, we can see that there are not
registered providers in the database:

.. code-block:: shell-session

    $ niamoto providers
    There are no registered data providers in the database.

The simplest data provider type implemented in Niamoto is the csv data
provider, which enables us to import occurrence and plot data from plain csv
files. All the available provider_types can be obtained using the
``niamoto provider_types`` command.

Adding a data provider can achieved using the ``niamoto add_provider`` command,
which have the following usage:

.. code-block:: shell-session

    $ niamoto add_provider --help
    Usage: niamoto add_provider [OPTIONS] NAME PROVIDER_TYPE [SYNONYM_KEY]

      Register a data provider. The name of the data provider must be unique.
      The available provider types can be obtained using the 'niamoto
      provider_types' command. The available synonym keys can be obtained using
      the 'niamoto synonym_keys" command.

    Options:
      --help  Show this message and exit.

Let's add three data providers: **csv_niamoto**, **csv_taxref** and
**csv_gbif**:

.. code-block:: shell-session

    $ niamoto add_provider csv_niamoto CSV niamoto
      Registering the data provider in database...
      The data provider had been successfully registered to Niamoto!

.. code-block:: shell-session

    $ niamoto add_provider csv_taxref CSV taxref
      Registering the data provider in database...
      The data provider had been successfully registered to Niamoto!

.. code-block:: shell-session

    $ niamoto add_provider csv_gbif CSV gbif
      Registering the data provider in database...
      The data provider had been successfully registered to Niamoto!

They are now available with the ``niamoto providers`` command:

.. code-block:: shell-session

    $ niamoto providers
               name provider_type synonym_key
    id
    2   csv_niamoto           CSV     niamoto
    3    csv_taxref           CSV      taxref
    4      csv_gbif           CSV        gbif

In the next section, we will see how to import data with these data providers.


Importing occurrence, plot and plot/occurrence data
---------------------------------------------------

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

In this tutorial, we will import occurrence data for the three previously
registered data providers. We will also import plot and plot/occurrence data,
only for the first provider.

1. Importing occurrence data
............................

The occurrences csv file must have a header and contain at least the
following columns:

- **id**: The provider's unique identifier for the occurrence.
- **taxon_id**: The provider's taxon id for the occurrence.
- **x**: The longitude of the occurrence (WGS84).
- **y**: The latitude of the occurrence (WGS84).

All the remaining column will be stored as properties.

For the ``csv_niamoto`` provider, let's consider the following dataset:

== ======== ========= ========= ======= ======
id taxon_id x         y         dbh     height
== ======== ========= ========= ======= ======
0  3        165.321   -21.47    21      18
1  2        165.321   -21.47    20.5    14
2  2        165.321   -21.47    22.5    16
3  3        165.125   -21.54    18      12
4  3        165.125   -21.54    19      18
5  2        162.001   -18.11    11      15
6  2        162.001   -18.11    24      20
7  2        162.001   -18.11    25      22
== ======== ========= ========= ======= ======

For the ``csv_taxref`` provider, let's consider the following dataset:

== ======== ========= ========= =======
id taxon_id x         y         status
== ======== ========= ========= =======
0  4        92.321    42.40     alive
1  4        91.224    41.56     alive
2  4        91.015    41.11     dead
3  4        92.221    42.10     alive
4  4        92.221    42.10     dead
5  4        92.221    42.10     alive
6  4        92.221    42.10     alive
== ======== ========= ========= =======

For the ``csv_gbif`` provider, let's consider the following dataset:

== ======== ========= =========
id taxon_id x         y
== ======== ========= =========
0  20       11.921    11.47
1  30       16.120    21.54
2  30       61.045    18.12
3  20       16.001    8.11
== ======== ========= =========

Now let's import the data:

.. code-block:: shell-session

    $ niamoto sync csv_niamoto csv_niamoto_occurrences.csv 0 0
    Syncing the Niamoto database with 'csv_niamoto'...
    [INFO] *** Data sync starting ('csv_niamoto' - CSV)...
    [INFO] ** Occurrence sync starting ('csv_niamoto' - CSV)...
    [INFO] ** Occurrence sync with 'csv_niamoto' done (0.08 s)!
    [INFO] *** Data sync with 'csv_niamoto' done (total time: 0.08 s)!
    The Niamoto database had been successfully synced with 'csv_niamoto'!
    Bellow is a summary of what had been done:
        Occurrences:
            8 inserted
            0 updated
            0 deleted

.. code-block:: shell-session

    $ niamoto sync csv_taxref csv_niamoto_taxref_occurrences.csv 0 0
    Syncing the Niamoto database with 'csv_taxref'...
    [INFO] *** Data sync starting ('csv_taxref' - CSV)...
    [INFO] ** Occurrence sync starting ('csv_taxref' - CSV)...
    [INFO] ** Occurrence sync with 'csv_taxref' done (0.08 s)!
    [INFO] *** Data sync with 'csv_taxref' done (total time: 0.08 s)!
    The Niamoto database had been successfully synced with 'csv_taxref'!
    Bellow is a summary of what had been done:
        Occurrences:
            7 inserted
            0 updated
            0 deleted

.. code-block:: shell-session

    $ niamoto sync csv_gbif csv_niamoto_gbif_occurrences.csv 0 0
    Syncing the Niamoto database with 'csv_gbif'...
    [INFO] *** Data sync starting ('csv_gbif' - CSV)...
    [INFO] ** Occurrence sync starting ('csv_gbif' - CSV)...
    [INFO] ** Occurrence sync with 'csv_gbif' done (0.08 s)!
    [INFO] *** Data sync with 'csv_gbif' done (total time: 0.08 s)!
    The Niamoto database had been successfully synced with 'csv_gbif'!
    Bellow is a summary of what had been done:
        Occurrences:
            4 inserted
            0 updated
            0 deleted

We now have 19 occurrences coming from 3 data providers in our Niamoto
database, as we can see using the following command:

.. code-block:: shell-session

    $ niamoto status
        3 data providers are registered.
        4 taxa are stored.
        3 taxon synonym keys are registered.
        19 occurrences are stored.
        0 plots are stored.
        0 plots/occurrences are stored.
        0 rasters are stored.
        0 vectors are stored.


2. Importing plot data
......................

The plot csv file must have a header and contain at least the following
columns:

- **id**: The provider's identifier for the plot.
- **name**: The name of the plot.
- **x**: The longitude of the plot (WGS84).
- **y**: The latitude of the plot (WGS84).

All the remaining column will be stored as properties.

Let's consider the following dataset for the ``csv_niamoto`` provider:

== ======== ========= ========= ======= ======
id name     x         y         width   height
== ======== ========= ========= ======= ======
0  plot_1   165.321   -21.47    100     100
1  plot_2   165.125   -21.54    100     100
2  plot_3   162.001   -18.11    100     100
== ======== ========= ========= ======= ======

We import the plot data using the following command:

.. code-block:: shell-session

    $ niamoto sync csv_niamoto 0 csv_niamoto_plots.csv 0
        Syncing the Niamoto database with 'csv_niamoto'...
        [INFO] *** Data sync starting ('csv_niamoto' - CSV)...
        [INFO] ** Plot sync starting ('csv_niamoto' - CSV)...
        [INFO] ** Plot sync with 'csv_niamoto' done (0.06 s)!
        [INFO] *** Data sync with 'csv_niamoto' done (total time: 0.07 s)!
        The Niamoto database had been successfully synced with 'csv_niamoto'!
        Bellow is a summary of what had been done:
            Plots:
                3 inserted
                0 updated
                0 deleted


3. Importing plot/occurrence data
.................................

The plot/occurrence data is a many to many relationship between occurrences and
plots. A plot can contains several occurrences and an occurrence can be
contained by several plots. The plot/occurrence csv file must have a header and
contain at least the following columns:

- **plot_id**: The provider's id for the plot.
- **occurrence_id**: The provider's id for the occurrence.
- **occurrence_identifier**: The occurrence identifier in the plot.

The additional columns will be ignored.

Let's consider the following data, for linking ``csv_niamoto``'s occurrences
with ``csv_niamoto``'s plots:

======= ============= =====================
plot_id occurrence_id occurrence_identifier
======= ============= =====================
0       0             PLOT_1__OCC_1
0       1             PLOT_1__OCC_2
0       2             PLOT_1__OCC_3
1       3             PLOT_2__OCC_1
1       4             PLOT_2__OCC_2
2       5             PLOT_3__OCC_1
2       6             PLOT_3__OCC_2
2       7             PLOT_3__OCC_3
======= ============= =====================

We import the plot/occurrence data using the following command:

.. code-block:: shell-session

    $ niamoto sync csv_niamoto 0 0 csv_niamoto_plots_occurrences.csv
    Syncing the Niamoto database with 'csv_niamoto'...
    [INFO] *** Data sync starting ('csv_niamoto' - CSV)...
    [INFO] ** Plot-occurrence sync starting ('csv_niamoto' - CSV)...
    [INFO] ** Plot-occurrence sync with 'csv_niamoto' done (0.05 s)!
    [INFO] *** Data sync with 'csv_niamoto' done (total time: 0.06 s)!
    The Niamoto database had been successfully synced with 'csv_niamoto'!
    Bellow is a summary of what had been done:
        Plots / Occurrences:
            8 inserted
            0 updated
            0 deleted

We can check the Niamoto database status with the ``niamoto status`` command:

.. code-block:: shell-session

    $ niamoto status
        3 data providers are registered.
        4 taxa are stored.
        3 taxon synonym keys are registered.
        19 occurrences are stored.
        3 plots are stored.
        8 plots/occurrences are stored.
        0 rasters are stored.
        0 vectors are stored.


Importing rasters
-----------------

Niamoto provides functionalities to import and manage raster within the Niamoto
database, these functionalities rely on the PostGIS raster functionalities.
The main advantage of storing rasters inside a PostGIS database is to benefit
from the power of the SQL language, and the PostGIS spatial functions. It is
also a convenient way for having all the data stored at the same place and for
using the same system for querying.

Importing a raster in Niamoto is straightforward using the
``niamoto add_raster`` command:

.. code-block:: shell-session

    $ niamoto add_raster --help
    Usage: niamoto add_raster [OPTIONS] NAME RASTER_FILE_PATH

      Add a raster in Niamoto's raster database.

    Options:
      --srid TEXT                SRID of the raster. If not specified, it will be
                                 detected automatically.
      -t, --tile_dimension TEXT  Tile dimension <width>x<height>
      --help                     Show this message and exit.


Now let's import a rainfall raster in our Niamoto database:

.. code-block:: shell-session

    $ niamoto add_raster rainfall rainfall.tif
    Registering the raster in database...
    The raster had been successfully registered to the Niamoto raster database!

We can see the registered rasters with the ``niamoto rasters`` command:

.. code-block:: shell-session

    $ niamoto rasters
              tile_width  tile_height  srid date_create date_update
    name
    rainfall         100          100  4326  2017/06/08  2017/06/08



Importing vectors
-----------------

(Available soon)


Extracting raster values to occurrences and plot properties
-----------------------------------------------------------

Niamoto provides utilities for extracting raster values directly into
occurrences or plots properties.

.. code-block:: shell-session

    $ niamoto raster_to_occurrences --help
    Usage: niamoto raster_to_occurrences [OPTIONS] RASTER_NAME

      Extract raster values to occurrences properties.

    Options:
      --help  Show this message and exit


.. code-block:: shell-session

    $ niamoto raster_to_plots --help
    Usage: niamoto raster_to_plots [OPTIONS] RASTER_NAME

      Extract raster values to plots properties.

    Options:
      --help  Show this message and exit.


.. code-block:: shell-session

    $ niamoto all_rasters_to_occurrences --help
    Usage: niamoto all_rasters_to_occurrences [OPTIONS]

      Extract raster values to occurrences properties for all registered
      rasters.

    Options:
      --help  Show this message and exit.


.. code-block:: shell-session

    $ niamoto all_rasters_to_plots --help
    Usage: niamoto all_rasters_to_plots [OPTIONS]

      Extract raster values to plots properties for all registered rasters.

    Options:
      --help  Show this message and exit.

For instance, let's extract the values of the previously registered raster,
``rainfall`` to the occurrences properties:

.. code-block:: shell-session

    $ niamoto raster_to_occurrences rainfall
    Extracting 'rainfall' raster values to occurrences...
    The raster values had been successfully extracted!


Extracting vector values into occurrences properties
----------------------------------------------------

(Available soon)


Processing and publishing data
------------------------------

(Available soon)
