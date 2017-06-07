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

Now that we set the taxonomic referential, we would like to import some data
in Niamoto. But before being able to do so, we need to define data providers.

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
whic have the following usage:

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
map existing plots with existing occurrences.

In this tutorial, we will import occurrence data for the three previously
registered data providers. We will also import plot and plot/occurrence data,
only for the first provider.

1. Importing occurrence data
............................

The occurrences csv file must have a header and contain at least the
following column:

- **id**: The provider's unique identifier for the occurrence.
- **taxon_id**: The provider's taxon id for the occurrence.
- **x**: The longitude of the occurrence (WGS84).
- **y**: The latitude of the occurrence (WGS84).

All the remaining column will be stored as properties.

For the ``csv_niamoto`` provider, let's consider the following dataset:

For the ``csv_taxref`` provider, let's consider the following dataset:

For the ``csv_gbif`` provider, let's consider the following dataset:


2. Importing plot data
......................

3. Importing plot/occurrence data
.................................


Managing rasters
----------------

(Available soon)


Managing vectors
----------------

(Available soon)


Extracting raster values into occurrences properties
----------------------------------------------------

(Available soon)


Extracting vector values into occurrences properties
----------------------------------------------------

(Available soon)


Processing and publishing data
------------------------------

(Available soon)
