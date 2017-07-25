.. _cli:

CLI reference
=============

General commands
----------------

init_niamoto_home
.................

.. code-block:: shell-session

    Usage: niamoto init_niamoto_home [OPTIONS]

      Initialize the Niamoto home directory.

    Options:
      --niamoto_home_path TEXT
      --help                    Show this message and exit.

init_db
.......

.. code-block:: shell-session

    Usage: niamoto init_db [OPTIONS]

      Initialize the Niamoto database.

    Options:
      --help  Show this message and exit.

status
......

.. code-block:: shell-session

    Usage: niamoto status [OPTIONS]

      Show the status of the Niamoto database.

    Options:
      --help  Show this message and exit.


Taxonomy commands
-----------------

set_taxonomy
............

.. code-block:: shell-session

    Usage: niamoto set_taxonomy [OPTIONS] CSV_FILE_PATH

      Set the taxonomy.

    Options:
      --no_mapping BOOLEAN
      --help                Show this message and exit.

map_all_synonyms
................

.. code-block:: shell-session

    Usage: niamoto map_all_synonyms [OPTIONS]

      Update the synonym mapping for every data provider registered in the
      database.

    Options:
      --help  Show this message and exit.

synonym_keys
............

.. code-block:: shell-session

    Usage: niamoto synonym_keys [OPTIONS]

      List the registered synonym keys.

    Options:
      --help  Show this message and exit.


Data providers commands
-----------------------

provider_types
..............

.. code-block:: shell-session

    Usage: niamoto provider_types [OPTIONS]

      List registered data provider types.

    Options:
      --help  Show this message and exit.

providers
.........

.. code-block:: shell-session

    Usage: niamoto providers [OPTIONS]

      List registered data providers.

    Options:

add_provider
............

.. code-block:: shell-session

    Usage: niamoto add_provider [OPTIONS] NAME PROVIDER_TYPE [SYNONYM_KEY]

      Register a data provider. The name of the data provider must be unique.
      The available provider types can be obtained using the 'niamoto
      provider_types' command. The available synonym keys can be obtained using
      the 'niamoto synonym_keys" command.

    Options:
      --help  Show this message and exit.

update_provider
...............

.. code-block:: shell-session

    Usage: niamoto update_provider [OPTIONS] CURRENT_NAME

      Update a data provider.

    Options:
      --new_name TEXT
      --synonym_key TEXT
      --help              Show this message and exit.

delete_provider
...............

.. code-block:: shell-session

    Usage: niamoto delete_provider [OPTIONS] NAME

      Delete a data provider.

    Options:
      -y TEXT
      --help   Show this message and exit.

sync
....

.. code-block:: shell-session

    Usage: niamoto sync [OPTIONS] PROVIDER_NAME [PROVIDER_ARGS]...

      Sync the Niamoto database with a data provider.

    Options:
      --help  Show this message and exit.


Raster commands
---------------

rasters
.......

.. code-block:: shell-session

    Usage: niamoto rasters [OPTIONS]

      List registered rasters.

    Options:
      --help  Show this message and exit.

add_raster
..........

.. code-block:: shell-session

    Usage: niamoto add_raster [OPTIONS] NAME RASTER_FILE_PATH

      Add a raster in Niamoto's raster database.

    Options:
      -t, --tile_dimension TEXT  Tile dimension <width>x<height>
      --help                     Show this message and exit.

update_raster
.............

.. code-block:: shell-session

    Usage: niamoto update_raster [OPTIONS] NAME RASTER_FILE_PATH

      Update an existing raster in Niamoto's raster database.

    Options:
      -t, --tile_dimension TEXT  Tile dimension <width>x<height>
      --new_name TEXT            The new name of the raster
      --help                     Show this message and exit.

delete_raster
.............

.. code-block:: shell-session

    Usage: niamoto delete_raster [OPTIONS] NAME

      Delete an existing raster from Niamoto's raster database.

    Options:
      --help   Show this message and exit.

raster_to_occurrences
.....................

.. code-block:: shell-session

    Usage: niamoto raster_to_occurrences [OPTIONS] RASTER_NAME

      Extract raster values to occurrences properties.

    Options:
      --help  Show this message and exit.

raster_to_plots
...............

.. code-block:: shell-session

    Usage: niamoto raster_to_plots [OPTIONS] RASTER_NAME

      Extract raster values to plots properties.

    Options:
      --help  Show this message and exit.

all_rasters_to_occurrences
..........................

.. code-block:: shell-session

    Usage: niamoto all_rasters_to_occurrences [OPTIONS]

      Extract raster values to occurrences properties for all registered
      rasters.

    Options:
      --help  Show this message and exit.

all_rasters_to_plots
....................

.. code-block:: shell-session

    Usage: niamoto all_rasters_to_plots [OPTIONS]

      Extract raster values to plots properties for all registered rasters.

    Options:
      --help  Show this message and exit.


Vector commands
---------------

vectors
.......

.. code-block:: shell-session

    Usage: niamoto vectors [OPTIONS]

      List the registered vectors.

    Options:
      --help  Show this message and exit.

add_vector
..........

.. code-block:: shell-session

    Usage: niamoto add_vector [OPTIONS] NAME VECTOR_FILE_PATH

      Add a raster in Niamoto's vector database.

    Options:
      --help  Show this message and exit.

update_vector
.............

.. code-block:: shell-session

    Usage: niamoto add_vector [OPTIONS] NAME VECTOR_FILE_PATH

      Add a raster in Niamoto's vector database.

    Options:
      --help  Show this message and exit.

delete_vector
.............

.. code-block:: shell-session

    Usage: niamoto delete_vector [OPTIONS] NAME

      Delete an existing vector from Niamoto's vector database.

    Options:
      --help   Show this message and exit.

Data publisher commands
-----------------------

publish_formats
...............

.. code-block:: shell-session

    Usage: niamoto publish_formats [OPTIONS] PUBLISHER_KEY

      Display the list of available publish formats for a given publisher.

    Options:
      --help  Show this message and exit.

publishers
..........

.. code-block:: shell-session

    Usage: niamoto publishers [OPTIONS]

      Display the list of available data publishers.

    Options:
      --help  Show this message and exit.

publish
.......

.. note::

    Please refer to :ref:`publishers` for details specific to each available
    data publisher.

.. code-block:: shell-session

    Usage: niamoto publish [OPTIONS] PUBLISHER_KEY PUBLISH_FORMAT [ARGS]...

      Process and publish data.

    Options:
      -d, --destination TEXT
      --help                  Show this message and exit.


Data marts commands
-------------------

dimension_types
...............

.. code-block:: shell-session

    Usage: niamoto dimension_types [OPTIONS]

      List the available dimension types.

    Options:
      --help  Show this message and exit.

dimensions
..........

.. code-block:: shell-session

    Usage: niamoto dimensions [OPTIONS]

      List the registered dimensions.

    Options:
      --help  Show this message and exit.

fact_tables
...........

.. code-block:: shell-session

    Usage: niamoto fact_tables [OPTIONS]

      List the registered fact tables.

    Options:
      --help  Show this message and exit.

create_taxon_dimension
......................

.. code-block:: shell-session

    Usage: niamoto create_taxon_dimension [OPTIONS]

      Create the taxon dimension.

    Options:
      --populate  Populate the dimension
      --help      Show this message and exit.

create_vector_dimension
.......................

.. code-block:: shell-session

    Usage: niamoto create_vector_dimension [OPTIONS] VECTOR_NAME

      Create a vector dimension from a registered vector.

    Options:
      --label_col TEXT  The label column name of the dimension
      --populate        Populate the dimension
      --help            Show this message and exit.

create_fact_table
.................

.. code-block:: shell-session

    Usage: niamoto create_fact_table [OPTIONS] NAME

      Create and register a fact table from existing dimensions. Use -d
      <dimension_name> for each dimension, and -m <measure_name> for each
      measure.

    Options:
      -d, --dimension TEXT  The fact table's dimension names  [required]
      -m, --measure TEXT    The fact table's measures names  [required]
      --help                Show this message and exit.

delete_dimension
................

.. code-block:: shell-session

    Usage: niamoto delete_dimension [OPTIONS] DIMENSION_NAME

      Delete a registered dimension.

    Options:
      --help  Show this message and exit.

delete_fact_table
.................

.. code-block:: shell-session

    Usage: niamoto delete_fact_table [OPTIONS] FACT_TABLE_NAME

      Delete a registered fact table.

    Options:
      --help  Show this message and exit.

populate_fact_table
...................

.. code-block:: shell-session

    Usage: niamoto populate_fact_table [OPTIONS] FACT_TABLE_NAME PUBLISHER_KEY

      Populate a registered fact table using an available publisher.

    Options:
      --help  Show this message and exit.
