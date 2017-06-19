.. _publishers:

Data publishers reference
=========================

Occurrence publisher
--------------------

Publish the occurrence dataframe with properties as columns.

:key:
    ``occurrences``
:formats:
    ``csv``
:options:
    :``--properties <properties>``: List of properties to extract as columns.
                                    If not specified, retrieve all the existing
                                    properties.
                                    Exemple: ``--properties height dbh``.
    :``--drop_null_properties``: Flag indicating that record with null values
                                 for properties must be dropped.


Plot publisher
--------------

Publish the plot dataframe with properties as columns.

:key:
    ``plots``
:formats:
    ``csv``
:options:
    :``--properties <properties>``: List of properties to extract as columns.
                                    If not specified, retrieve all the existing
                                    properties.
                                    Exemple: ``--properties width height``.


Plot/Occurrence publisher
-------------------------

Publish the plot/occurrence dataframe.

:key:
    ``plots_occurrences``
:formats:
    ``csv``
:options:


Taxon publisher
---------------

Publish the taxa dataframe.

:key:
    ``taxa``
:formats:
    ``csv``
:options:
    :``--include_mptt``: Flag indicating that the MPTT (Modified Pre-ordered
                         Traversal Tree) columns must be included

