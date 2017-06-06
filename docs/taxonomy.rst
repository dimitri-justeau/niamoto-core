.. _taxonomy:

Managing taxonomy
=================

Niamoto is taxonomy agnostic. It means that it does not make any assumption
on the taxonomic referential to be used, and therefore let you define it.
Since the choice of a taxonomic referential should not be a blocking decision
either a point of no return, we designed Niamoto's taxonomy mechanisms in a way
that it is always possible to change the taxonomic referential without
losing information, and to define correspondences between Niamoto's referential
and provider's one. Those correspondences are called **synonyms**. A synonym is
identified with a unique key.


Scenarios
---------

When importing occurrences from a data provider, Niamoto stores the taxon ids
as they are given (``provider_taxon_id``), and also tries to map them with the
defined Niamoto's referential (``taxon_id``). Three scenarios are possible:

1. The data provider uses the same referential as Niamoto
.........................................................

Then ``taxon_id`` will have the same value as ``provider_taxon_id``. This is a
special synonym, that has the key ``niamoto``.

2. The data provider uses another referential for which a synonym had been defined
..................................................................................

The ``taxon_id`` will be mapped according to the defined synonym for the
``provider_taxon_id``. If the synonym is incomplete (i.e. some
``provider_taxon_id`` values cannot be mapped with à ``taxon_id`` value, then
``taxon_id`` will be set null. The missing values can be updated when the synonym is
updated.

3. The data provider uses another referential for which no synonym is defined
.............................................................................

In this case, all values for ``taxon_id`` will be set null. Those value can be
later updated when a synonym is defined for the data provider.


In practice (Using the CLI)
---------------------------

1. Defining the Niamoto taxonomic referential
.............................................

Defining the Niamoto referential is done using a csv file. The csv file must
have a header defining at least the following columns:

- ``id``: The unique identifier of the taxon, in the provider's referential.
- ``parent_id``: The parent's id of the taxon. If the taxon is a root, let the value blank.
- ``rank``: The rank of the taxon, can be a value among: 'REGNUM', 'PHYLUM', 'CLASSIS', 'ORDO', 'FAMILIA', 'GENUS', 'SPECIES', 'INFRASPECIES'.
- ``full_name``: The full name of the taxon.
- ``rank_name``: The rank name of the taxon.

All the additional columns will be considered as synonyms, their values must
therefore be integers corresponding to the corresponding value in the referential
pointed by the synonym key. Let's consider the following example:

==== ========= ======= ========= ========= ==== ======
id   parent_id rank    full_name rank_name gbif taxref
==== ========= ======= ========= ========= ==== ======
0              FAMILIA A         A         10   1
1    0         GENUS   A a       a         20   2
2    1         SPECIES A a 1     1         30   3
3    1         SPECIES A a 2     2         40   4
==== ========= ======= ========= ========= ==== ======

We can set this table as Niamoto's taxonomy using the ``set_taxonomy`` command:

.. code-block:: bash

    $ niamoto set_taxonomy taxonomy.csv
    Setting the taxonomy...
    The taxonomy had been successfully set!
        4 taxa inserted
        2 synonyms inserted: {'taxref', 'gbif'}
        Advice: run 'niamoto map_all_synonyms' to update occurrences taxon identifiers
