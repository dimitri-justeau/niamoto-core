# Niamoto #

[![Build Status](https://travis-ci.org/dimitri-justeau/niamoto-core.svg?branch=master)](https://travis-ci.org/dimitri-justeau/niamoto-core)
[![codecov](https://codecov.io/gh/dimitri-justeau/niamoto-core/branch/master/graph/badge.svg)](https://codecov.io/gh/dimitri-justeau/niamoto-core)

**Ecological information system for decision support in ecosystems and biodiversity conservation.**

Niamoto aggregates ecological data and provides a platform for defining and running scientific data workflows. The value-added data produced through workflow can be published in app-specific databases and then feed websites, decision support applications, scientific application, etc.

Niamoto is funded by the COGEFOR projet in New Caledonia, which is a partnership between the North Province of New Caledonia, the New Caledonian Agronomic Institute (IAC) and the CIRAD. The AMAP joint research unit and the IRD are also implied in the project.
 
![alt text](docs/_static/logos/logo_pn.png "Province Nord") ![alt text](docs/_static/logos/logo_iac.png "IAC") ![alt text](docs/_static/logos/logo_cirad.png "CIRAD") ![alt text](docs/_static/logos/logo_amap.png "UMR AMAP") ![alt text](docs/_static/logos/logo_ird.png "IRD")

## Overview ##

*Niamoto is currently in active development and thus not ready for production use. A lot of functionalities are still to implement. If you are interested in the project and willing to contribute or collaborate feel free to contact*

Niamoto is an ecological information system designed to fill the gap between scientists and decision makers. It is currently being used and developed for tropical rainforest ecosystems in New Caledonia, but had been designed generic enough to be adapted for other ecosystems and contexts.

Among more, Niamoto provides:

- Aggregation of occurrence and plot data from several data providers.
- Dynamic properties on occurrences and plots.
- Matching between internal taxonomic referential and provider's ones.
- A strong emphasis on the geographical dimension of ecological data.
- Generic methods for storing geographic layers (raster and vector).
- Schedulable scientific data workflows.
- Publication of value-added aggregated data.

## Seting up a development environment ##

**Prerequesite**: You need to have `PostgreSQL` and `PostGIS` install in your system, or have access to a distant instance of it. Check the test settings in the `tests/test_data/test_niamoto_home/settings.py` file.

It is recommended to use `virtualenv` to setup a development environment with python 3.4, 3.5 or 3.6. Please refer to https://virtualenv.pypa.io/en/stable/

First, clone the repository in your system using `git`:

```
git clone https://github.com/dimitri-justeau/niamoto-core.git
```

Move in the cloned repository and install the dependencies using `pip`:

```
pip install -r requirements.txt
```

Finally download the test data:

```
sh tests/download_test_data.sh
```

You can check the tests using:

```
python run_tests.py
```
