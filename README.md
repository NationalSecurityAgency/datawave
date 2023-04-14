<p align="center">
   <img src="datawave-readme.png" />
</p>

[![Apache License][li]][ll] ![Build Status](https://github.com/NationalSecurityAgency/datawave/workflows/Tests/badge.svg)

DataWave is a Java-based ingest and query framework that leverages [Apache Accumulo](http://accumulo.apache.org/) to provide fast, secure access to your data. DataWave supports a wide variety of use cases, including but not limited to...

* Data fusion across structured and unstructured datasets
* Construction and analysis of distributed graphs
* Multi-tenant data architectures, with tenants having distinct security requirements and data access patterns
* Fine-grained control over data access, integrated easily with existing user-authorization services and PKI

The easiest way to get started is the [DataWave Quickstart](https://code.nsa.gov/datawave/docs/quickstart)

Documentation is located [here](https://code.nsa.gov/datawave/docs/)

Basic build instructions are [here](BUILDME.md)

## How to Use this Repository

The microservices and associated utility projects are intended to be
developed, versioned, and released independently and as such are stored
in separate repositories. This repository includes them all as submodules
in order to provide an easy way to import them all in an IDE for viewing
the code, or refactoring. [Git submodules](https://git-scm.com/book/en/v2/Git-Tools-Submodules)
require some extra commands over the normal ones that one may be familiar
with.

### Cloning with all submodules
It's easiest to clone the repository pointing the submodules AT the same branch
```bash
# This will checkout the feature/queryMicroservicesAccumulo2.1 branch for all of the submodules.
# By default, the submodules will all be in a detached head state.
git clone --recurse-submodules git@github.com:NationalSecurityAgency/datawave.git --branch feature/queryMicroservicesAccumulo2.1

# Checkout the feature/queryMicroservicesAccumulo2.1 branch for each submodule so that we are no longer in a detached head state.
# The addition of `|| :` will ensure that the command is executed for each submodule, 
# ignoring failures for submodules that don't have a main branch.
cd datawave
git submodule foreach 'git checkout feature/queryMicroservicesAccumulo2.1 || :'

# If you clone without the --recurse-submodules or a new submodule was added afterwards,
# then you will need to execute this within the new (empty) submodule directory
git submodule update --init --recursive
git submodule foreach 'git checkout feature/queryMicroservicesAccumulo2.1 || :'

# It is recommended to build the project using multiple threads
mvn -Pdocker,dist clean install -T 1C

# If you don't want to build the microservices, you can skip them
mvn -Pdocker,dist -DskipMicroservices clean install -T 1C
```

### DataWave Microservices

For more information about deploying the datawave quickstart and microservices, check out the [Docker Readme](docker/README.md#usage)

[li]: http://img.shields.io/badge/license-ASL-blue.svg
[ll]: https://www.apache.org/licenses/LICENSE-2.0
