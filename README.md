<p align="center">
   <img src="datawave-readme.png" />
</p>

# My Project

[![Apache License][li]][ll] ![Build Status](https://github.com/NationalSecurityAgency/datawave/actions/workflows/tests.yml/badge.svg)
![CI](https://img.shields.io/endpoint?url=https://github.com/NationalSecurityAgency/datawave/actions/workflows/tests.yml/badge.svg?jobs=build-and-test-microservices)
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
Cloning with all of the submodules is not required; however, if you are interested in checking 
out and building all of the datawave projects under one repo, read this!

It's easiest to clone the repository pointing the submodules at the same branch
```bash
# Start out by cloning the project as you normally would.
git clone git@github.com:NationalSecurityAgency/datawave.git

# Now, use git to retrieve all of the datawave submodules.
# This will leave your submodules in a detached head state.
cd datawave
git submodule update --init --recursive

# You can checkout the main branch for each submodule so that you are no longer in a detached head state.
# The addition of `|| :` will ensure that the command is executed for each submodule, 
# ignoring failures for submodules that don't have a main branch.
git submodule foreach 'git checkout main || :'

# It is recommended to build the project using multiple threads.
mvn -Pdocker,dist clean install -T 1C

# If you don't want to build the microservices, you can skip them.
mvn -Pdocker,dist -DskipMicroservices clean install -T 1C

# If you decide that you no longer need the submodules, you can remove them.
git submodule deinit --all
```

### DataWave Microservices

For more information about deploying the datawave quickstart and microservices, check out the [Docker Readme](docker/README.md#usage)

[li]: http://img.shields.io/badge/license-ASL-blue.svg
[ll]: https://www.apache.org/licenses/LICENSE-2.0
