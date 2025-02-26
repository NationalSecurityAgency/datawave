<p align="center">
   <img src="datawave-readme.png" />
</p>

[![Apache License][li]][ll] ![Build Status](https://github.com/NationalSecurityAgency/datawave/actions/workflows/tests.yml/badge.svg)

DataWave is a Java-based ingest and query framework that leverages [Apache Accumulo](http://accumulo.apache.org/) to provide fast, secure access to your data. DataWave supports a wide variety of use cases, including but not limited to...

* Data fusion across structured and unstructured datasets
* Construction and analysis of distributed graphs
* Multi-tenant data architectures, with tenants having distinct security requirements and data access patterns
* Fine-grained control over data access, integrated easily with existing user-authorization services and PKI

The easiest way to get started is the [DataWave Quickstart](https://code.nsa.gov/datawave/docs/quickstart)

Documentation is located [here](https://code.nsa.gov/datawave/docs/)

Basic build instructions are [here](BUILDME.md)

## How to Use this Repository

The microservices and associated utility projects are intended to be developed, versioned,
and released independently.  The following subdirectories contain those independently
versioned modules:

```
core/utils/type-utils
contrib/datawave-utils
core/base-rest-responses
core/in-memory-accumulo
core/metrics-reporter
core/utils/accumulo-utils
core/utils/common-utils
core/utils/metadata-utils
microservices/microservice-parent
microservices/microservice-service-parent
microservices/starters/audit
microservices/starters/cache
microservices/starters/cached-results
microservices/starters/datawave
microservices/starters/metadata
microservices/starters/query
microservices/starters/query-metric
microservices/services/accumulo
microservices/services/audit
microservices/services/authorization
microservices/services/config
microservices/services/dictionary
microservices/services/file-provider
microservices/services/hazelcast
microservices/services/map
microservices/services/mapreduce-query
microservices/services/modification
microservices/services/query
microservices/services/query-executor
microservices/services/query-metric
```

Each of those subdirectories contain a .gitrepo file that keeps track of where the code came from.

### Updating one of the datawave sub-repositories
At one point we used submodules to link in a all of the sub-repositories.  We have now switched
to including the submodules' code directly into the main datawave repository.  The git subrepo
mechanism (https://github.com/ingydotnet/git-subrepo) was used to facilitate the transition.
That same mechanism can be used to pull in changes from the other repositories as needed until
they can be removed altogether.  The original cloning of the sub repositories was done using
the subrepo command as follows:
```
git subrepo clone <repo> <dir>
```
If changes need to be pulled in, then the following process can be used:
```
git subrepo pull <dir>
```
### Building

It is recommended to build the project using multiple threads.  This will not build the starters, utilities, and services.
```
mvn -Pdocker,dist clean install -T 1C
```

If you want to build the starters, util modules, and services as well then try this
```
mvn -Pdocker,dist -Dstarters -Dservices -Dutils clean install -T 1C
```
If you want to build the service apis but not the services themselveds then add -DonlyServiceApis

NOTE: The util modules, starters, and services are actually tagged and deployed separately.
  Hence the snapshot versions within those sub repos are not connected together.

### DataWave Microservices

For more information about deploying the datawave quickstart and microservices, check out the [Docker Readme](docker/README.md#usage)

[li]: http://img.shields.io/badge/license-ASL-blue.svg
[ll]: https://www.apache.org/licenses/LICENSE-2.0
