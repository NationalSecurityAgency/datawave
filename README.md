# Building Datawave

To perform a full (non-release) 'dev' build  without unit tests:

```bash
mvn -Pdev -Ddeploy -Dtar -DskipTests -DskipITs clean install
```

This command will produce the following deployment archives:

1. Web Service: `./web-service-deployment/web-service-ear/target/datawave-web-service-${project.version}-dev.tar.gz`
2. Ingest: `./warehouse/assemble-core/deploy/target/datawave-dev-${project.version}-dist.tar.gz`

### Building A Release

In order to build a release, you must also define the dist variable by adding `-Ddist` to the command-line as follows:

```bash
mvn -Pdev -Ddeploy -Dtar -Ddist -DskipTests -DskipITs clean install
```

Note that this will build javadocs and source jars.

### Prerequisites

Note that, currently, you may have to install the read-properties and assert-properties plugins before you are able
to build:

```bash
# Build ReadProperties
pushd contrib/read-properties
mvn clean install
popd

# Now build AssertProperties
pushd contrib/assert-properties
mvn clean install
popd
```

## Quickstart

See **contrib/datawave-quickstart** to quickly deploy a standalone DataWave cluster
including ingest and web service components
