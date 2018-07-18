### Prior to Building

Before you can build Datawave, you must install the read-properties, assert-properties, and code-style packages. These
only need to be installed once as a stopgap until these artifacts are published to maven central. You can either add
`-Dplugins` to the build line you use below in the "Building Datawave" section, so perform the following steps:

```bash
# Build ReadProperties
pushd contrib/read-properties
mvn clean install
popd

# Now build AssertProperties
pushd contrib/assert-properties
mvn clean install
popd

pushd contrib/code-style
mvn clean install
popd
```

# Building Datawave

To perform a full (non-release) 'dev' build  without unit tests:

```bash
mvn -Pdev -Ddeploy -Dtar -DskipTests -Dservices clean install
```

This command will produce the following deployment archives:

1. Web Service: `./web-services/deploy/application/target/datawave-ws-deploy-application-${project.version}-dev.tar.gz`
2. Ingest: `./warehouse/assemble/datawave/target/datawave-dev-${project.version}-dist.tar.gz`

### Building A Release

In order to build a release, you must also define the dist variable by adding `-Ddist` to the command-line as follows:

```bash
mvn -Pdev,examples -Ddeploy -Dtar -Ddist -DskipTests -Dservices clean install
```

Note that this will build javadocs and source jars.
