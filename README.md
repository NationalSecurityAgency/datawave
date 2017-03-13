To perform a full 'dev' build  without unit tests:

```bash
mvn -Pdev -Ddeploy -Dtar -Ddist -DskipTests -DskipITs clean install
```

This command ill produce the following deployment archives:

1. Web Service: `./web-service-deployment/web-service-ear/target/datawave-web-service-${project.version}-dev.tar.gz`
2. Ingest: `./warehouse/assemble-core/deploy/target/datawave-dev-${project.version}-dist.tar.gz`

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
