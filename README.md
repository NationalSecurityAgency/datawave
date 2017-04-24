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

Note that you may also need to install JPoller artifacts in your local maven repository since we currently depend on them and they are not published in maven's central repository.

```bash
pushd contrib/jpoller
mvn install:install-file -Dfile=org.sadun.util.jar -DgroupId=org.sadun -DartifactId=util -Dversion=1.5.1 -Dpackaging=jar
mvn install:install-file -Dfile=pollmgt.jar -DgroupId=org.sadun -DartifactId=JPoller -Dversion=1.5.1 -Dpackaging=jar
popd
```