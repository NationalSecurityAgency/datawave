#Building Datawave

To perform a full (non-release) 'dev' build  without unit tests:

```bash
mvn -Pdev -Ddeploy -Dtar -DskipTests -DskipITs clean install
```

This command will produce the following deployment archives:

1. Web Service: `./web-service-deployment/web-service-ear/target/datawave-web-service-${project.version}-dev.tar.gz`
2. Ingest: `./warehouse/assemble-core/deploy/target/datawave-dev-${project.version}-dist.tar.gz`

###Building A Release

In order to build a release, you must also define the dist variable by adding `-Ddist` to the command-line as follows:

```bash
mvn -Pdev -Ddeploy -Dtar -Ddist -DskipTests -DskipITs clean install
```

Note that this will build javadocs and source jars. The javadocs for JAX-RS and JAX-B classes are built using a plugin
that only runs with JDK 7 currently. Therefore, in order to execute a release build, you must have a JDK 7
(and it can't be too new of an OpenJDK release) installed, and the JDK7_HOME environment variable set to point
to the installation (i.e., `${JDK7_HOME}/bin/javadoc` should exist). This is a stopgap measure that we hope to fix
in the near future.

###Prerequisites

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