# Query State Service

[![Apache License][li]][ll] ![Build Status](https://github.com/NationalSecurityAgency/datawave-query-state-service/workflows/Tests/badge.svg)

The Query State Service is used to store the current state of a query's executioni
such that any of the query exection services can pick up a query and continue
its execution.

### Getting Started

Follow the instructions in the [services/README](https://github.com/NationalSecurityAgency/datawave-microservices-root/blob/master/README.md#getting-started), or...

First, build the service with:
```bash
mvn -Pexec clean package
# Optional: use -Pdocker instead of -Pexec to build a docker image
```

Next, ...

Now launch the configuration service. 

```bash
export CONFIG_DIR=/path/to/services-root
java -jar service/target/query-state-service*-exec.jar --spring.profiles.active=dev,nomessaging,native,open_actuator --spring.cloud.config.server.native.searchLocations=file://$CONFIG_DIR/sample_configuration 
```

[sample-config]:https://github.com/NationalSecurityAgency/datawave-microservices-root/tree/master/sample_configuration

[li]: http://img.shields.io/badge/license-ASL-blue.svg
[ll]: https://www.apache.org/licenses/LICENSE-2.0
