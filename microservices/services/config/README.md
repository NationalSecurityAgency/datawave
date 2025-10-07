# Config Service

[![Apache License][li]][ll] ![Build Status](https://github.com/NationalSecurityAgency/datawave-config-service/workflows/Tests/badge.svg)

The Config service is an implementation of the Spring Cloud
[config](https://cloud.spring.io/spring-cloud-static/Greenwich.SR3/single/spring-cloud.html#_spring_cloud_config) 
service with some DATAWAVE-specific extensions. In particular, this service
adds the following on top of the default config service provided by Spring
Cloud:

* Encrypted private keys are supported
* Allows for specification of PKCS12 repositories as `.p12` files. The default
  Spring behavior is to take the extension and use it as the keystore type.
  However, Java has no `p12` keystore type, only `pkcs12` and `jks`.

### Getting Started

Follow the instructions in the [services/README](https://github.com/NationalSecurityAgency/datawave-microservices-root/blob/master/README.md#getting-started), or...

First, build the service with:
```bash
mvn -Pexec clean package
# Optional: use -Pdocker instead of -Pexec to build a docker image
```

Next, check out and modify the [sample_configuration][sample-config] directory
from the services root project. Make configuration changes as appropriate for
your environment. Or, if you prefer, create an entirely new configuration repo
to expose via the configuration service.

Now launch the configuration service. 

```bash
export CONFIG_DIR=/path/to/services-root
java -jar target/config-service*-exec.jar --spring.profiles.active=dev,nomessaging,native,open_actuator --spring.cloud.config.server.native.searchLocations=file://$CONFIG_DIR/sample_configuration 
```

[sample-config]:https://github.com/NationalSecurityAgency/datawave-microservices-root/tree/master/sample_configuration

[li]: http://img.shields.io/badge/license-ASL-blue.svg
[ll]: https://www.apache.org/licenses/LICENSE-2.0