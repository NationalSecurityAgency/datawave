# Overview

This project helps prepares and assembles the EAR for deployment to Wildfly. It also prepares artifacts that should be copied over onto the Wildfly distribution once unpacked.

## JBOSS Modules

Wildfly uses a modular class loading system through the use of JBOSS modules. Each module can define the libraries it provides, as well as dependencies on other modules in order to have the modules from that module added to its class path. In order to avoid classloader conflicts, it is essential to have commonly used libraries isolated in their own modules, and added as dependencies in other modules or deployments that require access to the classes.

In addition to the Wildfly application modules, EAR deployments are treated as modules, and are defined by the following rules:

1. The `lib/` directory of the EAR is a single module called the parent module.
2. Each WAR deployment within the EAR is a single module.
3. Each EJB JAR deployment within the EAR is a single module.

Managing the module dependencies for the Datawave EAR deployment is done through the [jboss-deployment.xml](src/main/application/META-INF/jboss-deployment-structure.xml). **IMPORTANT**: any dependencies that are provided to the EAR deployment through a module should be configured with a `provided` scope in the `webservice-parent` [pom.xml](../../pom.xml) to prevent them from being included in the EAR's `/lib` folder.

### Custom Modules

We create several JBOSS modules that will be part of Wildfly upon startup. These modules can be found in the project [modules](src/main/wildfly/overlay/modules) directory. Most of these modules are self-explanatory and are, except for the module `datawave.webservices.datawave-security-elytron-module`, common dependencies between other modules and the Datawave deployment.

* [datawave.webservice.datawave-security-elytron-module](src/main/wildfly/overlay/modules/datawave/webservice/datawave-security-elytron-module)
    * This module contains the custom Wildfly security components that create the Datawave authentication and authorization workflow leveraging Elytron. These classes must be deployed as a separate JBOSS module. This module should not be imported as a dependency to the Datawave deployment.
* [datawave.webservice.datawave-security-elytron](src/main/wildfly/overlay/modules/datawave/webservice/datawave-security-elytron)
    * Contains security-related artifacts that are commonly used between the Datawave deployment and the module `datawave.webservice.datawave-security-elytron-module`.
* [datawave.commons.datawave-commons-security](src/main/wildfly/overlay/modules/datawave/commons/datawave-commons-security)
    * Contains security-related artifacts that are commonly used between the Datawave deployment, the module `datawave.webservice.datawave-security-elytron-module`, and the microservices.
* [com.fasterxml.jackson.datatype.jackson-datatype-guava](src/main/wildfly/overlay/modules/com/fasterxml/jackson/datatype/jackson-datatype-guava)
* [com.fasterxml.jackson.module.jackson-module-jaxb-annotations](src/main/wildfly/overlay/modules/com/fasterxml/jackson/module/jackson-module-jaxb-annotations)
* [com.github.ben-manes.caffeine](src/main/wildfly/overlay/modules/com/github/ben-manes/caffeine)
* [io.jsonwebtoken.jjwt-api](src/main/wildfly/overlay/modules/io/jsonwebtoken/jjwt-api)
* [io.jsonwebtoken.jjwt-impl](src/main/wildfly/overlay/modules/io/jsonwebtoken/jjwt-impl)
* [io.jsonwebtoken.jjwt-jackson](src/main/wildfly/overlay/modules/io/jsonwebtoken/jjwt-jackson)
* [org.apache.commons.commons-text](src/main/wildfly/overlay/modules/org/apache/commons/commons-text)
* [org.apache.hadoop.common](src/main/wildfly/overlay/modules/org/apache/hadoop/common)

### Overridden Modules

We also override the library versions of several modules that are part of the core Wildfly application modules in order to synchronize the versions used by Datawave and Wildfly. These modules may get automatically added to deployments by Wildfly and may be dependencies of other core modules. The versions are overridden by copying updated versions of the libraries and `module.xml` files to the [modules/system/layers/base](src/main/wildfly/overlay/modules/system/layers/base) directory.

- [org.slf4j](src/main/wildfly/overlay/modules/system/layers/base/org/slf4j)
- [org.slf4j.impl](src/main/wildfly/overlay/modules/system/layers/base/org/slf4j/impl)
- [org.apache.commons.lang3](src/main/wildfly/overlay/modules/system/layers/base/org/apache/commons/lang3)
