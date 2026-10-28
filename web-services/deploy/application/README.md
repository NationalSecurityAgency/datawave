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

### Spring Boot Configuration

The Datawave web service uses a combination of Spring and Spring Boot to configure the application context. The configuration is done through a combination of XML configuration files and YML property files. Beans that are part of the application context can be defined either in the XML configuration files or through Spring Boot's component scanning. The XML configuration files are generally located on the classpath, primarily in the [dtawave-ws-deploy-configuration resources](../configuration/src/main/resources) directory and in the [spring-boot-starter-datawave-query resources](../../../microservices/starters/query/src/main/resources) directory. Reference YML property files are located in the [docker/config](../../../docker/config) directory but are normally retrieved from a spring configuration server.

The Spring application context is initialized through the `SpringCDIExtension` class, which is a CDI extension that is loaded by the Wildfly container. The `SpringCDIExtension` class is responsible for initializing the Spring application context and loading the XML configuration files and YML property files. The `SpringCDIExtension` class also provides a mechanism for reporting the configuration of the application context, including the configured and loaded XML configuration files, YML property files, a property report that list all property names, values, and source YML, and the names of all beans that are part of the application context.

THe SpringCDIExtension class first creates a bootstrap context which loads YML files from the classpath, and either the configuration server or the directories specified in the `spring.config.additional-location` property. The SpringCDIExtension class then creates a simple AnnotationConfigApplicationContext taht searches the datawave.configuration.spring package. If datawave.configuration.spring.configure-from-properties=true (supplied via YML or system property override), then the SpringContextPropties bean will be loaded from YML otherwise we will look for a beanRefContext.xml file on the classpath to load the SpringContextProperties bean. 

The SpringContextProperties bean is used to configure an AnnotationConfigApplicationContext and a ClasspathXmlApplicationContext. The annotation context is used to load beans that are defined through component scanning of the packages specified in SpringContextProperties's scan-base-packages property and the XML context is used to load beans that are defined in XML configuration files which are specified in SpringContextProperties's sources property. The annotation context is a child of the bootstrap context and the XML context is a child of the annotation context. Child contexts can access beans and properties of their parent contexts but not vice versa.

The spring.config.name property is used to specify the name of the YML property file(s) to use for configuration. The default for the datawave webserver is `application, wildfly` which means that whether you are using the configuration server or a directory for your YML property files, the base filenames `application.yml` and `wildfly.yml` will be used for configuration. The property spring.active.profiles is used to specify the active Spring profile(s) to use and will cause any matching YML property files to also be added to the application context.  The query profile is added in [datawave-ws-common bootstrap.yml](../../common/src/main/resuorces/config/bootstrap.yml) via the spring.profiles.include property which forces that profile to always be included. For -Dspring.active.profiles=dev,federation the following YML property files (if found) will be used for configuration:

- bootstrap.yml
- application.yml 
- wildfly.yml, 
- bootstrap-query.yml
- bootstrap-dev.yml
- bootstrap-federation.yml
- application-query.yml
- application-dev.yml
- application-federation.yml
- wildfly-query.yml
- wildfly-dev.yml
- wildfly-federation.yml

This list is in increasing precedence from top to bottom. Properties defined in YML files lower in the list will override properties defined in YML files higher in the list. i.e. properties in the wildfly- files will override properties in the application- files which will override properties in the bootstrap- files.

Spring properties can be defined in YML and can also be overridden on the command line

- `spring.profiles.active` - The active Spring profile(s) to use. This can be a comma-separated list of profiles.
- `spring.cloud.config.enabled` - Whether to enable the Spring Cloud Config client. The default is `true`.
- `spring.config.additional-location` - Additional locations to search for configuration files. This can be a comma-separated list of locations. The default is empty.

To start the Datawave web service without using the configuration server and instead use YML files at a file path that works for your environment, set the following properties on the command line

- `-Dspring.cloud.config.enabled=false -Dspring.config.additional-location=file:/data/git/workspaces/datawave/docker/config/`

To start the Datawave web service without starting other microservices (authorization, cache, audit, querymetrics), ensure that the dev profile is included in -Dspring.profiles.active and that that you have the following set in wildfly-dev.yml or another YML file that is included

- `datawave.active-implementation.datawave-user-service=TestDatawaveUserService` (do not use the remote user service, configure test users in wildfly-dev.yml)
- `datawave.active-implementation.audit-service=NoOpAuditor` (do not use the remote audit service)
- `datawave.query.metric.writer.use-remote-service=false` (do not use the remote query metrics service)