# Overview

This project contains security-related classes that are commonly used between the main Datawave project and the microservices.

## [datawave-ws-security](security)

Contains security-related EJBs. Expected to be deployed as part of the Datawave EAR.

## [datawave-ws-security-elytron](security-elytron)

Contains security-related artifacts that are commonly used between projects that are deployed as part of the Datawave EAR, and the datawave-ws-security-elytron-module project. It is expected to be configured and deployed as a JBOSS module via the [Wildfly assembly](../../web-services/deploy/application) project.

## [datawave-ws-security-elytron-module](security-elytron-module)

Contains security-related artifacts that are required to create the Wildfly security domains that are used for authentication and authorization. It is expected to be configured and deployed as a JBOSS module via the [Wildfly assembly](../../web-services/deploy/application) project. Wildfly requires custom Elytron security components to be deployed in separate JBOSS modules. See the project [README](security-elytron-module/README.md) for more details.
## Note:
All non-test dependencies for datawave-ws-security-elytron and datawave-ws-security-elytron-module are expected have the scope `provided`. These dependencies must be provided via JBOSS modules in order to avoid classloader conflicts between the JBOSS modules and the Datawave EAR deployment. See the [Wildfly assembly README](../deploy/application/README.md) for more details.
