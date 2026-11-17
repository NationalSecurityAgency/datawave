# Overview

This project contains security-related classes that are commonly used between the main Datawave project and the microservices. It is expected that this project will be configured and deployed as a JBOSS module via the [Wildfly assembly](../../web-services/deploy/application) project to make it available to the Datawave EAR deployment.

## Note:
Any compile dependencies here are expected to be imported into the Datawave webservices projects with scope `provided`, and provided via JBOSS modules. This is required to avoid classloader conflicts between the JBOSS modules and the Datawave EAR deployment. See the [Wildfly assembly README](../../web-services/deploy/application/README.md) for more details.
