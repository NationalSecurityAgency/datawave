## DataWave Spring Boot Starter for Query Auditing

This starter may be used by any microservice that needs to support auditing of DataWave queries. It provides an
auto-configured REST client for submitting query audit messages to a remote auditor service.

The REST client is provided as an [AuditClient](src/main/java/datawave/microservice/audit/AuditClient.java)
instance, which can be injected by the service as needed to integrate auditing functionality. In this context,
an individual audit request is given as an instance of [AuditClient.Request](src/main/java/datawave/microservice/audit/AuditClient.java#L89).

Other features include the ability to enable/disable auditing altogether via config properties, the ability to
enable automatic discovery of the remote audit service, and others.
