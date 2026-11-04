# Annotation Messaging

This module provides shared Spring messaging support for services that exchange
`datawave.annotation.protobuf.v1.AnnotationMessage` values.

## Why this module exists

`AnnotationMessage` is a generated Protocol Buffers message and its native wire representation is protobuf binary. Spring's standard JSON conversion does not reliably reconstruct generated protobuf message classes, and separate service-local converters can easily drift.

`AnnotationProtobufMessageConverter` extends Spring's standard `ProtobufMessageConverter`. The DATAWAVE-packaged subclass is intentional: Spring Cloud Stream 3.2 filters most `org.springframework.*` converter beans from its custom converter collection, while accepting application-provided converter classes.

## Module dependencies

`annotation-messaging` depends on `annotation-api`, which exposes the annotation
model from `datawave-core-annotation`. Both `annotation-service` and
`annotation-cache-service` depend on `annotation-messaging`, so the cache service
receives `annotation-api` transitively. `annotation-service` also declares
`annotation-api` directly.

Both services retain direct `datawave-core-annotation` dependencies because their
source code uses its classes. Keep those versions aligned with the core version
declared by `annotation-api`. `annotation-cache-api` contains cache-specific types
and does not depend on the annotation model or messaging.

## Using the module

Add the dependency to each annotation message producer or consumer:

```xml
<dependency>
    <groupId>gov.nsa.datawave.microservice</groupId>
    <artifactId>annotation-messaging</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

DATAWAVE microservices that scan `datawave.microservice` discover `AnnotationMessagingConfiguration` automatically. Applications with a narrower component scan should import it explicitly:

```java
@Import(AnnotationMessagingConfiguration.class)
```

Every binding that carries `AnnotationMessage` must declare the protobuf content type:

```yaml
spring:
  cloud:
    stream:
      bindings:
        annotationSource-out-0:
          content-type: application/x-protobuf
        annotationSink-in-0:
          content-type: application/x-protobuf
```

Use the same content type on all producers and consumers attached to a destination. Do not register another protobuf converter in an individual service.

## Compatibility

The serialized contract is the protobuf schema supplied by `datawave-core-annotation`. Schema evolution must follow protobuf compatibility rules: do not reuse field numbers, and prefer additive changes with new field numbers.
