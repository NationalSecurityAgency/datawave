## DataWave Spring Boot Starter for Microservices

[![Apache License][li]][ll] ![Build Status](https://github.com/NationalSecurityAgency/datawave-spring-boot-starter/workflows/Tests/badge.svg)

This is a Spring Boot "starter" class to build a DATAWAVE microservice. This
starter provides custom default behavior that is useful for new services.

## Authentication/Security

* Enables JSR-250 method annotations for spring security.
* Provides a Spring Security configuration that authenticates based on
  the presence of a JSON Web Token (JWT) in the Authorization request header.
  The JWT payload should be an encoded list of `DatawaveUser` objects.
* If the `remoteauth` profile is active, then this provides a Spring Security
  configuration that uses the provided PKI information to authenticate to a
  remote authorization service, provided no JWT was supplied instead.
* Spring Security pre-authentication for a proxied entity, where the primary
  caller can be trusted to delegate for a chain of users. This supports
  placing the delegate credential (subject/issuer DNs) in trusted headers
  `X-ProxiedEntitiesChain` and `X-ProxiedIssuersChain`.

## Web Customization

* [RestClient customization](src/main/java/datawave/microservice/config/web/RestClientProperties.java)
  to specify number of threads used overall and per-route for Spring RestClient.
* Customization of both RestClient and WebClient to provide client certificates
  based on the property `server.outbound-ssl.enabled`.
* Undertow customization to support collection of request timing
* Use Jackson for JSON conversions, but pay attention to JAX-B bindings.
* Adorn responses with headers indicating system name, request time, etc.
* CORS configuration

## Other Miscellany

### RabbitMQ Discovery

Provides Spring Cloud discovery of the RabbitMQ instance backing the
Spring Cloud Event bus.

### Accumulo

Provides [Accumulo configuration](src/main/java/datawave/microservice/config/accumulo/AccumuloClientConfiguration.java)
to access both the warehouse and metrics Accumulo clusters.

### Markings

Provides default markings configuration including `MarkingFunctions` and
a caffeine cache manager to storing cached markings.

### Metrics

Provides DropWizard metrics configuration/reporting via the
`metrics.reporter` prefix. See [MetricsConfigurationProperties](src/main/java/datawave/microservice/config/metrics/MetricsConfigurationProperties.java).

### HTML Responses

Provides [message converters](src/main/java/datawave/microservice/http/converter/html)
for returning a formatted HTML page.

### Protostuff Responses

Reads/writes Google protobuf entities/responses using the protostuff library
for messages implementing the protostuff Message interface.

### REST Exceptions

[RestExceptionHandler](src/main/java/datawave/microservice/rest/exception/RestExceptionHandler.java)
returns a datawave `VoidResponse` upon receipt of an exception.

### Validators

[NotBlankIfFieldEquals](src/main/java/datawave/microservice/validator/NotBlankIfFieldEquals.java)
validates that a field must not be blank if another field matches a specified
value.
[RequiredValueIfFieldEquals](src/main/java/datawave/microservice/validator/RequiredValueIfFieldEquals.java)
validates that a field is set to a specified value if another field matches a
specified value.

### Events

[AuthorizationEvictionEvent](src/main/java/org/springframework/cloud/bus/event/AuthorizationEvictionEvent.java)
is an event that is published to the event bus when user credential data is
evicted from the authorization service's cache. Other services may want to
respond to this event to refresh a display or force the user to re-authenticate.

### PKI

A test certificate authority and user/server certificates for use in tests
and/or demo applications. See the [resources directory](src/main/resources).

### CSS

A default [`screen.css`](src/main/resources/css/screen.css) file is provided
for displaying tables in web apps.

### Banner

A default [datawave banner](src/main/resources/banner.txt) for display by
Spring at application startup.

[li]: http://img.shields.io/badge/license-ASL-blue.svg
[ll]: https://www.apache.org/licenses/LICENSE-2.0