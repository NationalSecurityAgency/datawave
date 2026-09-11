## Annotation Service

[![Apache License][li]][ll]

The annotation service is a DATAWAVE microservice that provides annotation storage, retrieval,
and update capabilities for the DATAWAVE ecosystem. The service supports federated reads across
both `annotation` and `truthmark` table pairs, source-metadata masking, and an asynchronous
ack/retry write pipeline with optional file-backup fallback.

### Annotation Context

*https://host:port/annotation/v1/*

---

### User API

| Method   | Operation                                                | Description                                                                  | Path Params                                        | Request Body     |
|:---------|:---------------------------------------------------------|:-----------------------------------------------------------------------------|:---------------------------------------------------|:-----------------|
| `GET`    | /source/{analyticHash}                                    | Retrieves the annotation source for the given analytic hash                 | [AnalyticHash]                                     | N/A              |
| `GET`    | /{idType}/{id}/types                                      | Lists the distinct annotation types for a document                          | [IdType], [Id]                                      | N/A              |
| `GET`    | /{idType}/{id}                                            | Retrieves all annotations for a document                                    | [IdType], [Id]                                      | N/A              |
| `GET`    | /{idType}/{id}/type/{annotationType}                      | Retrieves all annotations of a specific type for a document                 | [IdType], [Id], [AnnotationType]                   | N/A              |
| `GET`    | /{idType}/{id}/annotation/{annotationId}                  | Retrieves a single annotation by annotation ID                              | [IdType], [Id], [AnnotationId]                     | N/A              |
| `GET`    | /{idType}/{id}/annotation/{annotationId}/segment/{segmentHash} | Retrieves a single segment of an annotation                          | [IdType], [Id], [AnnotationId], [SegmentHash]      | N/A              |
| `POST`   | /{idType}/{id}/annotation                                 | Adds a new annotation for a document                                        | [IdType], [Id]                                      | [Annotation]     |
| `PUT`    | /{idType}/{id}/annotation/{annotationId}                  | Updates an existing annotation by linking a new version to the target       | [IdType], [Id], [AnnotationId]                     | [Annotation]     |

Users must possess the **AnnotationWriter** role to access the `POST` and `PUT` endpoints.

* See [AnnotationControllerV1] class for details

### Architecture

#### Read Path (Federated + Truthmark)

Reads are performed via `FederatedAnnotationReader`, which fans out queries across
both the `annotation`/`annotationSource` table pair and the
`truthmark`/`truthmarkSource` table pair. This mirrors the legacy behavior in
`AnnotationManagerBean.RequestContext`.

The table names are configurable via `AnnotationProperties`:

| Property (YAML)                        | Default          | Description                              |
|:---------------------------------------|:-----------------|:-----------------------------------------|
| `annotation.annotation-table-name`     | `annotation`     | Primary annotation table                 |
| `annotation.annotation-source-table-name` | `annotationSource` | Primary annotation source table    |
| `annotation.truthmark-table-name`      | `truthmark`      | Truthmark annotation table (writes here) |
| `annotation.truthmark-source-table-name` | `truthmarkSource` | Truthmark annotation source table (writes here) |
| `annotation.mask-source-metadata`      | `["visibility"]` | Metadata keys to mask from sources     |
| `annotation.shard-table-name`          | `shard`          | Shard table for internal ID lookups     |

#### Source-Metadata Masking

When returning annotation sources injected into annotations, certain metadata
fields (e.g., `visibility`) are masked based on the `maskSourceMetadata`
configuration. This is applied **only** in `lookupAndInjectAnnotationSource`,
never in the `getAnnotationSource` endpoint.

#### Write Path (Microservice-Only)

The annotation service is the sole writer for truthmark tables. The monolith
ingest framework owns the `annotation`/`annotationSource` tables. Writes are
asynchronous via a message sink (`AnnotationConsumer` → `AccumuloAnnotationWriter`),
with configurable retry (`maxAttempts`, `failTimeoutMillis`,
`backoffIntervalMillis`) and optional file-backup fallback
(`fileAnnotationWriter`, injected as `@Autowired(required = false)`).

#### Ack / Retry Protocol

The `AnnotationAckTracker` singleton bean (replacing the static
`correlationLatchMap`) tracks in-flight writes by correlation ID. The
`processConfirmAck` handler counts down latches when acks arrive from the
message broker. This protocol is microservice-only; the legacy
`AnnotationManagerBean` has no write path.

### Test Data

The test suite includes sample annotation data for baseline testing:

* `service/src/test/resources/data/annotation_baseline.ndjson` — generated
  compatibility reference produced through the serialization code in
  `core/annotation`. It is retained with the service test resources for
  compatibility validation and is not currently loaded directly by the
  annotation-service unit tests.

### Test Certificates

The test suite includes SSL certificates suitable for local testing:

* `service/src/test/resources/ssl/host.p12` — Server certificate
* `service/src/test/resources/ssl/rootCA.p12` — Root certificate authority

### Roles

Users must possess the **AnnotationWriter** role to add or update annotations.
Query operations (`GET`) are available to any authenticated user.

### Configuration

Configuration is managed via `AnnotationProperties` (Spring `@ConfigurationProperties`
with prefix `annotation`). See [AnnotationProperties] for the full set of options,
including retry settings and ack timeout configuration.

### Docker

For local containerized integration testing, refer to the
[DATAWAVE Docker Compose README][docker-compose] for instructions on building
and running DATAWAVE microservices. The annotation service is opt-in through
the Compose `annotation` profile; these repository assets do not enable a
production deployment.

[docker-compose]:../../../docker/README.md

---

### Getting Started

1. Prepare the supporting services required by the selected configuration.
   A typical development environment includes Config Server, authorization,
   RabbitMQ, Accumulo, Hazelcast, and the DATAWAVE web service used for internal
   identifier lookups. The [Docker Compose guide][docker-compose] is the
   authoritative in-repository setup for the complete local environment.

2. Make the annotation configuration available to Config Server. The checked-in
   examples are `service/src/main/config/annotation.yml` and
   `docker/config/annotation.yml`; select and adapt the one appropriate for the
   development environment.

3. Launch this service with the `remoteauth` profile to enable client-certificate
   authentication:

   ```
   java -jar service/target/annotation-service*-exec.jar --spring.profiles.active=dev,remoteauth
   ```

4. Ensure that the repository's [testUser.p12][testUser] (password: *ChangeIt*) cert is
   imported into your browser, and then visit any of the following:

   * https://localhost:8643/annotation/v1/source/testAnalyticHash
   * https://localhost:8643/annotation/v1/DOCUMENT/20250704_249:testDataType:abcde.fghij.klmno
   * https://localhost:8643/annotation/v1/DOCUMENT/20250704_249:testDataType:abcde.fghij.klmno/annotation/testAnnotationId

[AnnotationControllerV1]:service/src/main/java/datawave/microservice/annotation/service/AnnotationControllerV1.java
[AnnotationProperties]:service/src/main/java/datawave/microservice/annotation/service/config/AnnotationProperties.java
[testUser]:../../starters/datawave/src/main/resources/testUser.p12
[analyticHash]:service/src/main/java/datawave/microservice/annotation/service/AnnotationControllerV1.java
[idType]:service/src/main/java/datawave/microservice/annotation/service/AnnotationControllerV1.java
[id]:service/src/main/java/datawave/microservice/annotation/service/AnnotationControllerV1.java
[annotationId]:service/src/main/java/datawave/microservice/annotation/service/AnnotationControllerV1.java
[segmentHash]:service/src/main/java/datawave/microservice/annotation/service/AnnotationControllerV1.java
[annotationType]:service/src/main/java/datawave/microservice/annotation/service/AnnotationControllerV1.java
[annotation]:service/src/main/java/datawave/microservice/annotation/service/AnnotationControllerV1.java

[li]: http://img.shields.io/badge/license-ASL-blue.svg
[ll]: https://www.apache.org/licenses/LICENSE-2.0
