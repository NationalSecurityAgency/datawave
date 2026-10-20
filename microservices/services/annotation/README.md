## Annotation Service

The annotation service is a DATAWAVE microservice that provides annotation storage, retrieval,
and update capabilities for the DATAWAVE ecosystem. The service supports federated reads across
both `annotation` and `truthmark` table pairs, source-metadata masking, and an asynchronous
ack/retry write pipeline with optional file-backup fallback.

### Annotation Context

*https://host:port/annotation/v1/*

---

### Deployment quick start

Use the same jar or image for all three roles:

| Profile | Responsibilities |
|:--------|:-----------------|
| `annotation-api` | REST reads and writes; publishes updates; no queue consumers. |
| `annotation-writer` | All enabled queue consumers; no REST API or direct API file fallback. |
| `annotation-combined` | API and all enabled queue consumers in one instance. |

Before starting a role, publish shared `annotation.yml` and its matching
`annotation-annotation-<role>.yml` overlay to Config Server. The files are supplied
in `service/src/main/config` and `docker/config`. Exactly one role profile and its
matching overlay marker are required. Keep `spring.application.name=annotation`.

```bash
java -jar service/target/annotation-service*-exec.jar \
  --spring.profiles.active=annotation-api,dev,remoteauth

java -jar service/target/annotation-service*-exec.jar \
  --spring.profiles.active=annotation-writer,dev,remoteauth
```

For a single instance, select `annotation-combined` explicitly. Put the role first
and environment/backend profiles afterward so deployment tuning can override its
defaults. The role and backend flags select messaging functions automatically;
remove explicit `spring.cloud.function.definition` and
`spring.cloud.stream.function.definition` settings from external configuration.

API instances need RabbitMQ routes, Accumulo for reads, and the identifier lookup
service. Writers need the configured output backend. All roles use local caches,
retain secured management endpoints, and can queue updates while writers are
stopped if the required durable routes exist.

For independent Compose pools, use the
[Docker split setup](../../../docker/README.md). The
[deployment reference](docs/deployment.md) covers backend flags, required routes,
health, discovery, scaling, file storage, and cutover.

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

#### Write acceptance

`AnnotationPublisher` reports broker-confirmed acceptance only when RabbitMQ
acknowledges the message and has not returned it as unroutable. The writer saves
queued annotations asynchronously. Concurrent identical requests share a send;
retries use separate confirmations. Publication failures feed the existing retry
and optional file-fallback policy.

Annotation bindings use `application/x-protobuf`. See the
[deployment reference](docs/deployment.md#write-acceptance-and-fallback) for
confirmation, retry, and storage requirements.

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
