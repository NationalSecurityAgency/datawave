# Annotation Cache Service

## Purpose

This service provides the regional Hazelcast cache used by Sonicweb. Annotation entries are immutable and are keyed by annotation ID inside a per-document map:

```text
annotations:<idType>:<documentId>
    annotationId -> AnnotationMessage

doc-fetch-record:<idType>:<documentId>
    authorizationHash -> FetchRecord
```

There is no separate document-to-annotation-ID index. A document's annotation map is the source of truth for its cached annotations, and Sonicweb enumerates the map values when retrieving them.

## Write and federation model

A local Sonicweb write is placed in Hazelcast and published by `AnnotationMapStore` to RabbitMQ. RabbitMQ delivery is acknowledged before the write is considered accepted by this service; Accumulo persistence and regional federation are asynchronous consumers.

Federated messages are consumed locally only when their `region.id` differs from the configured `region.name`. The message must also contain `id.type`, because `Annotation` contains the document ID but not the Sonicweb identifier type.

Federated entries use `putTransient()` so they are not republished by the local `MapStore`. Duplicate delivery is expected and is safe because annotations are immutable and keyed by annotation ID.

Messages use these shared `AnnotationMessage.parameters` entries:

```text
id.type   - identifier type used in the Hazelcast document map name
region.id - region in which the message originated
```

## Fetch freshness

Fetch records are transient Sonicweb freshness metadata, stored separately from annotation values. They expire using the configured fetch TTL. Removing, expiring, or evicting annotation entries clears fetch records for that document so the next Sonicweb read can consult permanent storage.

After partition loss, member removal, or a split-brain merge event, `AnnotationCacheTopologyListener` clears tracked fetch maps once the cluster has settled. Annotation entries themselves are the complete read set, so there is no derived index to rebuild or reconcile.

## Configuration

```yaml
annotation-cache:
  max-cache-age: 1h
  max-fetch-age: 5m
  topology-monitoring-enabled: true
  topology-settle-delay: 15s
  topology-poll-interval-ms: 5000
```

Annotation and fetch TTLs are validated at startup. Annotation TTL must be greater than or equal to fetch TTL.

## Assumptions and guarantees

- Annotation IDs identify immutable annotation content.
- RabbitMQ delivery and federation are at-least-once; persistence consumers must tolerate duplicates.
- A message originating in the local region is already present locally and is ignored by the federated consumer.
- `region.name` is configured consistently for each deployment.
- `id.type` and `region.id` are present on newly produced messages.
- Visibility and authorization decisions remain the responsibility of Sonicweb; this service does not authorize annotations.
- Publisher confirmation means broker acceptance, not Accumulo persistence or federation completion.

## Consistency notes

- Annotation and fetch-record changes are not one atomic transaction.
- Hazelcast entry listeners are asynchronous. Fetch invalidation after annotation removal can have a short delay; it is also explicitly performed by Sonicweb during local cache clearing.
- A topology event may cause harmless extra backend reads because fetch records are discarded.
- Per-document annotation and fetch maps remain distributed objects after their entries are cleared unless explicitly destroyed.
