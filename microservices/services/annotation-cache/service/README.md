# Annotation Cache Service

## Purpose

This service provides a regional Hazelcast cache for annotation data. Annotation entries are immutable and are keyed by annotation ID inside a per-document map:

```text
annotations:<idType>:<documentId>
    annotationId -> AnnotationMessage

doc-fetch-record:<idType>:<documentId>
    authorizationHash -> FetchRecord
```

There is no separate document-to-annotation-ID index. A document's annotation map is the source of truth for its cached annotations, and consumers enumerate its values when retrieving them.

## Write and federation model

For a local write-through, Hazelcast invokes `AnnotationMapStore`, which publishes to RabbitMQ and waits for a publisher ACK before returning. A successful ACK marks the write accepted by this service, but confirms broker acceptance only—not downstream processing or persistence. Surviving a broker restart also depends on persistent message delivery to a durable queue. Cache-only and remote-origin entries are not republished by the MapStore.

`RabbitTopologyConfig` declares a durable topic exchange (`annotation`), a durable queue (`annotation.cache`), and a `#` binding that routes all messages from the exchange to that queue. These durable declarations preserve the topology across broker restarts; they do not, on their own, guarantee message survival or consumer processing. Surviving a restart also requires persistent message delivery, and processing depends on consumer behavior.

Federated messages are consumed locally only when their `region.id` differs from the configured `region.name`. The message must also contain `id.type`, because `Annotation` contains the document ID but not the identifier type used to construct the per-document map name.

Federated entries use `putTransient()` so they are not republished by the local `MapStore`. Duplicate delivery is expected and is safe because annotations are immutable and keyed by annotation ID.

Messages use these shared `AnnotationMessage.parameters` entries:

```text
id.type   - identifier type used in the Hazelcast document map name
region.id - region in which the message originated
```

## Fetch freshness

Annotation maps hold the cached annotation payloads; their companion `doc-fetch-record:<idType>:<documentId>` maps hold freshness markers keyed by `authorizationHash`. A marker means the annotation set for that document and authorization context has been checked and may be treated as fresh until the marker expires. The marker contains no annotation data, so its validity depends on the associated annotation map still containing the complete set represented by that freshness decision.

Maintain the annotation map and its fetch records as one logical cache state: removing, expiring, evicting, or clearing annotation data invalidates the companion fetch records. Otherwise, a still-live marker could cause incomplete cached data to be treated as fresh. Fetch records expire by `max-fetch-age`, which must not exceed annotation `max-cache-age`; invalidation may cause an extra source fetch, but avoids trusting stale freshness metadata.

`AnnotationSyncListener` clears a document's fetch records after annotation removals, expirations, evictions, or map clears. After partition loss, member removal, failed replica migration, or a split-brain merge event, `AnnotationCacheTopologyListener` clears tracked fetch maps once the cluster has settled and is safe. These topology changes may leave annotation maps incomplete, so discarding freshness markers allows the cache to be repopulated rather than treated as complete. Annotation entries themselves are the complete read set, so there is no derived index to rebuild or reconcile.

## Configuration

```yaml
annotation-cache:
  max-cache-age: 1h
  max-fetch-age: 5m
  federation-lock-wait: 5s
  topology-monitoring-enabled: true
  topology-settle-delay: 15s
  topology-poll-interval-ms: 5000
```

Both TTLs must be positive whole seconds supported by Hazelcast, and annotation TTL must be greater than or equal to fetch TTL.

## Assumptions and guarantees

- Annotation IDs identify immutable annotation content.
- RabbitMQ delivery and federation are at-least-once; persistence consumers must tolerate duplicates.
- A message originating in the local region is already present locally and is ignored by the federated consumer.
- `region.name` is configured consistently for each deployment.
- `id.type` and `region.id` are present on newly produced messages; the MapStore rejects local write-through annotations missing either value before publishing.
- Authorization decisions remain the responsibility of calling applications; this service does not authorize annotations.
- A publisher confirm means broker acceptance, not Accumulo persistence or federation completion.

## Consistency notes

- Annotation and fetch-record changes are not one atomic transaction.
- Hazelcast entry listeners are asynchronous. Fetch invalidation after annotation removal can have a short delay; callers that clear annotation data should also clear the corresponding fetch records.
- A topology event may cause harmless extra backend reads because fetch records are discarded.
- Per-document annotation and fetch maps remain distributed objects after their entries are cleared unless explicitly destroyed.
