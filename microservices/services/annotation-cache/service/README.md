# Annotation Cache Service

## Purpose

This service provides the regional Hazelcast cache used by Sonicweb. Annotation entries are immutable and are keyed by annotation ID within a document-specific map:

```text
annotations:<idType>:<documentId>
    <annotationId> -> AnnotationMessage
```

`doc-annotations` is a derived document-to-annotation-ID index. Fetch records are transient Sonicweb freshness metadata.

## Write and federation model

A local Sonicweb write is written to Hazelcast and published by `AnnotationMapStore` to RabbitMQ. RabbitMQ delivery is acknowledged before the write is considered accepted by this service; Accumulo persistence and regional federation are asynchronous consumers.

Federated messages are consumed locally only when their `region.id` differs from the configured `region.name`. The message must also contain `id.type`, because `Annotation` contains the document ID but not the Sonicweb identifier type.

Federated entries use `putTransient()` so they are not republished by the local `MapStore`. Duplicate delivery is expected and is safe because annotations are immutable and indexed by annotation ID.

## Synchronization

`AnnotationSyncListener` maintains the derived index and invalidates fetch records when annotations are removed, evicted, or expired. `LoadCacheConsumer` also updates the index after federation, including when the annotation already exists, so it can repair a missed index event.

Index updates use an idempotent entry processor. The listener remains necessary for local writes and lifecycle events; consumer-side indexing is not a replacement for it.

## Reconciliation

`AnnotationCacheReconciler` periodically treats the `annotations:*` maps as authoritative and unions missing annotation IDs into `doc-annotations`. Reconciliation is intentionally add-only: stale IDs are measured and logged, while normal expiration/removal listeners remain responsible for deleting them. Repairing an index also invalidates that document's fetch records.

Only one Hazelcast member runs a reconciliation batch at a time. Work is bounded by `annotation-cache.reconciliation-max-maps-per-run`, and a distributed cursor allows later polls to finish the cycle. Reconciliation waits for the cluster to report a safe partition state.

Member additions/removals, split-brain merges, failed replica migrations, and partition loss request an additional delayed pass. Partition loss and merge events also request global fetch-record invalidation so Sonicweb can consult permanent storage. These event triggers are debounced and do not replace the periodic pass.

Configuration defaults:

```yaml
annotation-cache:
  reconciliation-enabled: true
  reconciliation-interval: 5m
  reconciliation-settle-delay: 15s
  reconciliation-max-maps-per-run: 1000
  reconciliation-poll-interval-ms: 5000
```

Sonicweb also performs targeted read repair when a document index is absent or empty while its annotation map contains entries.

## Assumptions

- Annotation IDs identify immutable annotation content.
- RabbitMQ delivery and federation are at-least-once; persistence consumers must tolerate duplicates.
- A message originating in the local region is already present locally and is ignored by the federated consumer.
- `region.name` is configured consistently for each deployment.
- `id.type` and `region.id` are present on all newly produced messages.
- Temporary index inconsistency is acceptable for the low-volume, human-scale workload.

## Limitations

- Annotation, index, and fetch-map changes are not one atomic transaction.
- Hazelcast entry listeners are asynchronous. A newly inserted annotation may briefly be absent from the index; an expired annotation may briefly remain indexed.
- Reconciliation is eventually consistent and add-only. It reports but does not remove stale index IDs, and a bounded cycle may require multiple polls.
- Listener failures or member restarts can leave derived state stale unless a later event or repair operation corrects it.
- A whole-map clear can race with concurrent additions.
- Consumer retries can partially process a batched message before a later annotation fails.
- `putTransient()` uses the annotation map's configured TTL/max-idle settings. Those settings must be configured consistently with Sonicweb cache expectations.

## Protections

- Federated inserts bypass local write-through to prevent RabbitMQ republishing loops.
- Per-annotation locking prevents concurrent duplicate federation work within the local map.
- Existing annotations are not overwritten.
- Index additions are set-based and idempotent.
- Index failures propagate to the message consumer so broker retry/dead-letter policy can handle them.
- Annotation validation rejects missing document IDs, annotation IDs, regions, and identifier types.
- Reconciliation rebuilds missing `doc-annotations` entries from the annotation maps and invalidates affected fetch records.
- Topology and partition-loss events trigger delayed reconciliation after the cluster becomes safe.
- Visibility and authorization decisions remain the responsibility of Sonicweb; this service does not authorize annotations.
