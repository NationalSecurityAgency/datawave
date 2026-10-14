# Annotation Cache

## Purpose

Annotation Cache makes successfully added annotations immediately available to Sonicweb, publishing them to RabbitMQ for eventual persistence into Accumulo. It also supports sharing annotations across regions and reduces repeated Datawave lookups.

This is a transient cache and messaging bridge, **not the system of record**. Downstream persistence into Accumulo and RabbitMQ federation are deployed separately.

## Architecture

- **Sonicweb** reads and writes through Hazelcast using `HazelcastAnnotationRepository`. It owns Datawave lookups, result merging, per-user visibility checks, and cache-hit auditing. The cache service does not query Datawave or enforce per-user visibility.
- **`service`** runs Hazelcast members. [AnnotationCacheConfiguration][configuration] defines two fixed shared maps: annotations and authorization-specific source-lookup markers. Fixed maps avoid creating a map per document.
- **`api`** provides the shared [map contract][constants], [annotation keys][annotation-key], and [fetch keys][fetch-key]. Both members and clients must depend on a compatible API.
- **RabbitMQ** receives local write-through annotations through [AnnotationMapStore][store]; source results cached by Sonicweb are cache-only and are not published. [LoadCacheConsumer][consumer] caches remote annotations without republishing them, preventing federation loops. Duplicate deliveries preserve existing entries; annotation IDs are assumed to identify immutable annotations, not revisions.

## Design boundaries

### Availability and persistence

For local write-through annotations, [AnnotationMapStore][store] requires confirmed RabbitMQ acceptance or fails the map write. [RabbitTopologyConfig][rabbit] declares the durable `annotation` topic exchange and durable `annotation.cache` queue with a `#` binding. The topology survives broker restarts; retaining messages also requires persistent delivery.

**The guarantee ends at RabbitMQ acceptance.** This service defines no guarantees for downstream processing or persistence into Accumulo. A confirmation timeout fails the write without cancelling publication: RabbitMQ may already have accepted the message or accept it later. Retrying may publish duplicates; downstream consumers must handle them. Hazelcast writes and RabbitMQ publication are not an atomic transaction.

Choose annotation retention to cover expected Accumulo persistence and federation lag. An annotation that expires before Datawave can return it may temporarily disappear from reads. Cache retention cannot substitute for reliable persistence into Accumulo.

Cache clears do not delete annotations persisted in Accumulo, and concurrent writes can repopulate cleared entries.

### Freshness, not completeness

A fetch marker means **a recent successful Datawave lookup for an authorization context, not a complete annotation snapshot**. Annotations are shared across authorization contexts; markers do not replace per-user visibility checks. Sonicweb caches successful empty results too. Existing cached annotation IDs take precedence when source results are merged.

Marker expiry causes another source lookup on the next request; there is no background refresh, and cache hits do not renew freshness. Marker identity uses the proxy chain's common authorizations, not `sonicQueryParams`; changing query parameters alone does not force a lookup.

[AnnotationSyncListener][sync] and [AnnotationCacheTopologyListener][topology] invalidate markers when annotation data may have been lost. Invalidation is asynchronous and best-effort; marker TTL bounds missed invalidations, and clears are not barriers against concurrent requests.

Disabling Sonicweb result caching forces source lookups and ignores cache-only results, but preserves cached write-through annotations and their persistence path.

### Compatibility

Members and clients must use compatible Hazelcast and Java serialization settings. The key serialization proxies and [tuple encoding][encoding] are a wire contract; do not replace them with custom key serializers. Incompatible contract changes require coordinated deployment and rebuilding cached state, not an assumed-safe rolling upgrade.

## Configuration and verification

See [AnnotationCacheProperties][properties] for retention and recovery settings, and [AnnotationCacheConfiguration][configuration] for map configuration and validation. Configure `region.name`, Hazelcast connectivity, and RabbitMQ bindings externally. Publisher confirms and returns are required for the write-through acceptance guarantee.

Deployment configuration is maintained in `configuration/configMapFiles/annotation-cache.yml` in the `datawave-helm-charts` repository.

After API, JDK, Hazelcast, or serialization changes, run the [member/client compatibility check](verify-supported-hazelcast-pairing.sh) against the actual application dependencies, supplying `SONICWEB_ROOT` for your checkout. It supplements, rather than replaces, both applications' test suites. See the [scaling guide](service/scaling-harness.md) for opt-in measurements.

[configuration]: service/src/main/java/datawave/microservice/annotationCache/config/AnnotationCacheConfiguration.java
[properties]: service/src/main/java/datawave/microservice/annotationCache/config/AnnotationCacheProperties.java
[constants]: api/src/main/java/datawave/microservice/annotationCache/api/Constants.java
[store]: service/src/main/java/datawave/microservice/annotationCache/AnnotationMapStore.java
[consumer]: service/src/main/java/datawave/microservice/annotationCache/LoadCacheConsumer.java
[sync]: service/src/main/java/datawave/microservice/annotationCache/AnnotationSyncListener.java
[topology]: service/src/main/java/datawave/microservice/annotationCache/AnnotationCacheTopologyListener.java
[rabbit]: service/src/main/java/datawave/microservice/annotationCache/config/RabbitTopologyConfig.java
[annotation-key]: api/src/main/java/datawave/microservice/annotationCache/api/AnnotationKey.java
[fetch-key]: api/src/main/java/datawave/microservice/annotationCache/api/FetchKey.java
[encoding]: api/src/main/java/datawave/microservice/annotationCache/api/KeyTupleEncoding.java
