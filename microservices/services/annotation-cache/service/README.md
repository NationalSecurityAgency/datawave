# Annotation Cache Service

The service caches annotations in Hazelcast. It also records successful Datawave lookups so Sonicweb can avoid repeating them until the record expires.

## Shared maps

| Map | Key | Value | Entry TTL |
|---|---|---|---|
| `annotations` | `AnnotationKey(idType, documentId, annotationId)` | `AnnotationMessage` | `annotation-cache.max-cache-age` (default `1h`) |
| `doc-fetch-record` | `FetchKey(idType, documentId, authorizationHash)` | `FetchRecord(fetchedAt, annotationCountReturned)` | `annotation-cache.max-fetch-age` (default `5m`) |

Keys are immutable composite keys. See the [API README](../api/README.md) for map constants, serialization, and compatibility requirements.

Both maps have HASH indexes on `(__key.idType, __key.documentId)` and on `__key.documentId`. These support exact document-and-type lookups and document-ID lookups across identifier types.

The map names do not depend on document IDs. Entry count and memory use still depend on traffic and retention. Sonicweb also uses a separate `annotation-cache` map for health checks.

## Writes and federation

For local write-through entries, Hazelcast calls `AnnotationMapStore` to publish to RabbitMQ. The map write waits for a broker ACK and fails if publication fails, is returned or rejected, or times out. Local write-through messages must include `region.id` and `id.type`.

A broker ACK confirms acceptance, not downstream processing or permanent storage. Surviving a broker restart also requires persistent message delivery to a durable queue. The service declares the durable `annotation` topic exchange, durable `annotation.cache` queue, and a `#` binding.

Cache-only and remote-origin entries are not republished. The federated consumer ignores messages from the local region and requires `region.id` and `id.type` on remote messages. It inserts each annotation under a distributed lock, using `putTransient()` to bypass the MapStore and prevent publication loops. Duplicate deliveries leave existing entries unchanged. This assumes immutable annotation IDs.

## Fetch markers and freshness

A fetch marker records a successful Datawave lookup for one authorization context. It does not guarantee that the annotation cache contains a complete snapshot. Its presence and TTL determine freshness; `FetchRecord` fields are diagnostic only.

Sonicweb writes a marker after source retrieval and cache merge succeed. Empty and not-found responses also receive markers. Errors do not. Cache hits do not renew markers, and expired markers cause a source lookup on the next request, not a background refresh. Because markers are written after retrieval, source latency adds to the age of the source observation.

Annotation removal, expiration, and eviction invalidate markers for the matching `(idType, documentId)`. Clearing or evicting the annotation map clears all markers. Member loss, partition loss, failed replica migration, and split-brain merge events request a delayed marker clear after the cluster settles and is safe.

Invalidation is asynchronous. Concurrent requests may recreate markers, and missed invalidations are bounded by marker TTL. Clear operations are not barriers against concurrent writes.

## Sonicweb reads and clears

With result caching enabled, Sonicweb checks the caller's authorization-specific marker before querying Datawave. After a source lookup, it merges annotations with `putIfAbsent`, so existing cached IDs take precedence, then writes the marker. It returns source results combined with cached document entries, even if concurrent cache loss removes entries during the request. Visibility checks and cache-hit auditing still apply.

When `sonicweb.annotation-cache.cache-datawave-results=false`, every read queries Datawave. Sonicweb ignores cache-only entries and combines source results with cached write-through entries. Markers do not suppress source reads, and write-through persistence is unchanged.

Clients can clear all entries, one `(idType, documentId)`, or one document ID across identifier types. Scoped clears use indexed exact predicates.

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

Both TTLs must be positive whole seconds, and `max-fetch-age` must not exceed `max-cache-age`. Max-idle expiration is disabled. Federation lock wait and topology poll interval must be positive; topology settle delay may be zero.

RabbitMQ bindings and connection settings are supplied separately through application configuration. Bind `persisted-out-0` and `loadCache-in-0` to `annotation`, use consumer group `cache`, and set the content type to `application/x-protobuf`. Publication requires correlated confirms and returns. The companion Helm configuration is `configuration/configMapFiles/annotation-cache.yml` in the `datawave-helm-charts` repository.

## Build and tests

From the Datawave repository root, build and install the API and service:

```bash
mvn -f microservices/services/annotation-cache/pom.xml -pl service -am clean install
```

Then, from the Sonicweb repository root:

```bash
mvn -pl service -am clean verify
```

Sonicweb uses `gov.nsa.datawave.microservice:annotation-cache-api:1.0.0-SNAPSHOT`, so install the API first. The aggregate `services` profile includes annotation-cache; its service module is included unless `onlyServiceApis` is set.

Server tests cover serialization, indexed queries, expiration, map events, and topology recovery using isolated Hazelcast instances. Sonicweb tests cover client repository behavior. For opt-in raw Hazelcast measurements, see the [scaling guide](scaling-harness.md).

### Member/client compatibility check

From `microservices/services/annotation-cache`, run:

```bash
SONICWEB_ROOT=/path/to/sonicweb ./verify-supported-hazelcast-pairing.sh
```

The runner starts separate member and client JVMs using each application's resolved runtime dependencies. It reports their Hazelcast versions and checks key equality, reads, `putIfAbsent`, removal, locks, and both query shapes without custom serializer registration. Rerun it when either application's dependencies change. The last documented pairing was Hazelcast `5.1.2` member / `5.1.7` client; these are not required versions.

The runner requires Bash, Maven, Java, GNU `timeout`, and access to both applications' dependencies. It rebuilds and installs the server, compiles Sonicweb, and disables Maven's build cache. It does not run Sonicweb's full verification suite; retain the separate `clean verify` command above.

| Environment variable | Default | Purpose |
|---|---|---|
| `SONICWEB_ROOT` | `/app/sonicweb` | Sonicweb repository root. |
| `ANNOTATION_CACHE_PAIRING_PORT` | `57991` | Loopback member port; choose an unused port for concurrent runs. |
