# Annotation Cache Scaling Harness

`AnnotationCacheScalingHarnessTest` measures indexed Hazelcast operations using isolated loopback members and clients. It applies the service's map configuration and verifies both HASH indexes on both data maps. Annotation values are cache-only protobuf messages, and the MapStore is mocked.

The harness does not start Sonicweb, Datawave, RabbitMQ, or an audit service. It measures Hazelcast operations, not application throughput. Members and clients share one test JVM, so JVM memory measurements include both roles.

The workload is opt-in and skipped by normal CI.

## Run

From the Datawave repository root, run this bounded smoke test:

```bash
mvn -Dmaven.build.cache.enabled=false \
  -f microservices/services/annotation-cache/pom.xml \
  -pl service -am \
  -Dtest=AnnotationCacheScalingHarnessTest \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Dannotation.cache.scaling=true \
  -Dannotation.cache.scaling.members=2 \
  -Dannotation.cache.scaling.clients=2 \
  -Dannotation.cache.scaling.uniqueDocs=30 \
  -Dannotation.cache.scaling.annotationsPerDoc=2 \
  -Dannotation.cache.scaling.authContextsPerDoc=2 \
  -Dannotation.cache.scaling.payloadBytes=64 \
  -Dannotation.cache.scaling.concurrency=2 \
  -Dannotation.cache.scaling.readPercent=80 \
  -Dannotation.cache.scaling.warmupMs=100 \
  -Dannotation.cache.scaling.measuredMs=300 \
  -Dannotation.cache.scaling.churnBatchRate=5 \
  -Dannotation.cache.scaling.churnBatchSize=3 \
  -Dannotation.cache.scaling.churnDurationMs=400 \
  -Dannotation.cache.scaling.annotationTtlSeconds=2 \
  -Dannotation.cache.scaling.fetchTtlSeconds=1 \
  -Dannotation.cache.scaling.settleMs=250 \
  -Dannotation.cache.scaling.seed=20251023 \
  -Dannotation.cache.scaling.output=/tmp/annotation-cache-scaling-smoke \
  test
```

This checks the harness, not performance. Its short timings and percentiles are not representative. Omit a parameter to use its default below. Add `-o` only if all Maven dependencies are already available locally.

## Parameters

All parameters use the `annotation.cache.scaling.` prefix and are validated before Hazelcast starts.

| Parameter | Default | Meaning |
|---|---:|---|
| `members` | 2 | Member count (1–8). |
| `clients` | 1 | Client count (1–8). Workers are distributed across clients. |
| `uniqueDocs` | 100 | Seeded document IDs. Each uses both `UUID` and `PAGE_ID`. |
| `annotationsPerDoc` | 2 | Annotations per document and identifier type. Zero creates an empty-result dataset. |
| `authContextsPerDoc` | 2 | Fetch markers per document; at least 1. |
| `payloadBytes` | 128 | ASCII padding bytes in each protobuf value's `benchmark-payload` parameter. |
| `concurrency` | 2 | Worker count (1–64), with no producer backlog. |
| `readPercent` | 80 | Read percentage in the mixed phase; remaining operations use `putIfAbsent`. |
| `warmupMs` | 500 | Warmup per operation phase; excluded from latency samples. |
| `measuredMs` | 1,000 | Measured duration per operation phase. |
| `clearPopulation` | -1 | Markers per global clear. `-1` uses `uniqueDocs × authContextsPerDoc`; 0 skips populated clears. Maximum: 250,000. |
| `clearIterations` | 5 | Measured populated clears (1–50). Total marker refill work is limited to 500,000 entries. |
| `churnBatchRate` | 2 | Fresh-document batches per second, including churn warmup. |
| `churnBatchSize` | 5 | Fresh documents per batch. |
| `churnDurationMs` | 2,000 | Measured churn duration. |
| `annotationTtlSeconds` | 6 | Annotation map and drift-entry TTL (1–86,400 seconds). |
| `fetchTtlSeconds` | 3 | Fetch map and drift-marker TTL (1–86,400 seconds); must not exceed annotation TTL. |
| `settleMs` | 1,000 | Wait after annotation TTL before checking old drift documents. |
| `seed` | 20251023 | ID and operation-selection seed. Thread scheduling can still vary. |
| `output` | `target/annotation-cache-scaling` | Output parent directory, absolute or relative to the Maven working directory. |

Fixture limits are 250,000 estimated live keys, 500,000 total marker refill entries, and 128 MiB of estimated annotation padding. These protect the test runner; they are not capacity guarantees. Plan capacity before increasing workloads. There are no latency or memory pass/fail thresholds.

## Measured phases

| Phase | Operation |
|---|---|
| `annotation-value-query-exact-pair` | `values(predicate)` for one `(idType, documentId)`. |
| `annotation-value-query-document-id-only` | `values(predicate)` for a document ID across both identifier types. |
| Annotation merge | `putIfAbsent`. |
| Document marker invalidation | Remove fetch markers for one document. |
| Document-ID annotation clear | Remove annotations across both identifier types. |
| Populated fetch-map clear | Clear a known nonempty marker fixture. |
| `mixed-read-write-annotation-values` | Exact-pair value reads and `putIfAbsent` writes. |
| Expired-document query | Read old documents after drift entries expire. |

Value-query timers include transfer, deserialization, and materialization of the full result collection. Setup, validation, and warmup are excluded from latency samples. Mixed-phase latency combines the reads and writes that actually ran; reports include separate operation counts.

### Populated clear measurements

Before each clear, the harness refills a marker-only fixture outside the clear timer and checks its size. After clearing, it checks that the map is empty. Fixture IDs have no annotation entries, so annotation-removal callbacks cannot invalidate them.

A positive `warmupMs` enables one unmeasured warmup clear; it does not set the number of clear iterations. Workers finish before this serial phase. Topology monitoring is disabled in the harness, so the measurement covers raw `IMap.clear()`, not topology-event recovery.

Reports separate refill/setup time, clear-only time, and full cycle time. Cycle throughput includes refill and validation. With few samples, individual clear timings are more useful than percentiles.

### Validation and edge cases

Checkpoints verify key and value counts, document and annotation identities, payload padding, and query isolation across documents and identifier types. Both index layouts must be present. Each read phase must also show its own indexed-query statistic increase where Hazelcast exposes it; the measurement includes that phase's warmup and timed operations.

| Case | Report behavior |
|---|---|
| `annotationsPerDoc=0` | Reads return no values; fetch markers remain. Verified payload bytes are zero. Synthetic writes use other document IDs. |
| Empty read with no indexed-query statistic increase | Allowed only for the zero-annotation workload when checkpoints verify empty results. Index configuration checks still apply. |
| Mixed phase executes no reads | Expected values per read is `-1`; query scope and read kind are `not-applicable`. No read-index evidence is required. |
| `readPercent=0` | Mixed latency measures writes only. |
| `clearPopulation=0` | Populated clear reports `empty-not-applicable` with zero samples. |

For nonempty datasets, pair reads return `annotationsPerDoc` values and document-ID reads return twice that count. Configured percentages and padding sizes are reported separately from actual operation counts and verified values. Do not reinterpret older empty-clear or key-only results as populated-clear or value-query measurements.

### Drift and expiration

The drift phase creates documents with annotations and markers, markers only (empty-source results), or no cache writes (disabled-cache behavior). It waits for annotation TTL plus `settleMs`, checks that old queries and markers are empty, verifies that only the two fixed data maps remain, and inserts another small batch. Memory snapshots surround drift, expiration, and reinsertion.

## Reports and interpretation

Each run creates a timestamped `run-...-seed-...` directory containing:

- `measurements.csv`: operation timings and counts.
- `populated-fetch-clear.csv`: fixture counts, setup time, clear time, and cycle time.
- `summary.json` and `summary.md`: workload, environment, validation, and measurement summaries.

Reports include errors, timeouts, latency sample counts, p50/p95/p99, throughput, index-query deltas, entry counts, map counts, memory estimates, Java/Hazelcast versions, JVM arguments, and workload settings.

Hazelcast heap-cost and index-memory values are estimates, not retained heap. No explicit garbage collection is requested, and index storage may not shrink immediately after expiration. TTL limits retention time, not memory independently of traffic rate.

Keep reports with their parameters and environment when comparing runs. Files under `target/` are generated output, not durable evidence.

## Small comparison series

Change one factor at a time and hold the remaining settings fixed:

| Comparison | Settings |
|---|---|
| Corpus growth | `uniqueDocs=100`, then `300`; `annotationsPerDoc=2`, `payloadBytes=64`. |
| Payload growth | `payloadBytes=64`, then `2048`; `uniqueDocs=100`, `annotationsPerDoc=2`. |
| Larger document results | `annotationsPerDoc=2`, then `16`; `uniqueDocs=100`, `payloadBytes=64`. |

Use the smoke command as a template, with longer intervals such as `measuredMs=2000`, `warmupMs=300`, and `churnDurationMs=3000`. Select TTLs that allow measured fixtures to survive their phases. Use a distinct output directory for each run.

Compare actual result counts, payload sizes, index deltas, sample counts, and environment alongside timings. Small, noisy runs need not show monotonic changes. Include setup, clear-fixture refill, expiration waits, and teardown when planning runtime and capacity.
