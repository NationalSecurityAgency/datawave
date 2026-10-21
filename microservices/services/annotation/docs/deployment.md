# Annotation deployment reference

Start with the [service quick start](../README.md#deployment-quick-start).
The same executable supports API, writer, and combined roles.

## Role and configuration selection

Activate exactly one of `api`, `writer`, or `combined`. Missing, conflicting, or
default-only role selection fails startup. Changing roles requires a restart.

These profiles replace `annotation-api`, `annotation-writer`, and
`annotation-combined`; the old names are not role aliases. Before deploying,
publish the renamed files below with their updated overlay markers and update
the active role profile in launch settings. Keep `spring.application.name=annotation`.
Consul and Compose service names remain `annotation-api` and `annotation-writer`.

Config Server combines application name `annotation` with the active profiles:

| File | Purpose |
|:-----|:--------|
| `annotation.yml` | Shared destinations, tables, TLS, authorization, backend flags, and tuning. |
| `annotation-api.yml` | Required API marker; broker acknowledgement enabled; direct fallback disabled by default. |
| `annotation-writer.yml` | Required writer marker; shared tuning remains inherited. |
| `annotation-combined.yml` | Required combined marker; shared tuning remains inherited. |

Each overlay declares `annotation.deployment.overlays.<profile>: true`. Keep the
marker in the matching Config Server overlay; putting it in packaged or shared
configuration defeats the missing-overlay check. Select the role first, followed
by environment/backend profiles. Timeout, retry, backend, and consumer concurrency
settings remain shared unless a later deployment profile changes them.

## Functions and backends

The service constructs its function list from the role, backend flags, and
`spring.cloud.bus.enabled`. API and combined always include `annotationSource`;
writer and combined include every enabled log, Accumulo, and dump consumer. An
API has no annotation consumers. A writer with no enabled consumer fails startup.
`busConsumer` is included when Spring Bus is enabled.

Explicit `spring.cloud.function.definition` and
`spring.cloud.stream.function.definition` properties are unsupported, including
empty strings. Remove them from shared or deployment configuration. Custom
subsets, extra functions, and function chains are not part of this service's
configuration contract.

| Backend flag | Effect |
|:-------------|:-------|
| `annotation.writers.accumulo.enabled` | Consume updates and persist them to truthmark tables. |
| `annotation.writers.log.enabled` | Consume updates and write log output. |
| `annotation.writers.dump.enabled` | Consume updates and write files. |
| `annotation.writers.file.enabled` | Direct API/combined file fallback after publication retries fail. |

Queue-backend flags accept only `true`/`false`, case-insensitively. Missing flags
mean disabled. Aliases such as `yes` or `1`, blanks, and surrounding whitespace
fail startup, even when another backend is enabled. Direct fallback is separate
from the dump consumer and is excluded from writer roles.

Messaging settings use the fixed bindings `annotationSource-out-0`,
`logAnnotationSink-in-0`, `accumuloAnnotationSink-in-0`, and
`dumpAnnotationSink-in-0`. Binding aliases may be absent or map a binding to itself;
renaming them fails startup. Configure destinations, stable groups, conversion,
and Rabbit settings under those names. `annotationSource` means message publisher;
it is separate from the provenance protobuf type and table with similar names.

## Required routes and health

The default persistence backend is Accumulo. Shared health configuration requires
the durable `annotation.accumulo` queue, its `#` binding to the `annotation`
exchange, and its dead-letter queue and route. Recovery can provision those routes
before writers start. Zero active consumers is healthy when the required durable
routes exist; the queue buffers updates until a writer resumes.

For another destination, group, or backend, supply matching health topology in a
later deployment profile. APIs must check every required persistence route;
writers must check their selected backends. `queues`, `exchanges`, and `bindings`
are replaced as complete lists, not appended. Enabling a backend does not update
those lists automatically. The dump binding uses `bindQueue: false`, so provision
its queue and routing rule explicitly.

Topology checks are periodic. A positive broker confirmation does not identify
which groups received copies, leaving a possible race when bindings change between
a health check and publication.

Aggregate health and `/annotation/mgmt/health/readiness` return HTTP 503 for
`RABBITMQ_UNHEALTHY`, including before the first topology check finishes. Readiness
includes all enabled indicators and application availability. Liveness at
`/annotation/mgmt/health/liveness` checks process availability without requiring
external dependencies. Use separate probes so a broker outage stops traffic
without restarting the process. Consul uses aggregate health. The status order is
`DOWN, OUT_OF_SERVICE, RABBITMQ_UNHEALTHY, UP, UNKNOWN`.

## Write acceptance and fallback

The Rabbit binder supplies `CorrelationData` confirmations through
`useConfirmHeader`; correlated publisher confirms and returns are enabled in
packaged, service, and Docker configuration. Acceptance requires a positive
confirmation and no returned message. No custom acknowledgement callback runs
on the secured REST controller. The writer persists accepted messages later.

Concurrent identical messages share an in-flight publication. Each physical send,
including a retry, has a unique correlation ID so a late confirmation cannot
confirm another attempt. Timeouts, negative confirmations, returns, and local
failures use the existing retry/backoff and optional fallback policy. Interrupted
waits preserve the thread's interrupt status. Explicitly disabling
`annotation.annotation-ack-enabled` accepts local enqueueing without broker
confirmation; the API overlay enables confirmation by default.

Direct API file fallback defaults off; a later profile can enable it. Combined
retains its configured fallback policy. File fallback needs persistent per-replica
storage and a replay procedure; dump consumers need persistent storage and unique
replica subpaths. Compose examples provide neither volumes nor a replay service.
Align retry budgets with client/proxy timeouts because fallback runs after retries.
Preserving an edit when a broker failure cannot be reported to the caller remains
a separate policy follow-up; its default is pending scenario validation.

## Dependencies, discovery, and cutover

All roles use bounded local Caffeine caches (1,000 entries per default cache,
five-minute expiry after write). Hazelcast and its health indicator are disabled.
Marking and federated-authorization caches retain their separate local policies.
Annotations do not use distributed cache state or locks. API reads still require
Accumulo and the configured identifier lookup service; file-only writers create
no Accumulo pool when that backend is disabled.

For split Compose deployments, run the command in the
[Docker README](../../../../docker/README.md). APIs register in Consul as
`annotation-api`; writers register as `annotation-writer`. REST clients discover
the API pool. Both keep application name `annotation` and HTTP context
`/annotation`. API host ports are assigned dynamically; use a load balancer for a
stable address. Writers expose management internally and publish no host port.
There is no legacy discovery alias because the split service is pre-production.

Provision durable queues and align protobuf content types before switching
traffic. Replicas within the stable `accumulo`, `log`, or `dump` group compete for
messages; different groups receive copies. Bring up writers, verify APIs, switch
REST traffic, then retire combined replicas. During overlap, consumers use the
same groups. Rollback restores combined mode with the same format and groups.

Tune API replicas by request load and read-connection budgets; tune writer replicas
and consumer concurrency by backlog and storage throughput. Confirmations return
to the originating API replica, so writes do not require sticky routing.
