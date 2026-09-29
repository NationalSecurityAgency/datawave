# Query Limit Enforcement

## Overview

This package contains the classes necessary for enforcing concurrent query limits for users and systems across a group of web servers. Query limit enforcement is done through the [QueryLimiter](QueryLimiter.java) class. Given a user, system, and query logic, it can determine if any of the following limits have been exceeded:

- The max allowed concurrent queries for the user.
- The max allowed concurrent queries of the query logic for the user.
- The max allowed concurrent queries for the system.
- The max allowed concurrent queries of the query logic for the system.

**Query limits are enforced if and only if all the following conditions are true:**
- `QueryLimiter.setup()` has been called.
- The `QueryLimiter` instance is configured with a non-null `QueryLimitConfiguration`.
- The `QueryLogicCache` and `QueryCountsCache` used by the QueryLimiter instance are both in a healthy state.

Information about active queries is tracked and managed in Zookeeper. The following information for each query is tracked:

- The query ID (required).
- The user who submitted the query (required).
- The system the query was submitted on (optional). Defaults to `EMPTY_SYSTEM_FROM` if a no system is provided.
- The query logic the query originated from (required).

## Configuration

The QueryLimiter bean is typically defined in the file [QueryLimiterFactory.xml](../../../../../../../../deploy/configuration/src/main/resources/datawave/query/QueryLimiterFactory.xml) within the datawave-ws-deploy-configuration module. At a minimum, the following beans must be configured:
- A single [QueryLimiter](QueryLimiter.java) instance. This acts as the entrypoint to the query limit feature.
- A single [QueryLimitConfiguration](QueryLimitConfiguration.java) instance. This contains the configured limits used by the `QueryLimiter` instance.
- A single [QueryHeartbeatCache](QueryHeartbeatCache.java) instance. This cache contains [QueryHeart](QueryHeartbeat.java) instances that maintain the presence of ephemeral nodes in Zookeeper for active queries.

See the [QueryLimiterFactory.xml](../../../../../../../../deploy/configuration/src/main/resources/datawave/query/QueryLimiterFactory.xml) file for examples on how to configure the beans.

Limits may be defined and customized on a per-user and per-system basis. They also may be defined for groups of query logics. On a platform-wide basis, the following may be configured:
- The default concurrent user query limit. This is the total concurrent queries a user may have running across all systems. May be overridden per user.
- The default concurrent system query limit. Primarily to avoid a system getting overloaded. If the value is set to a negative limit, no limit will be enforced by default on systemsMay be overridden per system.

Custom limits for users, systems, and query logic groups are created by defining instances of the following classes and referencing them within the QueryLimitConfiguration bean:
- [UserLimitConfiguration](UserLimitConfiguration.java) - supports specifying:
  - The user DN.
  - The user's concurrent query limit. Overrides the default limit.
  - The user's concurrent query limit for different query logic groups. Overrides the default limits for the groups. Regex matching against group names is supported. Pattern uniqueness per user is enforced.
- [SystemLimitConfiguration](SystemLimitConfiguration.java) - supports specifying:
  - The system supplied via the `systemFrom` query parameter. Regex matching is supported, pattern uniqueness is enforced.
  - The systems' concurrent query limit. Overrides the default value. A negative value implies no limit.
  - The systems' concurrent query limit for different query logic groups. Regex matching against group names is supported. Pattern uniqueness per system config is enforced.
- [QueryLogicGroupLimitConfiguration](QueryLogicGroupLimitConfiguration.java) - supports specifying:
  - The group name.
  - The query logics included in the group. Regex matching is supported. Pattern uniqueness is enforced.
  - The default concurrent user query limit. This applies to the total concurrent queries a user may run that originate from a query logic in the group across all systems.

When using regex patterns in the configurations above, there is the possibility for exact matches, partial regex matches, and wildcard regex matches. The determination of the best limit to use for any particular system, query logic, or query logic group is done by sorting matches into the following 'matching buckets' (in best-match priority):

1. Exact match: We attempt to find an exact match first and use the associated limit.
2. Partial regex (non-wildcard-only): If we cannot find an exact match, then we attempt to find all partial matches, and see if any of their limits are met, checking against the lowest limits first.
3. Wildcard-only regex: In the case of no exact or partial matches, we use the wildcard match with the lowest limit.

## Implementation

Checking limits and marking as active/inactive is done through the [QueryLimiter](QueryLimiter.java) class. The three main methods for interacting with the query limit feature are:
- `QueryLimiter.checkForLimits()`: Will check if any limits will be met if a new query is created with the given user, query logic, and the current system.
- `QueryLimiter.countQueryTowardsLimits()`: Will mark a new query as active.
- `QueryLimiter.stopCountingQueryTowardsLimits()`: Will mark a query as no longer active.

When a query is marked as active via `QueryLimiter.countQueryTowardsLimits()`, it will delegate to the [ActiveQueryTracker](ActiveQueryTracker.java) class, which will in turn create nodes in Zookeeper under the namespace `ActiveQueries`. When `ActiveQueryTracker.trackQuery()` is called, the following nodes will be created:

```
# Container nodes (will be eligible for auto-cleanup by Zookeeper if they are empty)
/queries     # Ephemeral nodes tracking active queries will be stored under this node
/queryLogics # Distinct query logics seen in queries will be stored under this node

# An ephemeral node representing the active query. The data will be a byte array containing the user DN, system, and query logic of the query.
/queries>/<queryId> 
```

`ActiveQueryTracker.trackQuery()` will return a [QueryHeartbeat](QueryHeartbeat.java) instance that contain a `PersistentNode` wrapper around the ephemeral node listed above. The `QueryHeartbeat` will maintain the connection to Zookeeper and attempt to keep the ephemeral node present in Zookeeper until `QueryHeartbeat.stop()` is called. If `QueryHeartbeat.stop()` is called, or the webserver crashes, all ephemeral nodes will automatically be deleted by Zookeeper.

NOTE: Only one `QueryHeartbeat` can be active per query ID for the Zookeeper server. This is enforced by acquiring an InterProcessMutex lock at the path `/locks/<queryId>` under the namespace `ActiveQueries` when tracking a new query. 

NOTE: When a null or blank system is provided to the `QueryLimiter`, the default system `EMPTY_SYSTEM_FROM` will be used instead.

## Caching Layer

The query limiter feature maintains local in-memory caches reflecting the distinct query logics and the total query counts for active queries within two classes:
- [QueryLogicCache](QueryLogicCache.java) - tracks the distinct query logics seen for all active queries
- [QueryCountsCache](QueryCountsCache.java) - tracks the total concurrent active queries for users and systems 

These classes both use a backing CuratorCache to listen for node creation/deletion events, and upon receiving them, updates local in-memory collections that are used when checking limits. Both caches also register listeners for connection state changes to their backing Zookeeper client. If the connection enters a state of SUSPENDED, LOST, or READ_ONLY, the caches will be marked unhealthy, and **query limits will not be enforced**. If the connection enters a state of RECONNECTED, a rebuild of the in-memory collections will be triggered to attempt to put the caches back into a healthy state that reflects the latest information.

The [QueryHeartbeatCache](QueryHeartbeatCache.java) will also register a connection state listener to the backing Zookeeper client used to create `QueryHeartbeat` instances. This listener will listen for connection losses and update `QueryHeartbeat.clientConnected` with the status of the client. If `QueryHeartbeat.shutdown()` is called and the client is not considered connected, `QueryHeartbeat.stop()` will not be called on the heartbeats stored in the cache before invalidating them. This avoids a slowdown in shutting down the QueryHeartbeatCache when the backing Zookeeper client may retry multiple times to connect to Zookeeper before giving up.

## HTTP Codes

The following HTTP status codes have been added for responses from the webserver:
```
412-20  - Concurrent query limit exceeded
500-164 - Error checking concurrent query limits
```
