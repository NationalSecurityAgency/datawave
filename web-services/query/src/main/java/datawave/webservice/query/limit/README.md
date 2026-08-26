# Query Limit Enforcement

## Overview

This package contains the classes necessary for enforcing concurrent query limits for users and systems across a group of web servers. Query limit enforcement is done through the [QueryLimiter](QueryLimiter.java) class. Given a user, system, and query logic, it can determine if any of the following limits have been exceeded:

- The max allowed concurrent queries for the user.
- The max allowed concurrent queries of the query logic for the user.
- The max allowed concurrent queries for the system.
- The max allowed concurrent queries of the query logic for the system.

Information about active queries is tracked and managed in Zookeeper where the following information for each query is tracked:

- The query ID.
- The user who submitted the query.
- The system the query was submitted on.
- The query logic the query originated from.

## Configuration

The QueryLimiter bean is typically defined in the file [QueryLimiterFactory.xml](../../../../../../../../deploy/configuration/src/main/resources/datawave/query/QueryLimiterFactory.xml) within the datawave-ws-deploy-configuration module. At a minimum, the following beans must be configured:
- A single [QueryLimiter](QueryLimiter.java) instance. This acts as the entrypoint to the query limit feature.
- A single [QueryLimitConfiguration](QueryLimitConfiguration.java) instance. This contains the configured limits used by the `QueryLimiter` instance.
- A single [QueryHeartbeatCache](QueryHeartbeatCache.java) instance. This cache contains and maintains connnections to Zookeeper for active queries.

Limits may be defined and customized on a per-user and per-system basis. They also may be defined for groups of query logics. On a platform-wide basis, the following may be configured:
- The default concurrent user query limit. This is the total concurrent queries a user may have running across all systems. May be overridden per user.
- The default concurrent system query limit. Primarily to avoid a system getting overloaded. If the value is set to a negative limit, no limit will be enforced by default on systemsMay be overridden per system.

Custom limits for users, systems, and query logic groups are created by defining instances of the following classes and referencing them within the QueryLimitConfiguration bean:
- [UserLimitConfiguration](UserLimitConfiguration.java) - supports specifying:
  - The user DN
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

## Dynamic Configuration Updates

The `QueryLimitConfiguration` for the `QueryLimiter` may be updated dynamically through Zookeeper. When the `QueryLimiter` is configured with a [ZkObjectPublisher](../../../zookeeper/ZkObjectPublisher.java), it will subscribe to updates from that publisher. When the publisher receives a triggering event, it will attempt to load a new `QueryLimitConfiguration` from the configured file. See the [ZkObjectPublisher README](../../../zookeeper/README.md) for more details. 

## Implementation

Checking limits and marking as active/inactive is done through the [QueryLimiter](QueryLimiter.java) class. The three main methods for interacting with the query limit feature are:
- `QueryLimiter.checkForLimits()`: Will check if any limits will be met if a new query is created with the given user, query logic, and the current system.
- `QueryLimiter.countQueryTowardsLimits()`: Will mark a new query as active.
- `QueryLimiter.stopCountingQueryTowardsLimits()`: Will mark a query as no longer active.

When a query is marked as active via `QueryLimiter.countQueryTowardsLimits()`, it will delegate to the [ActiveQueryTracker](ActiveQueryTracker.java) class, which will in turn create nodes in Zookeeper under the namespace `ActiveQueries`. When `ActiveQueryTracker.trackQuery()` is called, the following nodes will be created:

```
# Container nodes (will be eligible for auto-cleanup by Zookeeper if they are empty)
/users/<userDn>/<queryLogic> # Only for queries on systems that count towards the user limit.
/systems/<system>/<queryLogic>
/distinctQueryLogics/<queryLogic> # Only created if it does not exist. The root node /distinctQueryLogics 
    # will be a container node. The individual queryLogic children will be persistent nodes.

# Ephemeral nodes. These will auto-delete themselves if their associated Zookeeper connection ever goes down.
/users/<userDn>/<queryLogic>/<queryId> # Only for queries on systems that count towards user limit
/systems/<system>/<queryLogic>/<queryId>
```

`ActiveQueryTracker.trackQuery()` will return a [QueryHeartbeat](QueryHeartbeat.java) instance that contain a list of `PersistentNode` (provided by the Apache Curator library) wrappers around the ephemeral nodes listed above. The `QueryHeartbeat` will maintain the connection  to Zookeeper and attempt to keep the ephemeral nodes present in Zookeeper until `QueryHeartbeat.stop()` is called. If `QueryHeartbeat.stop()` is called, or the webserver crashes, the ephemeral nodes will automatically be deleted by Zookeeper.

The following HTTP status codes are available for responses from the webserver:
```
412-20  - Concurrent query limit exceeded
500-164 - Error checking concurrent query limits
```
