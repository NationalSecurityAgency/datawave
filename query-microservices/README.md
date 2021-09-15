# DATAWAVE Query Microservices

This module contains a collection of query microservices which are intended tobe a replacement for our current 
monolithic Wildfly webapp.  

What follows is an evolving description of the core components of which DATAWAVE query will be comprised.  

## Query State Storage
This will store the current state of the query such that any instance of a query logic execution service can pick up a 
query where it left off.  This will store the query, the list of ranges with query plans, and the last range+result that
was placed on the results queue.

## Results Queue
This is a (maybe persistent) queue per query on which results will be dropped into.

## Query Lock Storage
This is a mechanism by which the query execution services can control which instance is actually handling a query.

## Query API/Handler Service
This service will accept the existing create/next/close client calls and will store the request in a Query State 
storage.  This service will pull results from the results queue to return pages to the client.

## Query Logic X Execution Service
This service will handle creating query plans and pulling results for a query.  Query ranges+plans will be stored in the
Query State storage.  The results will be placed on a query specific results Results queue.  


3/5/2021:
Meeting with Ivan and Whitney discussing some of the design points

Query flow:
   1) user sends in create
      a) audits
      b) start metrics
      c) rest service stores the query which sends a task notification
   2) executor service (for that pool and query logic)
      a) receives create task notification
      b) does the query logic create to generate the ranges
      c) starts storing ranges in a checkpoint
      d) sends out N (equal to max semaphore) task notifications to start processing ranges
      e) sends notification to rest service as well ?
   3) rest service responds to user with query id
      a) updates metrics?
   4) executor services
      a) receives range/next task notification
      b) determines that he can grab a lock and there are not too many results in the result queue
         i) task is simply dropped cannot be processed
      c) processes range
      d) puts results in result queue
      e) generates 1 task notification to process next range
   5) user sends in a next
      a) rest service checks if executors are working on results and there are ranges remaining to be processed
        i) rest service sends out range/next task notifications if needed
      b) pulls results into a page
      c) updates metrics?

## Existing External Interfaces

|Covered?|Existing Class|Base Path URI|Keep or Drop|New Class|Service|Notes|
|:-------|:-------------|:------------|:-----------|:--------|:------|:----|
|Yes|AuditBean|/Common/Auditor|Keep|AuditController|Audit Service|Add remote auth profile and add a route in HAProxy.|
|Yes|QueryExecutorBean|/Query|Keep|QueryController|Query Service|Most endpoints will be preserved - some won't.|
|No|CachedResultsBean|/CachedResults|Keep|?|?|Should this live in the query service, or somewhere else?|
|Yes|TableAdminBean|/Accumulo|Keep|AdminController|Accumulo Service|Looks like this bean forwards to the accumulo service, so we just need to make sure to add the remote auth profile and add a route in HAProxy.|
|Yes|StatsBean|/Accumulo|Keep|StatsController|Accumulo Service|Looks like this bean forwards to the accumulo service, so we just need to make sure to add the remote auth profile and add a route in HAProxy.|
|Yes|LookupBean|/Accumulo|Keep|LookupController|Accumulo Service|Looks like this bean forwards to the accumulo service, so we just need to make sure to add the remote auth profile and add a route in HAProxy.|
|Yes|PermissionsBean|/Accumulo|Keep|AdminController|Accumulo Service|Looks like this bean forwards to the accumulo service, so we just need to make sure to add the remote auth profile and add a route in HAProxy.|
|Yes|UpdateBean|/Accumulo|Keep|AdminController|Accumulo Service|Looks like this bean forwards to the accumulo service, so we just need to make sure to add the remote auth profile and add a route in HAProxy.|
|Yes|ListTablesBean|/Accumulo|Keep|AdminController|Accumulo Service|Looks like this bean forwards to the accumulo service, so we just need to make sure to add the remote auth profile and add a route in HAProxy.|
|Yes|ListUserAuthorizationsBean|/Accumulo|Keep|AdminController|Accumulo Service|Looks like this bean forwards to the accumulo service, so we just need to make sure to add the remote auth profile and add a route in HAProxy.|
|Yes|ListUserPermissionsBean|/Accumulo|Keep|AdminController|Accumulo Service|Looks like this bean forwards to the accumulo service, so we just need to make sure to add the remote auth profile and add a route in HAProxy.|
|Yes|ListUsersBean|/Accumulo|Keep|AdminController|Accumulo Service|Looks like this bean forwards to the accumulo service, so we just need to make sure to add the remote auth profile and add a route in HAProxy.|
|Yes|DataDictionaryBean|/DataDictionary|Keep|DataDictionaryOperations|Dictionary Service|Looks like this bean forwards to the dictionary service, so we just need to make sure to add the remote auth profile and add a route in HAProxy.|
|Yes|EdgeDictionaryBean|/EdgeDictionary|Keep|EdgeDictionaryOperations|Dictionary Service|Looks like this bean forwards to the dictionary service, so we just need to make sure to add the remote auth profile and add a route in HAProxy.|
|No|AtomServiceBean|/Atom|?| | |Can we drop this?|
|Yes|ConfigurationBean|/Common/Configuration|Keep|*|*|All of the spring-boot microservices have refresh endpoints now, so we should be covered.|
|No|AccumuloTableCacheBean|/Common/AccumuloTableCache|Keep|QueryExecutorController|Executor Service|The executor service may need the ability to reload it's accumulo table cache.|
|Yes|AccumuloConnectionFactoryBean|/Common/AccumuloConnectionFactory|Keep|QueryExecutorController|Executor Service|Add remote auth profile and add a route in HAProxy.|
|No|HealthBean|/Common/Health|Keep|*|*|This provides the shutdown endpoint which is used to drain and shutdown the webservices.  This mechanism will likely differ with our new implementation, but it's a concept we should be aware of.|
|No|ModelBean|/Model|Keep|?|?|Where should model access/manipulation live?  In the query service?|
|No|ModificationBean|/Modification|?|?|?|Is this used anywhere?  Do we need this?  It allows us to list and modify modification services.|
|No|ModificationCacheBean|/Modification|?|?|?|Is this used anywhere?  Do we need this?  It allows us to list mutable fields and reload the mutable field cache.|
|No|QueryCacheBean|/Query/Cache|?|?|?|All queries are cached now.  What endpoints do we want to support for accessing our query cache and where should that live?|
|Yes|DashboardBean|/Query/Metrics/dashboard|Drop?| | |As far as I know, this is not used.  Seems droppable to me.|
|Yes|HudBean|/Query/queryhud|Drop?| | |As far as I know, this is not used.  Seems droppable to me.|
|Yes|QueryMetricsBean|/Query/Metrics|Keep|QueryMetricOperations|Query Metric Service|Just need to add a route in HAProxy for this.|
|No|BasicQueryBean|/BasicQuery|Drop?| | |Query wizard stuff.  Do we need this?  Does anyone use this?|
|No|IdTranslatorBean|/Query|?|?|?|Not sure what to do with this.|
|No|MapReduceStatusUpdateBean|/MapReduceStatus|?|?|?|Looks like this is a callback handler for BulkResults jobs which updates the internal state of the job.  Is this still needed?|
|No|CredentialsCacheBean|/Security/Admin/Credentials|Keep| | |We need a similar endpoint to enable us to list, flush and evict credentials from the cache.  |
|No|UserOperationsBase|/Security/User|Keep| | |We need a similar endpoint to enable users to list their effective authorizations, and flush their cached credentials.|
