# DATAWAVE Query Microservices

This module contains a collection of query microservices which are intended to be a replacement for our current 
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
|Yes|CachedResultsBean|/CachedResults|Keep|CachedResultsQueryController|CachedResultsQueryService|Functionality lives in starter - rest API is in query service.|
|Yes|TableAdminBean|/Accumulo|Keep|AdminController|Accumulo Service|Add remote auth profile and add a route in HAProxy.|
|Yes|StatsBean|/Accumulo|Keep|StatsController|Accumulo Service|Add remote auth profile and add a route in HAProxy.|
|Yes|LookupBean|/Accumulo|Keep|LookupController|Accumulo Service|Add remote auth profile and add a route in HAProxy.|
|Yes|PermissionsBean|/Accumulo|Keep|AdminController|Accumulo Service|Add remote auth profile and add a route in HAProxy.|
|Yes|UpdateBean|/Accumulo|Keep|AdminController|Accumulo Service|Add remote auth profile and add a route in HAProxy.|
|Yes|ListTablesBean|/Accumulo|Keep|AdminController|Accumulo Service|Add remote auth profile and add a route in HAProxy.|
|Yes|ListUserAuthorizationsBean|/Accumulo|Keep|AdminController|Accumulo Service|Add remote auth profile and add a route in HAProxy.|
|Yes|ListUserPermissionsBean|/Accumulo|Keep|AdminController|Accumulo Service|Add remote auth profile and add a route in HAProxy.|
|Yes|ListUsersBean|/Accumulo|Keep|AdminController|Accumulo Service|Add remote auth profile and add a route in HAProxy.|
|Yes|DataDictionaryBean|/DataDictionary|Keep|DataDictionaryOperations|Dictionary Service|Add remote auth profile and add a route in HAProxy.|
|Yes|EdgeDictionaryBean|/EdgeDictionary|Keep|EdgeDictionaryOperations|Dictionary Service|Add remote auth profile and add a route in HAProxy.|
|No|AtomServiceBean|/Atom|Shelve| | |Can we drop this?|
|Yes|ConfigurationBean|/Common/Configuration|Keep|*|*|All of the spring-boot microservices have refresh endpoints now, so we should be covered.|
|Yes|AccumuloTableCacheBean|/Common/AccumuloTableCache|Keep|QueryExecutorController|Executor Service|The executor service may need the ability to reload it's accumulo table cache.|
|Yes|AccumuloConnectionFactoryBean|/Common/AccumuloConnectionFactory|Keep|QueryExecutorController|Executor Service|Add remote auth profile and add a route in HAProxy.|
|Yes|HealthBean|/Common/Health|Keep|*|*|This will now be handled as part of the actuator health (i.e. mgmt/health) and shutdown (i.e. mgmt/shutdown) endpoints.  Shutdown prevents new requests while allowing existing requests to finish. |
|Yes|ModelBean|/Model|Keep|?|?|Where should model access/manipulation live?  In the query service?|
|Yes|ModificationBean|/Modification|Keep|?|?|INB - Is this used anywhere?  Do we need this?  It allows us to list and modify modification services.|
|Yes|ModificationCacheBean|/Modification|Keep|?|?|INB - Is this used anywhere?  Do we need this?  It allows us to list mutable fields and reload the mutable field cache.|
|No|QueryCacheBean|/Query/Cache|?|?|?|All queries are cached now.  What endpoints do we want to support for accessing our query cache and where should that live?|
|Yes|DashboardBean|/Query/Metrics/dashboard|Drop| | |As far as I know, this is not used.  Seems droppable to me.|
|Yes|HudBean|/Query/queryhud|Drop| | |As far as I know, this is not used.  Seems droppable to me.|
|Yes|QueryMetricsBean|/Query/Metrics|Keep|QueryMetricOperations|Query Metric Service|Just need to add a route in HAProxy for this.|
|No|BasicQueryBean|/BasicQuery|Shelve| | |Query wizard stuff.  Do we need this?  Does anyone use this?|
|Yes|IdTranslatorBean|/Query|Keep|QueryController|Query Service| |
|Yes|MapReduceBean|/MapReduce|?|?|?|JWO - Might still be needed?  Query Microservices might work as a substitute.  |
|Yes|MapReduceStatusUpdateBean|/MapReduceStatus|?|?|?|Looks like this is a callback handler for BulkResults jobs which updates the internal state of the job.  Is this still needed?|
|Yes|CredentialsCacheBean|/Security/Admin/Credentials|Keep|AuthorizationOperations|Authorization Service|Add remote auth profile and add a route in HAProxy.  The listAccumuloAuths and reloadAccumuloAuths endpoints will probably be left behind.  |
|Yes|UserOperationsBase|/Security/User|Keep|AuthorizationOperations|AuthorizationService|Add remote auth profile and add a route in HAProxy.  |
|Yes|web-services/deploy/docs/docs/index.html|index.html|Keep|  |  |Hosted as a static resource in the query service.  |
|Yes|web-services/deploy/docs/docs/query_help.html|query_help.html|Keep|  |  |Hosted as a static resource in the query service.  |
|Yes|Enunciate Docs| /doc |Keep|  |  |  |
