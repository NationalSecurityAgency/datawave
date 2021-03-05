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


