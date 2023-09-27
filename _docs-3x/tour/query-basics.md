---
title: "DataWave Tour: Query Basics"
layout: tour
tags: [getting_started, query]
summary: |
  The examples below will demonstrate usage of DataWave's Query API. In order to follow along in your own DataWave
  environment, you should first complete the <a href="../getting-started/quickstart-install">Quickstart Installation</a>
---

## A Simple Query Example

Here, we'll construct a simple query that uses the *GENRES* field from our **tvmaze** data type to find TV shows in the
action and adventure genres.

{% include data_dictionary_note.html %}

{% include tvmaze_note.html %}

### The Query Expression

DataWave accepts query expressions in either JEXL or Lucene [syntax](../query/syntax) as shown below.

<div class="row">
  <div class="col-md-6">
       <h4>Lucene</h4>
       <pre>GENRES:action OR GENRES:adv*</pre>
  </div>
  <div class="col-md-6">
       <h4>JEXL</h4>
       <pre>GENRES == 'action' || GENRES =~ 'adv.*'</pre>
  </div>
</div>

## Using the Query API

Most query examples in the guided tour will utilize the quickstart's **[datawaveQuery](../getting-started/quickstart-reference#datawave-web-functions)**
bash function. It provides a curl-based client that streamlines your interactions with the Query API and sets reasonable
defaults for most parameters. Query parameters can also be easily added and/or overridden.

{% include tip.html content="Use **datawaveQuery --help** for assistance" %}

To demonstrate Query API usage, each example will show key aspects of the required curl command and the associated web
service response 

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a class="noCrossRef" href="#create-query" data-toggle="tab"><b>1: Create the Query</b></a></li>
    <li><a class="noCrossRef" href="#get-results" data-toggle="tab"><b>2: Fetch Paged Results</b></a></li>
    <li><a class="noCrossRef" href="#close-query" data-toggle="tab"><b>3: Close the Query</b></a></li>
</ul>
<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="create-query" markdown="1">
### Step 1: Create the Query

DataWave/Query/{ query-logic }/create (*POST*)

Initializes server-side resources and responds with a unique ID for the query

```bash
 # Quickstart client command...

 $ datawaveQuery --verbose \                       # To output the actual curl command used
    --create-only \                                # For /create endpoint. Default is /createAndNext
    --logic EventQuery \                           # Query logic identifier for the path parameter
    --syntax LUCENE \                              # To use JEXL: --syntax JEXL
    --query " GENRES:action OR GENRES:adv* "
 
 # Curl command (abbreviated)...

 $ /usr/bin/curl -X POST https://localhost:8443/DataWave/Query/EventQuery/create \
 ... \ 
 -d pagesize=10 \                                  # Max results to return per page
 -d auths=BAR,FOO,PRIVATE,PUBLIC \                 # Accumulo auths to enable for user
 -d begin=19700101 -d end=20990101 \               # Date range filter
 -d queryName=Query_20180312121809 \               # Query name that's meaningful to user
 -d columnVisibility=BAR%26FOO \                   # Viz expression to use for query logging, etc.
 -d query=GENRES%3Aaction%20OR%20GENRES%3Aadv%2A \ # Query expression, URL-encoded
 -d query.syntax=LUCENE                            # Syntax identifier
 
 # Web service response...
 {
   "HasResults": true,
   "OperationTimeMS": 70,
   "Result": "758b6e03-4eb0-4923-b098-c161f0cb322d"
 }
 
```
<button type="button" class="btn" data-toggle="collapse" data-target="#details1">Step 1 - More Info</button>
<div id="details1" class="collapse" markdown="1">
To initialize our query, we invoked **DataWave/Query/{ query-logic }/create** using *EventQuery* for the
*query-logic* parameter...

* *EventQuery* is the main [query logic](../query/development#query-logic-components) for retrieving data objects
  from DataWave's [primary data table](../getting-started/data-model#primary-data-table). Semantically, the term *event*
  simply reflects the fact that these data objects are sorted and partitioned primarily by date. The date value assigned
  to a given object for this purpose is typically user-specified, per data type via ingest configuration
  
* Query logics are typically configured in the web service via Spring XML files such as [this one][dw_blob_qlf_xml]
  
* Alternatively, we could have used the **createAndNext** endpoint, which combines *Step 1* and *Step 2* into
  a single client request for convenience, automatically retrieving the first page of results, if any

* Our required parameters here were typical for most DataWave query types: the *query* expression to be evaluated, the *syntax*
  associated with that expression, the set of Accumulo *auths* to enable for data access, date range, etc. In practice, required
  and optional parameters for any given query may be specified by a combination of the endpoint being invoked, the query
  logic being used, and the REST service itself
</div>

</div>
<div role="tabpanel" class="tab-pane" id="get-results" markdown="1">
### Step 2: Fetch Paged Results

DataWave/Query/{ query-id }/next (*GET*)

Repeat this step until all pages have been returned, indicated by HTTP status code 204 

```bash
 # Quickstart client command...

 $ datawaveQuery --verbose --next 758b6e03-4eb0-4923-b098-c161f0cb322d

 # Curl command (abbreviated)...

 $ /usr/bin/curl ... \
   -X GET https://localhost:8443/DataWave/Query/758b6e03-4eb0-4923-b098-c161f0cb322d/next
   
 # Web service response (abbreviated)...
 
 {
   "Events": [
     # Record 1 of N... 
     {
       "Fields": [
          # Field name/value data for record 1 (omitted) 
       ],
       # Record-level security markings for record 1...
       "Markings": {  
         "entry": [
           { "key": "columnVisibility", "value": "PRIVATE|(BAR&FOO)" }
         ]
       },
       # Database metadata for record 1...
       "Metadata": {
         "DataType": "tvmaze",
         "InternalId": "-27cfzr.phrhgm.-jax0u1",
         "Row": "20180305_0",
         "Table": "shard"
       }
     },
     ... # Records 2 thru N omitted...
   ],
   # Listing of all field names returned by the query (most omitted here)
   "Fields": [
       ...
       "EXTERNALS_IMDB",
       "EXTERNALS_THETVDB",
       "EXTERNALS_TVRAGE",
       "GENRES",
       "ID",
       "LOAD_DATE",
       "NAME",
       "NETWORK_ID",
       "NETWORK_NAME",
       "OFFICIALSITE",
       "PREMIERED",
       "RECORD_ID",
       "RUNTIME",
       "SCHEDULE_TIME",
       "STATUS",
       "SUMMARY",
       ...
     ],
     "HasResults": true,
     "LogicName": "EventQuery",
     "OperationTimeMS": 229,
     "PageNumber": 1,
     "PartialResults": false,
     "QueryId": "758b6e03-4eb0-4923-b098-c161f0cb322d",
     "ReturnedEvents": N
 }

```
<button type="button" class="btn" data-toggle="collapse" data-target="#details2">Step 2 - More Info</button>
<div id="details2" class="collapse" markdown="1">
To retrieve results, we invoked **DataWave/Query/{ query-id }/next** with our query ID from *Step 1*...

* The **next** endpoint should have returned at least one page of results containing multiple TV shows, the size of which was bounded
by the **pagesize** parameter that we specified in *Step 1* (defaulted to 10 by the *datawaveQuery* client)

* Clients continue to invoke **next** with the given query ID until all results have been returned.

* If there are no more results to fetch, the **next** endpoint responds with HTTP status code **204**
</div>
  
</div>
<div role="tabpanel" class="tab-pane" id="close-query" markdown="1">
### Step 3: Close the Query

DataWave/Query/{ query-id }/close (*PUT*)

Release any server-side resources (*datawaveQuery* client may have already done this for you automatically)

```bash
 # Quickstart client command...
 
 $ datawaveQuery --verbose --close 758b6e03-4eb0-4923-b098-c161f0cb322d
 
 # Curl command (abbreviated)...
 
 $ /usr/bin/curl ... \
   -X PUT https://localhost:8443/DataWave/Query/758b6e03-4eb0-4923-b098-c161f0cb322d/close
   
 # Web service response...
 
 <?xml version="1.0"?>
 <html>
   <head>
     <meta http-equiv="content-type" content="text/html; charset=UTF-8"/>
     <title>DATAWAVE - Void Response</title>
     <link rel="stylesheet" type="text/css" href="/screen.css" media="screen"/>
   </head>
   <body>
     <h1>datawave.webservice.result.VoidResponse</h1>
     <div>
        <b>MESSAGES:</b><br/>
        758b6e03-4eb0-4923-b098-c161f0cb322d closed.<br/>
        <b>EXCEPTIONS:</b><br/>
     </div>
   </body>
 </html>
 
```
<button type="button" class="btn" data-toggle="collapse" data-target="#details3">Step 3 - More Info</button>
<div id="details3" class="collapse" markdown="1">
To release server-side resources, we invoked **DataWave/Query/{ query-id }/close** with our query ID from *Step 1*...

* Query clients should always invoke the *close* endpoint to release server-side resources when no further interaction
  with the query is needed

* Production query clients should be designed to automatically invoke *close* when *next* has no more results (as
  indicated by HTTP status code *204*), and also when the client encounters an unrecoverable error anytime after
  query creation

* Queries will also be closed automatically at the server after a configured period of idle time
</div>

</div>
</div>

[dw_blob_qlf_xml]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/web-services/deploy/configuration/src/main/resources/datawave/query/QueryLogicFactory.xml#L420