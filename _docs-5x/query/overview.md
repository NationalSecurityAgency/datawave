---
title: DataWave Query Overview
tags: [getting_started, architecture, query]
devtags: [todo]
toc: false
---

## JEXL / Iterator Framework

The design of DataWave Query is predicated in large part on [Java Expression Language (JEXL)][jexl], which serves two
key roles within the query framework. First, JEXL is used as the basis for DataWave's [query language](syntax). Thus, it
plays a central role within each client request to convey the user's search criteria. Secondly, JEXL is utilized within the
framework's internals to drive query evaluation against DataWave's [Accumulo tables](../getting-started/data-model). More specifically,
the framework leverages JEXL libraries to harness a variety of custom and stock [Accumulo Iterators][acc_iterators] to facilitate
both query evaluation and object retrieval.

## Query Logics

The functionality and behavior of DataWave's query API is highly customizable, through a variety of client- and server-side
configuration options and through direct extension of software. Software extension within the API is typically accomplished
through the implementation of new [query logic](development#query-logic-components) components. A query logic in this context
is simply a loosely-coupled Java class that leverages the JEXL/Iterator framework described above to support a specific type
of query. DataWave provides several query logic implementations that will satisfy the basic needs of most users.

## Web Services

Query logics are typically instantiated within DataWave's web tier through dependency injection, using standard IoC
frameworks such as Spring and CDI, and they are typically exposed to users through REST services. DataWave's REST services
are designed to support a large number of concurrent users and may be deployed to one or more application servers as needed
for scalability. DataWave's query services support a variety of client-side strategies for retrieving query results,
and they support a variety of response formats for serializing those results, such as XML, JSON, Protocol Buffer, and
other formats.

{% include devtag_todo.html content="Add graphic depicting the above overview" %}

[acc_iterators]: https://accumulo.apache.org/1.9/accumulo_user_manual.html#_iterators
[jexl]: http://commons.apache.org/proper/commons-jexl/