---
title: DataWave Overview
tags: [getting_started, architecture]
toc: false
---

## Introduction

The DataWave project provides a general purpose framework to facilitate persistence, indexing
and retrieval of both structured and unstructured textual objects. Central to DataWave's design is that it leverages
[Apache Accumulo][apache_accumulo] to implement a flexible [data model](data-model) and to implement [ingest](../ingest/overview)
and [query](../query/overview) components that are robust and scalable.

Common use cases for DataWave include...

* Data fusion across structured and unstructured datasets
* Construction and analysis of distributed graphs
* Multi-tenant data architectures, with tenants having distinct security requirements and access patterns

DataWave provides flexible and extensible data security features predicated on Accumulo's [security][cell_level_sec] model.
As a result, organizations can apply either coarse- or fine-grained access controls to their data, and they can easily
integrate DataWave query clients into existing security infrastructure.

## System Architecture

DataWave is written in Java and its ingest and query components provide extensible software frameworks, which developers can
easily customize. In terms of system architecture, DataWave's ingest and query services are loosely coupled and may be
configured to operate on distinct clusters, if needed. In the configuration shown below, ingest and query services are segregated
so that CPU, memory, and network resources within the data warehouse cluster may be prioritized for query processing.

{% include image.html file="dw-system-overview.png" alt="DataWave System Overview" %}

However, if resource contention between ingest processing and query processing is not an immediate concern, then DataWave
may be hosted on a single, shared environment. From the perspective of the services themselves, selecting one of these two deployment
strategies is a simple matter of changing a few config settings.

Regardless of the deployment strategy, the flow of data into and out of the system remains largely the same. Raw data
to be ingested may arrive in a staging area for pre-processing, if needed, or it may be written directly to HDFS. Once the
input data arrives in HDFS, DataWave will process it via MapReduce and ultimately write the MapReduce output to DataWave's Accumulo
[tables](data-model) in the data warehouse. Lastly, query clients utilize various web services exposed through DataWave's
REST API to retrieve data of interest from the warehouse Accumulo repository.

[apache_accumulo]: http://accumulo.apache.org/
[apache_hadoop]: http://hadoop.apache.org/
[cell_level_sec]: https://accumulo.apache.org/1.9/accumulo_user_manual.html#_security


