---
title: "DataWave Ingest API"
tags: [development, ingest]
devtags: [todo]
summary: This page describes the primary components of DataWave Ingest API
---

{% include devtag_todo.html content="Add concrete examples and guidance for the API constructs below" %}

### Input Formats & RecordReaders

Parse raw records ('events') from blocks to be passed as k,v pairs to the mapper, EventMapper

### EventMapper Class

The EventMapper class passes events to each configured DataTypeHandler implementation 

### DataTypeHandler Interface

DataTypeHandler instances process events using an IngestHelper to create mutations for a specific data type

### IngestHelper Interface

An IngestHelper implementation parses field names and field values from a single raw record, on behalf of the DataTypeHandler

### Normalizer Interface

Normalizer classes may be configured on a per-field basis to ensure that values for the field are formatted consistently
and can be sorted lexicographically
