---
title: "DataWave Tour: Edge Query"
layout: tour
tags: [getting_started, query]
summary: |
  The examples below will demonstrate usage of DataWave's Edge Query API. In order to follow along in your own DataWave
  environment, you should first complete the <a href="../getting-started/quickstart-install">Quickstart Installation</a>
---

## Edge Query Model

DataWave's [edge schema](../getting-started/data-model#edge-table) is generic and allows for several optional attributes
to be appended to the standard vertex pair. For this reason, DataWave allows the field names that are assigned to the
components of the edge schema to be overridden at web service build/deploy time, in order to satisfy local preferences.

For example, the default query model for DataWave's edge table is defined by the **edge.model.base.map** property...
```properties
# From DW_SOURCE/properties/default.properties...
...
# The keys in the map below are used internally by DataWave's query code. Their associated
# values denote the field names known to users and may be used in query expressions
edge.model.base.map= \
\n    <util:map id="baseFieldMap" key-type="java.lang.String" value-type="java.lang.String"> \
\n           <entry key="EDGE_SOURCE" value="SOURCE" /> \
\n           <entry key="EDGE_SINK" value="SINK"/> \
\n           <entry key="EDGE_TYPE" value="TYPE"/> \
\n           <entry key="EDGE_RELATIONSHIP" value="RELATION"/> \
\n           <entry key="EDGE_ATTRIBUTE1" value="ATTRIBUTE1"/> \
\n           <entry key="EDGE_ATTRIBUTE2" value="ATTRIBUTE2"/> \
\n           <entry key="EDGE_ATTRIBUTE3" value="ATTRIBUTE3"/> \
\n           <entry key="DATE" value="DATE"/> \
\n           <entry key="STATS_EDGE" value="STATS_TYPE"/> \
\n    </util:map>
...
```
{% include edge_dictionary_note.html %}

{% include note.html content="Queries targeting the Edge Table must use **JEXL** syntax, as Lucene is not currently supported" %}

## Edge Query Examples

The following examples are intended to demonstrate edge query expressions only. Query API usage is beyond the scope of this
example. Therefore, it is left to the user to perform **next** and **close** operations on the queries below and to inspect
underlying *curl* commands if needed, as demonstrated in the [previous exercise](query-basics).

These queries should yield results within your quickstart environment, assuming that you've completed the
[quickstart installation](../getting-started/quickstart-install) and also the [ingest exercise](ingest-basics#load-some-new-data)
to load additional TVMAZE-API data.

```bash
# Get all actors known to have costarred in TV shows with Kevin Bacon, i.e., all edges where
# the 'SOURCE' value is 'Kevin Bacon' and the 'SINK' value will be the name of a costar...
  
$ datawaveQuery --logic EdgeQuery --syntax JEXL --param "stats=false" \
    --query "SOURCE == 'kevin bacon' && TYPE == 'TV_COSTARS'"
```

```bash
# Get the names of all TV show characters known to have been played by William Shatner...
  
$ datawaveQuery --logic EdgeQuery --syntax JEXL --param "stats=false" \
    --query "SOURCE == 'william shatner' && TYPE == 'TV_CHARACTERS'"
```

```bash
# Get all cast members associated with the show Westworld...

$ datawaveQuery --logic EdgeQuery --syntax JEXL --param "stats=false" \
    --query "SOURCE == 'westworld' && TYPE == 'TV_SHOW_CAST'"
```

```bash
# Who is the actor that plays that 'Tyrion' fellow in that one show, ummmm, something about 'thrones'?
# Note the use of the regex operator here...

$ datawaveQuery --logic EdgeQuery --syntax JEXL --param "stats=false" \
     --query "SOURCE =~ 'tyrion.*' && RELATION == 'CHARACTER-PERSON' && ATTRIBUTE2 =~ '.*thrones'"
```

By default, the **EdgeQuery** logic will return "STATS" information associated with the returned edges. That behavior
was disabled in the queries above by setting the **stats** parameter to *false*, just to keep query responses as compact
as possible.

## Get Objects Associated with an Edge

Given a distinct edge key, we can retrieve the data object(s) residing in the [primary data table](../getting-started/data-model#primary-data-table)
from which that edge was derived.

For example, given the edge described by the following attributes...

| SOURCE | SINK | TYPE | RELATION |
|--------|------|------|----------|
| Don Knotts | Andy Griffith | TV_COSTARS | PERSON-PERSON |

...we can retrieve the object(s) (a.k.a, "event(s)") that caused this edge to be created at ingest time, via the
**EdgeEventQuery** logic...

```bash
$ datawaveQuery --logic EdgeEventQuery --syntax JEXL \
    --query "SOURCE == 'Don Knotts' \
             && SINK == 'Andy Griffith' \
             && TYPE == 'TV_COSTARS' \
             && RELATION == 'PERSON-PERSON'"
```
In other words, if multiple/distinct data objects are ingested into the primary data table on the same date (i.e., into
the same YYYYMMDD row partition), and if all of them happen to generate the same edge tuple, then those distinct occurrences
will be aggregated into a single key/value pair in the DataWave edge table. Therefore, it is possible that more than one
object ("event") will be returned by *EdgeEventQuery* for a given query tuple for a given date.

[dw_blob_default_properties]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/properties/default.properties#L440