---
title: "DataWave Data Model"
tags: [getting_started, data_model, ingest, query]
summary: DataWave utilizes the Accumulo table schemas described below as the basis for its ingest and query components
---

## Primary Data Table

The primary data table uses a sharded approach and can be described as an intra-day hash partitioned table
where fields in a data object are stored collocated in a single partition. The *Shard ID* is a function of
the *UID* and therefore should be reproducible given the same object ingested at different points in time. This enables
de-duplication of objects when they are re-ingested. The *Data Type* is a user defined category of the data that will
typically be used at query time. The *Data Type* allows for further reduction in the amount of data to be searched.

The primary data table also contains an in-partition index, which we call the *Field Index*, and we denote the K,V
pairs that are in the field index with a leading 'fi' in the column family. The field index is used by
custom Accumulo iterators at query time to find data objects in the partition.

Optionally, if the table is used to store documents, then the original document or different views of the
document can be stored in the 'd' column family. Typically this column family would be set up as its own
locality group. An example of different views of a document could be .txt and .html versions of the original
document.

To enable phrase queries on documents, the 'tf' column family contains a protocol buffer (PB) in the value
that is a list of word offsets for the term in the document. This too could also be stored in a separate
locality group.

{% include table_primary.html %}

## Global Index Tables

The *forward* and *reverse* index tables serve as global indexes mapping terms to partitions. The index maps a
*NFN:NFV* pair to a category of data within the partitions of the primary data table. The *Uid.List Protocol Buffer (ULPB)* object contains
the number of occurrences of the *NFN:NFV* pair in a category of data in the partition. Additionally, the *ULPB* may contain
the UIDs of the objects that contain the *NFN:NFV*. We say “may contain” because there is an upper limit on the number of
UIDs in the *ULPB*.

{% include table_index_fwd.html %}

*NFV*'s that are indexed within the global reverse index table can be searched using leading wildcards. Thus, the index
is created by simply reversing the characters in the *NFV*...

{% include table_index_rev.html %}

## Data Dictionary Table

The data dictionary table contains information about the data stored in the other tables, to include
whether or not a field is indexed, the normalizer that is used for the field, etc. The structure of the table
is as follows.

{% include table_metadata.html %}

## Edge Table

The edge table may represent one or more graphs, any of which may be comprised of unidirectional and bidirectional edges
depending on the user's needs. A single edge key represents a unidirectional pair of *Normalized Field Value* vertices,
which may be thought of as a *source* vertex and a *sink* vertex respectively. Thus, bidirectional edges are generated
by simply creating a second key having the original *source*, *sink*, and other attributes reversed. Additional
information may be encoded into an edge key as well, such as the *relationship* between the two vertices, the *type* of
the edge, and others.

{% include table_edge.html %}

## Date Index Table

By design, the primary data table permits at most one *YYYYMMDD* value to be encoded within the assigned row partition (i.e.,
*Shard ID*) of a given data object, and, by default, this date will serve as the basis for the *date range* criteria of any query that
targets the object. However, a given data object may contain any number date-related fields, any of which may be important to a user
at query time for filtering purposes.

Indexing may be configured for such fields at ingest time. If date indexing is configured for a field, then its values along with the
field name itself will be mapped by entries within this table to the partitions in the primary data table where the source objects
are stored. Query clients can then leverage this table to enable date range filtering based on these dates, rather than on the
dates encoded within the *Shard IDs* of the stored objects.

Note that the *Date Type Name (DTN)* column family identifier here is typically leveraged to provide semantic grouping of
distinct field names from disparate datasets. For example, a DTN of "SALE_DATE" might be used to group the values of semantically
equivalent fields such as "PURCHASE_DATE", "RECEIPT_DATE", "DATE_OF_SALE", "DATE_PURCHASED", etc.

{% include table_index_date.html %}

## Load Dates Table

The load dates table tracks the dates on which specific field names were loaded into specific tables via DataWave Ingest.
This information may be leveraged internally for the purposes of query optimization, load date-based filtering for queries,
etc.

{% include table_load_dates.html %}

## Other Tables

### Ingest Error Tables

The layouts associated with the four ingest error tables are identical to those listed above for the *Primary Data Table*,
*Global Index Tables*, and *Data Dictionary Table*. The only difference is that the respective error tables here are meant
to capture *Data Objects* that failed to be fully loaded during ingest due to one or more processing errors.

That is, these tables are intended to capture all successfully-processed NFN:FV pairs from their respective *Data Objects*,
just as they would have appeared in the normal schema, including supplemental key/value pairs related to the errors themselves.
Since schema descriptions for the four primary data tables apply here as well, we describe below only the specific entries
used to convey information about the error(s)

{% include table_ingest_errors.html %}

### Query Metrics Tables

The layouts associated with the four query metrics tables are identical to those listed above for the *Primary Data Table*,
*Global Index Tables*, and *Data Dictionary Table*. The only difference here is that the respective query metrics tables
are intended to persist information associated with user queries exclusively. They can be leveraged by users to gain insight
into their own queries, and by administrators to gain insight into active and historical queries. Since schema descriptions
for the primary data tables apply here as well, we describe below only the specific NFN and FV components that are used to
represent a query metrics *Data Object*.

{% include table_query_metrics.html %}

## Terms and Definitions

{% include data_model_terms.html %}

[apache_accumulo]: http://accumulo.apache.org/
[apache_hadoop]: http://hadoop.apache.org/
[data_fusion]: https://en.wikipedia.org/wiki/Data_fusion
[graph_theory]: https://en.wikipedia.org/wiki/Graph_theory
[cell_level_sec]: https://accumulo.apache.org/1.9/accumulo_user_manual.html#_security
[acc_data_model]: https://accumulo.apache.org/1.9/accumulo_user_manual.html#_data_model
