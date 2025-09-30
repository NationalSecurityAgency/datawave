## Query Refactor

#### Motivation

The global index structure of a sorted set has advantages and disadvantages. It allows a query for a single field and value to jump directly to a specific shard,
and optionally to a document if the global index is configured to store uids. 

However, there are drawbacks to this model. 

Indexing a field becomes an intractable problem. Verifying the integrity of a field is intractable. The load on the shard index via query and ingest can hotspot.
The shard index is used for much more than simply finding which shard or document contains a document -- it supports field expansion, value expansion, etc. 

Queries with a significant date range potentially incur a penalty for field and value expansion (ivarators) that is then paid by each shard in the search space.
This is true even when a specific regex may expand into a finite number of terms for a specific shard.

### Proposal

#### Reducing the size of the global index

The current structure contains runs of similar shards
```
# existing structure
value FIELD datatype\0shard  Value

# in practice high cardinality fields look like this
value FIELD:20240101_0<null>datatype-a
value FIELD:20240101_1<null>datatype-a
value FIELD:20240101_11<null>datatype-a
value FIELD:20240101_12<null>datatype-a
...
value FIELD:20240101_99<null>datatype-a
value FIELD:20240101_99<null>datatype-b
value FIELD:20240101_99<null>datatype-c
```

Given a datatype with N shards per day and M datatypes, a high cardinality field-value pair will contain up to N * M keys per day in the database.

Completely dropping uids and collapsing individual shards into a bitset enables a compressed table. This effectively cuts the size of the shard index by the 
number of shards per day, averaged by field cardinality.

```
# proposed table structure
20240101<null>value FIELD:datatype-a  (010101)
20240102<null>value FIELD:datatype-a  (101010)
20240103<null>value FIELD:datatype-a  (000111)
```

#### Partitioning the global index

Even with a significant size reduction the global index is still large and is still susceptible to the initial list of motivating problems (indexing, data
verification, planning costs, etc).

Thus, we partition the global index by days. The ratio of global index tablets to shard tablets is now 1 to N where N is the number of shards per day. This 
makes adding an index, removing an index, index verification, field expansion, and term expansion tractable problems.

New Global Index
```
# global index (inverted index of years)
value FIELD:year (bitset of days)
---------------------------------
value FIELD:2020 (010101)
value FIELD:2021 (101010)
value FIELD:2022 (000111)
```

Day Index
```
# day index
date<null>value | FIELD | datatype | (array of counts)
------------------------------------------------------
20240101<null>value:FIELD datatype-a ([0,0,123,32...])
20240102<null>value:FIELD datatype-a ([2,12,24,56...])
20240103<null>value:FIELD datatype-a ([9,10,14,17...])
```

Document Index
```
# global index structure for unique identifier fields, CQ is record id
---------------------------------------------
value FIELD:20240101<null>datatype-a<null>uid
```

#### Splitting shard table into local index


### Implications

Query Planning becomes a multi-stage, parallelized process. 

1. Consider the query `FOO == 'bar'` with a date range of Dec 2023 to Jan 2024
2. The global index is scanned and we find that the query appears in two distinct days.
3. The GlobalQueryPlanner creates a PlanTask for each day found in the Year Index.
4. The Plan tasks submits a single scan for this term. A single key is returned.

#### Implications of Space Utilization

In terms of space utilization and keys scanned we must consider cases based on cardinality.
1. Full cardinality. Term appears in every shard for every day.
2. Half cardinality. Term appears in half the shards for a given day.
3. 10% cardinality. Term appears in 10% of shards for a day.
4. Unique cardinality. Term appears in precisely one shard for a day.

Previously an indexed field value had three copies per shard (global index, field index, document).

Now an indexed field has four copied (global index, day index, field index, document). However, the number of global index 
keys is reduced by a factor of 365. 

Given 100 shards per day.
```
  Full Cardinality: previously 36,500 now 2 (99.9945% reduction)
  Half Cardinality: previously 18,200 now 2 (99.9890% reduction)
   10% cardinality: previously 3,650 now 2 (99.9452% reduction)
Unique cardinality: previously 1 now 2 (100% growth)
```

#### Implications of Keys Scanned And Scanners Created

Single term query

Complex query

Field Expansion

Regex/Range Expansion

#### Implications of Scanners Created

### Exceptional cases

1. Query for a single day can jump straight to the Day Index and construct the effective query from the index (day of year).
2. Lookup UUID can use a Document Index

### Conclusion

In theory this refactor allows for faster global index scanning at the cost of higher table utilization.