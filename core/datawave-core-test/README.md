## DataWave Test Framework

### Purpose/History

Legacy tests were written from a feature-first approach. Data and queries were hand-crafted to validate specific 'happy 
path' tests. 

This approach does not exercise edge cases by default. As new features are added the complexity increases beyond what a 
'happy path' suite of tests can reliably cover.

This implies that a statistical method of validating code is required.

### How it works

The framework takes a 'metadata-first' approach. The DatawaveMetadata table dictates where a field exists and how it is 
normalized. Broadly speaking:
 - global index values are normalized
 - field index values are normalized
 - event values are not normalized (original value)
 - term frequency values are normalized

#### Core Components

Core components include:
- metadata columns (i, ri, e, tf)
- normalizers (LcType, NumericType, etc)
- field generators (alphabetic or numeric)
- value generators (string, number, etc)

#### Misc. Features
- events are sharded deterministically via the event id 

#### FieldMetadata

The FieldMetadata object encapsulates the field, values, metadata columns, normalizers and maps that information to a list
of event ids. FieldMetadata is used to populate tables and generate queries. Critically, the FieldMetadata is used to predict
which event ids should be returned for a given query.

### Future Work

- additional Normalizers — only `LcNoDiacriticsType` and `NumberType` are supported today; `IpAddressType` and `DateType` are explicitly rejected (`IngestMetadata.getValueGeneratorForType` throws), and Geo/Point/Hex/List normalizers aren't attempted. Each addition needs a matching `ValueGenerator` that produces values compatible with the normalizer.
- Datatypes — every field is written under a single hardcoded datatype (`"datatype-a"`, `IngestMetadata.baseDatatypes`); no field or event varies its datatype. Supporting multiple datatypes would let tests exercise datatype-scoped queries and same-field-different-type collisions.
- Visibilities — every key is written with a single hardcoded visibility (`"ALL"`; see the `// TODO: viz` markers in `ShardTableWriter`). Needs a visibility generator/config so tests can exercise column-visibility filtering and auth combinations.
- Timestamps — every key uses one fixed timestamp (`TableWriter.TIMESTAMP`, derived from a single hardcoded date). Needs support for varying timestamps/dates per event so date-range and age-off-style queries can be exercised.
- additional QueryTerms — `EqTerm`, `NeTerm`, `IsNullTerm`, `IsNotNullTerm`, `PhraseTerm` (`content:phrase`), and `BoundedRangeTerm` (bounded ranges) exist today. Still missing: regex terms, plus terms for the remaining content functions (`content:within`, `content:adjacent`, `content:scoredPhrase`).
- Regex — no `QueryTerm` exists for regex-style matching (e.g. `FIELD =~ 'pattern'`); would need a generator that derives a pattern guaranteed to match a field's known values.
- Range against content fields — `BoundedRangeTerm` returns no ranges for content/tokenized fields: a content field's index carries a per-word entry in addition to the field's own whole-value entries, so a range scan between two whole values can also sweep in unrelated per-word entries this framework's value model can't predict. Extending range support to content fields needs a way to compute the full set of index entries (word-level and whole-value) a range would match.
- functions — `filter:isNull`/`filter:isNotNull` and `content:phrase` are implemented today. Still missing: the other content functions (`content:within`, `content:adjacent`, `content:scoredPhrase`) and other function namespaces entirely (e.g. geo, grouping/unique).
- QueryMetadataGenerators — `SingleTermFactory`, `UnionFactory`, `IntersectionFactory`, and `BinaryTermFactory` already cover single-term and 2-way union/intersection shapes. Still missing: deeper n-ary/nested compositions (e.g. 3+ term unions/intersections, mixed AND/OR trees) to exercise more complex query plans.
- Splits for MAC test — tests run against a single-tablet in-memory Accumulo instance; tablet-split behavior isn't exercised. A real MiniAccumuloCluster variant with explicit splits would validate range-stitching/tablet-boundary behavior the in-memory instance can't surface.
- Detailed walkthrough explaining entire process — the README's "Detailed Walkthrough" section is currently just a placeholder (`dsf`). Needs a full step-by-step example tracing `IngestMetadataBuilder` → `IngestMetadata.create()` → `TableWriter.write()` → `QueryGenerator` → `executeQueryMetadata()`, showing how a query's expected event ids are derived from the same `FieldMetadata` used to populate the tables.

### Examples

See the StatisticalQueryTest in query-core for an example
