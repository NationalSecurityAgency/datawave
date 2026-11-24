# Annotation projection ingest checks

Annotation projection selects annotation JSON values and turns them into DataWave fields for indexing and querying annotations and truthmarks. Shared inputs, profiles and expected logical fields belong to [core/annotation-projection](../../../../../../core/annotation-projection/README.md). This module reuses their test JAR and checks behavior owned by ingest.

The source calls the prepared test context a fixture. The local files are **test data and expected answers** used by `IngestAnnotationProjectionFixtureContractTest`; they do not perform ingest or write to a database.

## Current coverage

The test class executes three kinds of checks:

- **Normalization:** instantiate the configured DataWave Type and compare its normalized term with a literal expected answer.
- **Reader framing:** run `SimpleAnnotationRecordReader` against temporary files containing a single object, newline-delimited JSON, adjacent objects, a root array or a malformed root.
- **Serialization for persistence:** compare existing annotation/source serializers with expected keys, decoded values and entry counts.

These checks do not execute the planned preparation, semantic projector, routed indexing, handler/mapper counting or storage retrieval implementation.

## Local test data and expected writes

| Location | Purpose |
| --- | --- |
| `expected/*-mutations.json` | Six independently authored sets of expected Accumulo entries and counts. Some include event/index answers for later integration tests. |
| `configuration/independent-route-profile.json` | Policies that enable each output destination separately, plus a field with all destinations disabled. |
| `raw/reader-malformed-root.json` | Invalid input used to check reader initialization failure. |

An expected entry specifies its table, row, column family, column qualifier, visibility, timestamp and value. The serializer test compares a **multiset**: every occurrence counts, including duplicate entries with the same key. It compares decoded protobuf messages so incidental map serialization order does not affect equality.

A serializer output entry is a candidate for a write; this test does not submit it to Accumulo. Event/index entries and expected handler counts in the same files are answers for later tests, not evidence that those writes or counters already work. Metadata cleanup counts are kept separate.

The Unicode case expects eight serializer entries, including distinct segment payloads sharing a complete key. This checks preservation before submission; proving retention during scans or compaction requires storage tests. The multiple-region indexed submission total still needs an independently established answer using native aggregation behavior.

The independent-route profile covers the field index (`fi`), event fields, global forward index, global reverse index and disabled output. Its future execution tests must verify both the expected entries and the absence of entries for disabled destinations.

## Verification

From the worktree root, run the shared core and ingest checks together:

```sh
mvn -o -B -pl core/annotation,core/annotation-projection,warehouse/ingest-annotation test \
  -Dtest=AnnotationProjectionFixtureContractTest,IngestAnnotationProjectionFixtureContractTest,AnnotationUtilsSortSegmentsTest,SegmentValueCompatibilityTest,AnnotationHelperTest,SimpleAnnotationIngestHelperTest,SimpleAnnotationRecordReaderTest \
  -Dsurefire.failIfNoSpecifiedTests=false -Dspotless.skip=true -Dsort.skip=true
```

See the [shared test data README](../../../../../../core/annotation-projection/src/test/resources/annotation-projection/README.md) for model, hash, formatting and extension cases.

Annotation service tests should reuse the core test data and verify their own coordinated persistence/index writes, including final identities, message retries and updates. Shared normalization and index-mutation construction remain planned reusable DataWave components.
