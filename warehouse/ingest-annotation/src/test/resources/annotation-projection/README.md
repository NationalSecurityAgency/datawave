# Annotation projection ingest checks

Annotation projection turns selected annotation JSON values into DataWave fields for indexing and querying annotations and truthmarks. Shared annotation inputs, profiles and logical-field answers belong to `core/annotation-projection`. This module consumes that module's test artifact and checks the ingest-specific behavior.

`IngestAnnotationProjectionFixtureContractTest` checks existing Type normalization, Hadoop reader framing and annotation/source persistence serializers. It does not execute the future preparation, projector, routed indexing or retrieval implementation.

The local resources contain six independent mutation/count snapshots, an independent-route profile and a malformed-reader input. Complete mutation snapshots retain row, family, qualifier, visibility, timestamp and typed value expectations. Persistence assertions compare multisets, retaining duplicate submissions and comparing decoded protobuf content rather than incidental map wire order. Event/index entries and handler counts in these snapshots remain independent expected answers for later integration tests.

The Unicode persistence case expects eight submissions, including distinct segment payloads sharing a complete key. Scanner/compaction retention is separate. Multiple-region indexed totals remain pending an independent native aggregation oracle. The independent-route profile distinguishes fi-only, event-only, forward-only, reverse-only and all-off policies.

From the worktree root, run the shared and ingest fixture suites together:

```sh
mvn -o -B -pl core/annotation,core/annotation-projection,warehouse/ingest-annotation test \
  -Dtest=AnnotationProjectionFixtureContractTest,IngestAnnotationProjectionFixtureContractTest,AnnotationUtilsSortSegmentsTest,SegmentValueCompatibilityTest,AnnotationHelperTest,SimpleAnnotationIngestHelperTest,SimpleAnnotationRecordReaderTest \
  -Dsurefire.failIfNoSpecifiedTests=false -Dspotless.skip=true -Dsort.skip=true
```

Service consumers should reuse the core fixture artifact and verify their own coordinated persistence/index writes, including final identities, message retries and updates. Shared normalization and index-mutation construction are planned reusable DataWave components; their production implementation is still pending.
