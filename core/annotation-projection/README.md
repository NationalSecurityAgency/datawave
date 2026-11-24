# Annotation projection

Annotation projection takes data in the annotation JSON format and projects selected values into named DataWave fields for indexing and querying annotations and truthmarks. The shared projection contract serves both warehouse ingest and annotation microservice write paths.

The `core/annotation-projection` module currently establishes shared test inputs and independent expected answers. The production semantic projector is still to be implemented. Its intended API consumes the existing built annotation model and emits logical field occurrences with explicit grouping; consumers supply event context and execute normalization, analysis and storage writes.

The `tests` artifact contains the shared fixture resources and the public `AnnotationProjectionTestCases` helper. Consumer tests depend on it with Maven type `test-jar` and scope `test`. Core test classes are excluded from that artifact, and fixtures are not packaged in the production JAR. The fixture JAR is attached during `process-test-classes` so consumer tests can use it in a reactor `test` build.

See [shared fixture conventions](src/test/resources/annotation-projection/README.md). Hadoop reader, Type normalization and persistence checks remain in `warehouse/ingest-annotation`; service tests will verify API/message handling and the coordinated truthmark and index write path.
