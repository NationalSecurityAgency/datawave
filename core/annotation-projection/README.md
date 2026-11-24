# Annotation projection

Annotation projection selects values from annotation JSON and turns them into named DataWave fields. These fields will support indexing and querying both annotations and truthmarks. The shared library is intended for warehouse ingest and the annotation microservice write path.

## Current scope

This module currently provides test inputs, independently authored expected answers and checks against existing annotation APIs. The production projector is still to be implemented. Passing these tests establishes that the examples are consistent with the behavior they check; it does not establish a working projection or indexing pipeline.

The intended projector accepts an immutable `datawave.annotation.protobuf.v1.Annotation` message after preparation. It emits logical field occurrences: a configured base field name, grouping components and a formatted value for each selected result. Repeated names and multiple values must be retained. Callers supply event context and perform normalization, analysis and storage writes. Persistence and projection must use the same prepared annotation and identities.

In testing, a **fixture** is the prepared context needed to run a test, which can include data, objects, setup and cleanup. Here, the JSON resources are **test data and expected answers**; `AnnotationProjectionTestCases` assembles models for the tests. The resources and helper do not themselves execute preparation or projection.

## Ownership and reuse

| Owner | Current checks and planned responsibility |
| --- | --- |
| `core/annotation-projection` | Shared examples and model, serialization, presence, identity-hash and formatting checks. The semantic projector will be implemented here. |
| `warehouse/ingest-annotation` | DataWave Type normalization, Hadoop reader framing and existing annotation/source serializers. Ingest execution remains with this adapter. |
| Annotation microservices | API/message handling and coordinated annotation or truthmark persistence and index writes. These integration checks remain to be implemented. |

The module publishes a `tests` JAR containing the shared resources and the public `AnnotationProjectionTestCases` helper. Consumer tests use Maven type `test-jar` and scope `test`, with the matching DataWave release version. The JAR excludes the core test classes. Test data and the helper are excluded from the production JAR.

The test JAR is attached during `process-test-classes` so downstream tests can use it in a reactor `test` build. A test JAR does not export its owner's test-scoped dependencies; consumers must provide the JUnit and Jackson dependencies used by the helper.

See [shared test data and checks](src/test/resources/annotation-projection/README.md) and [ingest-specific checks](../../warehouse/ingest-annotation/src/test/resources/annotation-projection/README.md) for the examples, coverage limits and commands.
