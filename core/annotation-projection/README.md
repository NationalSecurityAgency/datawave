# Annotation projection

Annotation projection selects values from annotation JSON and turns them into named DataWave fields. These fields will support indexing and querying both annotations and truthmarks. The shared library is intended for warehouse ingest and the annotation microservice write path.

## Current scope

This module compiles semantic projection definitions and selects values from an immutable `datawave.annotation.protobuf.v1.Annotation` message after preparation. It streams immutable `ProjectedField` occurrences containing the configured binding ID, base field name, grouping components and formatted event value. It also provides test inputs, independently authored expected answers and conformance tests.

Bindings execute in configured order, with segment/value frames in accepted input order. Wildcard arrays retain list order; wildcard maps use Java `String.compareTo` key order. Repeated occurrences and distinct values at the same group are retained. Preparation owns identity completion and segment sorting; projection preserves supplied identities and does not modify the accepted message.

Callers supply event context and perform normalization, analysis and storage writes. Persistence and projection must use the same prepared annotation and identities. The core conformance tests establish semantic projection behavior; the integrated preparation, configuration-loading and indexing paths remain to be implemented.

In testing, a **fixture** is the prepared context needed to run a test, which can include data, objects, setup and cleanup. Here, the JSON resources are **test data and expected answers**; `AnnotationProjectionTestCases` assembles models for the tests. The resources and helper do not themselves execute preparation or projection.

## Ownership and reuse

| Owner | Responsibility |
| --- | --- |
| `core/annotation-projection` | Semantic compilation, typed selection, scalar expressions, grouping and immutable logical output; shared examples and independent conformance checks. |
| `warehouse/ingest-annotation` | DataWave Type normalization, Hadoop reader framing and existing annotation/source serializers. Ingest execution remains with this adapter. |
| Annotation microservices | API/message handling and coordinated annotation or truthmark persistence and index writes. These integration checks remain to be implemented. |

The module publishes a `tests` JAR containing the shared resources and the public `AnnotationProjectionTestCases` helper. Consumer tests use Maven type `test-jar` and scope `test`, with the matching DataWave release version. The JAR excludes the core test classes. Test data and the helper are excluded from the production JAR.

The test JAR is attached during `process-test-classes` so downstream tests can use it in a reactor `test` build. A test JAR does not export its owner's test-scoped dependencies; consumers must provide the JUnit and Jackson dependencies used by the helper.

## Semantic projection API

A projection profile describes how to turn annotation content into DataWave field names and values. The **semantic** part of the profile specifies which annotation properties to read, how to format or combine their values, and which grouping context to attach. The separate **datawave** part specifies the field's `Type`, the values used for indexing, and the enabled event and index destinations. This library implements the semantic part. The ingest handler or microservice that calls it supplies the DataWave configuration and performs normalization and writes.

Start with [baseline-profiles.json](src/test/resources/annotation-projection/configuration/baseline-profiles.json), which contains complete example profiles for text extraction, binary labels and multiclass labels. The binary-label profile is paired with [binary-label.json](src/test/resources/annotation-projection/accepted/binary-label.json) and its [expected fields](src/test/resources/annotation-projection/expected/binary-label.json). These are test configurations and independently authored answers; a production configuration loader remains to be implemented.

### Bindings and scope

Each profile contains a list of **bindings**. A binding is one configured rule for producing a field. For example, this binding from the binary-label profile produces a category field from each annotation value:

```json
{
  "id": "value",
  "scope": "value",
  "field": "BINARY_CATEGORY",
  "required": false,
  "expression": { "op": "select", "path": "$.value" },
  "format": "text",
  "grouping": { "kind": "source-segment-value" }
}
```

The `id` identifies the rule in configuration and error messages. `field` is the DataWave base field name, before any grouping context is added. `scope` determines which part of the annotation the rule processes and what `$` means in its selection path:

| Scope | Object processed by the binding |
| --- | --- |
| `annotation` | The complete `Annotation`, once. |
| `source` | The embedded `AnnotationSource`, when present. |
| `segment` | Each `Segment`, in the order supplied by preparation. |
| `value` | Each `SegmentValue` within each segment, retaining its enclosing source and segment identities. |

In the example above, `$.value` means the `value` property of the current `SegmentValue`. The binding runs separately for every value. It does not select all labels into one list and then try to pair them with scores or identities from another list.

### Compiling a profile and projecting an annotation

To use the API, the calling component follows these steps:

1. During setup, parse and validate the complete configuration document. Pass a profile's `semantic` object, represented as a `Map<String, ?>`, to `CompiledSemanticProfile.compile`. This validates the binding properties, selection paths, formats and grouping rules and creates an immutable profile that can be reused. The calling component also validates the DataWave field policies, reserved field names and configured `Type` and Analyzer implementations.
2. Prepare one immutable `Annotation` with its final content and identities. Use that same object for annotation persistence and projection. Preparation is responsible for missing-ID completion, source-reference validation and segment ordering.
3. Call `AnnotationProjector.project` with the annotation, its compiled profile, the annotation's resource-limit checks, and a callback that receives the resulting fields.

For the binary-label example, the call has this form:

```java
CompiledSemanticProfile profile = CompiledSemanticProfile.compile(
        "binary-label-baseline", "BINARY_LABEL", semanticDefinition);

AnnotationProjector projector = new AnnotationProjector();
projector.project(acceptedAnnotation, profile, annotationBudget, fieldCollector);
```

Here, `semanticDefinition` is the parsed `semantic` object from the example profile. `acceptedAnnotation` is the built `Annotation` after preparation. `annotationBudget` implements `ProjectionWorkGuard`, described below, and `fieldCollector` is a `Consumer<ProjectedField>` supplied by the calling component. [AnnotationProjectorTest](src/test/java/datawave/annotation/projection/AnnotationProjectorTest.java) demonstrates this sequence: `configured()` reads the example semantic definition, and the tests collect actual fields into a list for comparison with independent answers.

A compiled profile applies to exactly its configured `annotationType`. For example, the `BINARY_LABEL` profile cannot be used with a `MULTICLASS_LABEL` annotation. Choosing the applicable profile, or following the persistence path when no profile is configured, is the calling component's responsibility.

### Projected fields and grouping context

Each `ProjectedField` represents one field name/value pair before DataWave normalization. It provides:

| Method | Value returned |
| --- | --- |
| `bindingId()` | The configured rule that produced the field, such as `value`. |
| `baseFieldName()` | The base field name, such as `BINARY_CATEGORY`. |
| `groupingComponents()` | The grouping context as separate components, such as `["aSource", "bAll", "vLabel"]`. An ungrouped field returns an empty list. |
| `eventValue()` | The formatted field value, such as `present`, before applying its configured DataWave `Type`. |

For the example annotation, the category has base name `BINARY_CATEGORY`, grouping context `aSource.bAll.vLabel`, and event value `present`. The calling component can combine the base and context into the grouped field name `BINARY_CATEGORY.aSource.bAll.vLabel`. The grouping components are the source's `analyticHash`, the segment's `segmentHash`, and the value's `valueHash`, respectively. They associate fields such as category and score with the same annotation value.

Formatting and normalization are separate operations. For example, the score binding returns event text `0.75`. The example's DataWave policy assigns `NumberType` to `BINARY_SCORE`, which normalizes that text to the index term `+ZE7.5`. The projector produces the event text; DataWave indexing code applies the `Type`. Both answers appear in [the expected binary-label fields](src/test/resources/annotation-projection/expected/binary-label.json).

### Selecting and combining values

Bindings can also combine properties into a field value. The supported expression operations are:

| Operation | Behavior |
| --- | --- |
| `select` | Reads a property or explicitly selected array/map elements using the configured path. |
| `literal` | Supplies a configured string, such as a prefix. Numeric and Boolean literals are not supported. |
| `format` | Converts one selected value to text using one of the formats below. |
| `concat` | Joins all supplied operands with the configured delimiter. If an operand is missing, the combined result is missing. |
| `join-nonblank` | Joins the operands that are present and nonblank. If none remain, the combined result is missing. |

The `BINARY_RESULT` binding in [baseline-profiles.json](src/test/resources/annotation-projection/configuration/baseline-profiles.json) shows `concat`: it selects the current value's label, formats its score, and joins them with `:` to produce `present:0.75`. The source bindings show `join-nonblank`, which combines model, engine and optional platform names.

Each operand inside `concat`, `join-nonblank` or a nested `format` must produce at most one result for the object being processed. Wildcard selectors are not allowed in those operands, because combining several independent result lists would leave their pairing undefined. A standalone binding may use a wildcard to produce several field values.

### Formatting field values

| Binding format | Accepted input and resulting text |
| --- | --- |
| `text` | A string, retained for later blank checking and trimming. |
| `java-float-text` | A finite protobuf Float score, using `Float.toString`, including `0.0` and `-0.0`. |
| `json-number-text` | An exact number, preserving its mathematical value with `BigDecimal` formatting. For example, `12.5000` becomes `12.5`; a very small number can use scientific notation. |
| `boolean-text` | A Boolean, producing `true` or `false`. |

These formats check the actual selected type. A string containing `"12"` remains a string; it is not automatically accepted by `json-number-text`. Similarly, `false` is a present Boolean value and produces a field. [scalar-format-vectors.json](src/test/resources/annotation-projection/oracles/scalar-format-vectors.json) contains concrete successful and rejected conversions.

### Required values, blank output, and trimming

`required` controls what happens when an expression has no result. An optional missing score produces no score field; a required missing score raises `ProjectionException`. A selected value with the wrong type raises an error regardless of `required`. Presence is checked before blank omission: an existing empty string satisfies required selection, then produces no field under `omitBlank`. Protobuf optional scores distinguish absence from an explicitly supplied zero. The protobuf value string has an implicit empty-string default, so an omitted string can still be present as `""` in the accepted model.

The binding's scope must also exist before it can run. An annotation without a source has no source object to process; the projector does not create one to satisfy a required source binding. Preparation performs the separate validation of required annotation content.

The projector combines expression operands before checking for blank output and applying `String.trim`. For example, a label of `" label "` and score `0.0` produce `"label :0.0"`, retaining the space before the colon. It does not trim each operand first. Source-label grouping similarly hashes the combined label before final trimming, using the configured `murmur3-32-unencoded-chars-v1` strategy. This derives only the synthetic `ALL` boundary and source-label hashes; supplied source, segment and value hashes are preserved. The padding and whitespace cases in [AnnotationProjectorTest](src/test/java/datawave/annotation/projection/AnnotationProjectorTest.java) document these details.

### Structured extensions and prepared state

Structured extensions require an additional preparation step because `AnnotationV1` stores extension entries as strings. [structured-codecs.json](src/test/resources/annotation-projection/configuration/structured-codecs.json) declares that `stats` is JSON for the example `FUTURE` annotation type. [exact-extension-profile.json](src/test/resources/annotation-projection/configuration/exact-extension-profile.json) then selects `$.extension.stats.count` and `$.extension.stats.enabled`. Its [expected fields](src/test/resources/annotation-projection/expected/extension-numeric-boolean-projection.json) include the exact number `9007199254740993` and Boolean text `false`.

Preparation supplies the decoded interpretation of declared extension entries through `ProjectionEvaluationState.ExtensionValues`. Entries without a declaration retain their stored strings, even when the text looks like JSON. The callback receives both the key and stored value, since different annotation values can have different content under the same key.

The prepared-state API lets the calling component retain these interpretations for projection:

```java
ProjectionEvaluationState state = ProjectionEvaluationState.from(
        acceptedAnnotation, preparedExtensionValues, annotationBudget);

projector.project(state, profile, fieldCollector);
```

`preparedExtensionValues` supplies validated strings, exact numbers, Booleans, maps or lists. The state copies mutable containers and retains the original accepted annotation without changing its stored extension strings. Reusing the state avoids repeating the callback; each projection still counts its own processing work. The callback's JSON decoding must enforce preparation limits before returning values. The state-construction checks cannot limit allocations performed inside that callback. The `declaredExtensionsRetainExactNumbersAndFalse()` and `preparedExtensionTreesOwnTheirMapsAndListsAndDecodeOnlyOnce()` tests show the input and ownership rules.

### Resource limits

`ProjectionWorkGuard` connects projection to the calling component's resource limits for one annotation. The component implements its methods to check the remaining budget and throw when a hard limit would be exceeded:

| Method | Work or allocation checked |
| --- | --- |
| `chargeTreeNode(depth)` | Counts each object, array or scalar in the internal selection data, and checks nesting before allocating its representation. |
| `chargeSelectorSteps(count)` | Counts scope traversal, property lookups including misses, and wildcard children before expanding them. |
| `checkStringLength(length)` | Checks space for a temporary string before composition or numeric formatting. Length is measured in Java UTF-16 code units; downstream UTF-8 field and mutation limits are separate checks. |
| `chargeCandidate()` | Counts each produced field before delivering it to the callback, including equal values that the calling component may later combine. |

The projector uses the supplied budget throughout and does not reset it when processing another binding or reusing prepared state. The finite `CountingGuard` in [AnnotationProjectorTest](src/test/java/datawave/annotation/projection/AnnotationProjectorTest.java) demonstrates these checks. Production limits, field overflow policies and mutation accounting belong to the calling integration.

### Collecting fields and handling errors

Field values are delivered to `fieldCollector` as they are produced. Repeated values are retained, and a later error does not undo fields already delivered. The calling component decides how to collect fields, apply field policies, normalize values and submit mutations. `ProjectedField` equality includes the binding ID, so it is not a field-name/value-only deduplication rule. Field combination happens after each produced field has been counted.

### Supported selection paths

Selection paths use the `scoped-paths-v1` subset of JSONPath: `$` for the current object, dot properties such as `$.value`, quoted map keys such as `$.extension['key.with.dot']`, nonnegative array indexes, and child wildcards such as `$.extension.items[*]`. Arrays retain their order; map wildcards visit keys in Java `String.compareTo` order. Filters, recursive descent, slices, unions, functions and negative indexes are rejected during compilation. The internal implementation uses Jayway JSONPath 2.10.0 and checks the supported grammar and resource limits before evaluating paths. Expression nesting is limited to 32 levels.

See [shared test data and checks](src/test/resources/annotation-projection/README.md) and [ingest-specific checks](../../warehouse/ingest-annotation/src/test/resources/annotation-projection/README.md) for the examples, coverage limits and commands.
