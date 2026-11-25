# Shared annotation projection test data

Annotation projection selects values from annotation JSON and turns them into named DataWave fields for indexing and querying annotations and truthmarks. A projection profile specifies which properties to read, how to format their values, and which grouping context to attach. Its DataWave field policies specify the Types used for normalization and the event/index destinations that receive output.

This directory contains annotation examples, configuration examples and independently authored expected results. Core projection and warehouse ingest tests use this data; annotation service tests can reuse the same artifact for their API/message write paths. The examples use synthetic text extraction and content labels, so they can exercise the annotation structure without depending on a particular producer, model or algorithm.

The Java test classes call their prepared test context a **fixture**. The files here provide its data; builders and setup code provide the other objects needed to run a test. For the production API and module responsibilities, see the [module README](../../../../README.md#semantic-projection-api).

## Start with a complete example

The binary-label example provides a small starting point:

1. Read [accepted/binary-label.json](accepted/binary-label.json). It contains one annotation value, `present`, with score `0.75`, along with source, segment and value identities and a DataWave event binding.
2. Find the `BINARY_LABEL` profile in [configuration/baseline-profiles.json](configuration/baseline-profiles.json). Its bindings select the category and score, combine them into a result, and add source fields. Its DataWave policies specify a Type and output destinations for each base field.
3. Read [expected/binary-label.json](expected/binary-label.json). It records the expected field values, grouping context and normalized index terms.
4. Look at `baselineProfilesMatchIndependentFieldMultisets()` in [AnnotationProjectorTest](../../java/datawave/annotation/projection/AnnotationProjectorTest.java). It compiles the semantic configuration, projects the accepted annotation, and compares the actual fields with those expected results.

The other complete examples use the same arrangement:

| Example | Accepted annotation | Expected fields |
| --- | --- | --- |
| One text extraction region | [extraction-single-region.json](accepted/extraction-single-region.json) | [extraction-single-region.json](expected/extraction-single-region.json) |
| Several text extraction regions | [extraction-multiple-regions.json](accepted/extraction-multiple-regions.json) | [extraction-multiple-regions.json](expected/extraction-multiple-regions.json) |
| Binary content label | [binary-label.json](accepted/binary-label.json) | [binary-label.json](expected/binary-label.json) |
| Multiclass content label | [multiclass-label.json](accepted/multiclass-label.json) | [multiclass-label.json](expected/multiclass-label.json) |

Both extraction examples use the `TEXT_EXTRACTION` profile. The binary and multiclass examples use their corresponding profiles, giving three baseline profiles for four annotations.

## Inputs and configuration

### Resource layout

| Location | What it contains |
| --- | --- |
| [raw/](raw/) | Inputs before event binding or extension adaptation, depending on the case. Some intentionally cannot be parsed directly as the stored protobuf representation. |
| [accepted/](accepted/) | Four authored examples of complete annotation content, including event binding, metadata and identities. |
| [configuration/](configuration/) | Projection profiles, extension-codec declarations, empty registries and invalid configuration examples. |
| [expected/](expected/) | Five sets of expected field results, including normalized terms for DataWave consumer tests. |
| [schema/](schema/) | Expected protobuf field inventory, an example containing every declared field, and optional-presence/default cases. |
| [oracles/](oracles/) | Independently authored hash, formatting and decoded-extension answers. An oracle is the expected result used to judge a test. |
| [provenance.json](provenance.json) | Authorship information, related file names and content hashes for the two raw extraction examples. |

### Raw input and accepted annotations

An event binding identifies the DataWave record addressed by an annotation through its `shard`, `dataType` and `uid`. The raw extraction files have not yet received that binding and associated ingest metadata. The accepted examples describe the expected complete content that later preparation tests can use as an answer and that projector tests can use directly as input.

Accepted examples are authored test data. Their presence does not mean a production preparation implementation has generated them from the raw files. The helper also builds variants with missing identities or received segment order so that later tests can verify the preparation step itself.

The `TEXT_EXTRACTION` examples select text and a content kind from `POINTS` regions. The single-region example uses `PRINTED_TEXT`; the multiple-region example uses `TEXT_IN_SCENE`. The label examples select a category, optional score, combined result and source fields. These are algorithmic content labels, separate from column visibility and access control. The multiclass example contains one selected category; the tests do not impose model-specific probability rules.

### Projection profiles and extension declarations

[baseline-profiles.json](configuration/baseline-profiles.json) contains the three baseline profiles and fourteen distinct base-field policies. Each profile has a `semantic` section for field selection, formatting and grouping, and a `datawave` section for Types and storage destinations. The semantic compiler is exercised by the projector tests; complete production configuration loading and policy validation remain integration work.

[exact-extension-profile.json](configuration/exact-extension-profile.json) shows additional fields selected from structured extension values. Its companion [structured-codecs.json](configuration/structured-codecs.json) declares that the `stats` extension entry contains JSON for the example `FUTURE` annotation type. The empty and duplicate-key configuration files provide inputs for registry/setup tests; a file's presence alone does not demonstrate that a production loader accepts or rejects it.

## Reading expected field results

### Field values, grouping context and normalized terms

An expected field entry describes both semantic output and the DataWave normalization answer needed by consumer tests. For example, the binary-label score has this expected entry:

```json
{
  "base": "BINARY_SCORE",
  "group": ["aSource", "bAll", "vLabel"],
  "field": "BINARY_SCORE.aSource.bAll.vLabel",
  "value": "0.75",
  "normalized": "+ZE7.5"
}
```

| Property | Meaning |
| --- | --- |
| `base` | The configured DataWave field name before grouping context is added. |
| `group` | The grouping components as separate strings. For value bindings, these are the source's `analyticHash`, the segment's `segmentHash`, and the value's `valueHash`. |
| `field` | The complete grouped field name formed from the base and those components. |
| `value` | The formatted event value produced by semantic projection, before Type normalization. |
| `normalized` | The independently expected term after applying the configured DataWave Type. The score example uses `NumberType`. |

The projector returns the base name, grouping components and event value separately in `ProjectedField`. The core projector tests compare those results. The [ingest checks](../../../../../../warehouse/ingest-annotation/src/test/resources/annotation-projection/README.md) instantiate the configured Types and compare normalized terms with the literal `normalized` answers.

Source-label fields use the accepted source hash, a synthetic `ALL` boundary hash, and a hash of the combined model/engine/platform label. That label is hashed before final trimming. Supplied source, segment and value identities remain unchanged.

### Repeated values and comparison order

Several field entries may have the same base and grouping context. Distinct values must survive, and repeated equal entries must still be counted. The tests therefore compare a **multiset**: the field values and the number of times each occurs. Converting the results to a set would lose duplicate occurrences.

Expected-file order is not a global field-publication order. Separate projector tests check configured binding order, accepted segment/value order, array order and map-key traversal order. This lets the expected files keep their authored organization without discarding ordering checks.

The intended indexing contract retains grouping context in event and term-frequency field names; field-index and global-index entries use the base name. The current shared tests verify semantic fields, not those index writes.

### Maintaining independent answers

Expected-result files are sometimes called **goldens**. They must describe the result independently of the implementation being tested. Do not regenerate them by running the new projector or handler and copying its output.

Hash answers distinguish the existing native identity operations from the synthetic source-label grouping operation. Their inputs use the appropriate UTF-8 or unencoded Java-character representation. [hash-vectors.json](oracles/hash-vectors.json) and [native-source-identity.json](oracles/native-source-identity.json) provide literal answers. Cases with supplied opaque IDs expect their spelling to remain unchanged.

Score cases distinguish absence, explicit zero and negative zero. They retain finite supplied values without rescaling them or imposing a probability range. Padding cases expect RESULT and SOURCE values to be combined before final trimming. [field-format-cases.json](oracles/field-format-cases.json) and [scalar-format-vectors.json](oracles/scalar-format-vectors.json) record these answers and conversion controls.

## Java helpers and test cases

### Complete models and case variants

[AnnotationProjectionTestCases](../../java/datawave/annotation/projection/AnnotationProjectionTestCases.java) uses ordinary Java builders and JUnit argument rows. A row supplies the input and any literal expected answers for a parameterized test. The helpers assemble those objects; they do not run production preparation or calculate an expected result with the projector.

| Helper | Cases supplied |
| --- | --- |
| `acceptedAnnotations()` | 32 complete-model cases assembled from the four accepted files and explicit variants. |
| `scoreCases()` | Optional-score inputs with expected Float text, combined RESULT text and normalized score terms. |
| `sourceReferences()` | Supplied source identities and the identities expected after completion/reference validation. |
| `identityInputs()` | Four received inputs paired with independently expected accepted annotations. |
| `partialIdentityInput()` | Input retaining its supplied long source hash while clearing identities that preparation must complete. |
| `unicodeInput()` | Received segment order that differs from the expected accepted order, including two equal-hash segments with distinct values. |

The Unicode cases distinguish Java string order from the unsigned UTF-8 byte order used for accepted Accumulo segment-key ordering. Existing model tests verify the inputs and literal answers. They do not execute common preparation to complete identities or sort the received annotation.

### Invalid inputs and the stage that must reject them

`setupFailures()` assembles invalid configuration inputs. `preparationFailures()` supplies 22 malformed or incompatible annotation inputs for later preparation rejection tests. Each row names the behavior it is intended to exercise. Its reason string is a test description, not a public error code.

Some specific rejection paths are already exercised by the semantic compiler/projector tests, such as unsupported selectors and incompatible formats. Complete registry setup and common preparation have separate responsibilities. Merely constructing an invalid JSON document does not verify those implementations.

### Reusing the test data

The core module publishes these resources and the public `AnnotationProjectionTestCases` helper in its `tests` JAR. Ingest and service tests can depend on that artifact with Maven type `test-jar`, scope `test`, and the matching DataWave release version. Core test classes are excluded, so consumers write tests for their own integration path. The helper and resources are excluded from the production JAR.

A test JAR does not export the owner's test-scoped dependencies. Consumer tests must provide the JUnit and Jackson dependencies used by the shared helper. The [module README](../../../../README.md#ownership-and-reuse) explains artifact ownership and reuse.

## Structured extensions and schema cases

### Native values and stored strings

`AnnotationV1` stores one string per extension key. The examples distinguish two preparation input modes:

| Mode | Input representation |
| --- | --- |
| `NATIVE` | Extension entries retain their JSON types: objects, arrays, strings, numbers or Booleans. Preparation encodes declared JSON entries into stored strings. |
| `STORED` | The entry already contains its stored string. A declared JSON entry is decoded once for field selection. |

Compare [extension-exact-native.json](raw/extension-exact-native.json) with [extension-exact-stored.json](raw/extension-exact-stored.json). The `stats` entry is a JSON object in the native example and JSON text inside a string in the stored example. Both represent the same exact number `9007199254740993`, Boolean `false`, and array containing `12.5000`, `1e-10000` and the string `"12"`.

The tests use `BigInteger` and `BigDecimal` when reading these examples so that large integers and small decimals are not rounded through floating-point conversion. Numeric values, Boolean values and strings retain their distinct types. Undeclared entries remain stored text, even when the text looks like JSON.

The [undeclared-structure example](raw/extension-undeclared-opaque-structures.json) covers native maps and arrays preserved as opaque JSON text rather than rejected. A projection needs a declared decoded interpretation to select their nested properties; it does not infer one from the stored string. The [mixed-array example](raw/extension-native-mixed-array.json) covers numbers, Booleans, strings and nested structures within one array.

The model tests compare native, stored and independently authored decoded examples. The projector tests supply prepared decoded values and verify field selection and formatting. Production input adaptation, codec lookup and decoding under preparation limits still need integration tests.

### Schema defaults, optional presence and wrappers

[presence-vectors.json](schema/presence-vectors.json) describes cases such as an absent score versus an explicitly supplied zero, an absent versus empty platform, and default point coordinates. These checks use protobuf presence/default behavior; an optional getter returning zero does not establish that the property was supplied.

The example in [all-fields-annotation.json](schema/all-fields-annotation.json) and the wrapper tests cover the field inventory in [inventory.json](schema/inventory.json). `AnnotationList`, `SegmentList` and `AnnotationMessage` are checked for JSON/binary serialization. They are not evidence that a reader extracts annotation events from every wrapper. Raw `SegmentList` input and mask/removal operations are outside ordinary ingest coverage.

The all-fields example combines boundary properties for schema coverage. It is not an ordinary valid annotation to use as an ingest acceptance case.

## Verification and current coverage

### Run the shared core tests

From the worktree root:

```sh
mvn -o -B -pl core/annotation,core/annotation-projection -am test \
  -Dtest=AnnotationProjectorTest,AnnotationProjectionFixtureContractTest,AnnotationUtilsSortSegmentsTest,SegmentValueCompatibilityTest \
  -Dsurefire.failIfNoSpecifiedTests=false -DskipFormat \
  -Dmaven.build.cache.enabled=false
```

`-am` builds the selected modules' reactor dependencies, including artifacts that may not yet be installed at the current DataWave version. `-o` assumes Maven dependencies are already cached; omit it when Maven needs to resolve them. The formatter is disabled for this focused test run, while Checkstyle still executes.

### What passing tests establish

| Test class | Behavior exercised |
| --- | --- |
| [AnnotationProjectionFixtureContractTest](../../java/datawave/annotation/projection/AnnotationProjectionFixtureContractTest.java) | Existing model parsing and validation, JSON/binary round trips, optional presence, literal hashes, exact-number examples and boundary/schema controls. |
| [AnnotationProjectorTest](../../java/datawave/annotation/projection/AnnotationProjectorTest.java) | Production semantic compilation and projection against independent answers; grouping, formatting, order, repeated values, prepared-state ownership, guard failures and output-callback failures. |

The model tests also establish that quoted nonfinite scores can reach protobuf Float values. The projector rejects present nonfinite scores defensively. Common preparation must still enforce rejection on every participating route, including annotations without a projection profile.

Core tests do not instantiate DataWave Types, run Hadoop readers or submit Accumulo mutations. Mutation/count expectations, the independent-route profile, reader cases and serializer checks belong to the [ingest test resources](../../../../../../warehouse/ingest-annotation/src/test/resources/annotation-projection/README.md).

### Integration coverage to add

Later tests must exercise common preparation, complete profile/codec loading, event/index/term-frequency writes, handler/mapper counts, diagnostics, Analyzer tokenization, queries and storage readback. Both ingest and service paths must persist and project the same final accepted annotation and identities.

Annotation service tests should reuse the shared inputs and verify their own API/message handling and coordinated persistence/index writes. Producer-specific output compatibility belongs to independently maintained consumer tests.
