# Shared annotation projection test data

Annotation projection selects values from annotation JSON and turns them into named DataWave fields for indexing and querying annotations and truthmarks. A projection profile identifies the values to select and how to name, format and group the resulting fields. Its DataWave policies specify the Types used for normalization and the event/index destinations that receive output.

This directory supplies examples and expected answers shared by warehouse ingest and annotation microservice tests. The examples are synthetic, so they exercise extraction and content-labeling shapes without depending on a particular producer, model or algorithm.

The tests and helper classes use the word **fixture** for the prepared test context. The files here are its data: inputs, configuration examples and expected answers. They do not run tests or implement the projection contract. See the [module overview](../../../../README.md) for ownership and reuse.

## What the files represent

| Location | Purpose |
| --- | --- |
| `raw/extraction-*.json` | Authored single-region and multiple-region text extraction inputs, before event binding. |
| `accepted/` | Four expected complete annotations: two extraction examples, a binary label and a multiclass label. |
| `expected/*.json` | Five sets of expected logical fields, including literal normalized terms for consumer checks. |
| `configuration/` | Profile and extension-codec examples, including three baseline profiles with fourteen distinct base-field policies and an invalid duplicate-key document. |
| `raw/extension-*.json` | Native JSON and stored-string examples covering exact numbers, Booleans, mixed arrays and undeclared structures. |
| `schema/` | Expected protobuf field inventory, an example containing every declared field, and optional-presence/default cases. |
| `oracles/` | Independently authored hash, formatting and decoded-extension answers. An oracle is the expected result used to judge a test. |
| `provenance.json` | Synthetic authorship and content hashes for the two raw extraction examples. |

The `TEXT_EXTRACTION` profile selects extracted text and a content kind from `POINTS` regions. The single-region example uses `PRINTED_TEXT`; the multiple-region example uses `TEXT_IN_SCENE`. The `BINARY_LABEL` and `MULTICLASS_LABEL` profiles select a category, optional score, composed result and source fields. These are algorithmic content labels, unrelated to visibility or access control. The multiclass example has one selected category; it does not impose classifier-specific probability rules.

“Raw” means the annotation has not yet been bound to a DataWave event. “Accepted” describes the expected state after preparation, including event identity, metadata and annotation/source/segment/value identities. Accepted examples are authored answers; their presence does not mean a preparation implementation produced them.

For a value field named `BASE.A.B.V`, `BASE` is the configured field name, `A` is `source.analyticHash`, `B` is the segment's `segmentHash`, and `V` is the value's `valueHash`. Source-label fields use the same source hash with a synthetic `ALL` boundary hash and a hash of the composed source label. That label is hashed before final trimming.

## How the cases are assembled

`AnnotationProjectionTestCases` uses ordinary Java builders and JUnit argument rows. It and these resources are published in the core module's test JAR for reuse by consumer tests. Core test classes are excluded from that JAR, and the helper and data are excluded from the production JAR.

`acceptedAnnotations()` supplies 32 complete-model cases, including variations assembled from the four stored annotations. `scoreCases()` supplies optional-score inputs and literal text/normalization answers. `sourceReferences()` identifies supplied and expected source identities. Builders assemble these inputs and answers; production preparation or projection does not generate them.

`identityInputs()` pairs four received inputs with their expected accepted annotations. `partialIdentityInput()` preserves the supplied long source hash and clears the identities that later preparation must complete. `unicodeInput()` places the supplementary-character segment before two private-use-character segments, while retaining the relative order of the two equal-hash segments. Existing tests check these input/output distinctions; they do not run the preparation implementation to complete or reorder them.

`setupFailures()` assembles invalid configuration inputs. `preparationFailures()` supplies 22 annotation inputs that later preparation tests must reject. The accompanying reason strings describe each case's intent and are not public error codes. Constructing these inputs does not verify rejection.

## Rules for expected answers

Each logical field is an occurrence with a name, group and value. Comparisons must retain repeated occurrences and multiple values without assuming a global publication order. The intended indexing contract preserves full grouping in event and term-frequency names; ordinary field-index and global-index names use the base. The current suite does not execute these index writes.

Keep expected field answers independent of the projector or handler being tested. Expected-answer files are sometimes called goldens; do not regenerate them by running the implementation they are intended to check.

Literal identity answers pin the existing seed-zero Murmur3 functions and their respective UTF-8 or unencoded-Java-character inputs. Synthetic source-label, boundary and value hashes have independently calculated answers. Cases with supplied opaque IDs expect those strings to remain unchanged.

Score cases distinguish absence, explicit zero and negative zero. They retain supplied finite values without assuming a probability range or rescaling them. Padding cases expect RESULT and SOURCE text to be composed before final trimming, rather than trimming each operand first.

`NATIVE` and `STORED` name the planned extension input modes. NATIVE input can contain JSON structures that must be encoded into stored strings once. STORED input already contains extension strings; declared keys are decoded once for selection. The examples distinguish `9007199254740993`, `false`, `12.5000`, `1e-10000` and the string `"12"`. Existing comparisons use `BigInteger`/`BigDecimal` nodes to retain exact values and JSON kinds, including mixed-array elements and nested content. The tests compare these representations; they do not execute the future codec bridge. Undeclared structures remain opaque text for projection.

Wrapper examples combine the same annotation into `AnnotationList`, `SegmentList` and `AnnotationMessage` messages. The tests verify JSON/binary serialization, not extraction from those wrappers. Raw `SegmentList` input and mask/removal operations are outside ordinary ingest coverage. The example containing every field combines boundary properties for schema coverage and is not an ordinary valid ingest example.

Mutation/count expectations, the independent-route profile and malformed-reader input belong to the [ingest test resources](../../../../../../warehouse/ingest-annotation/src/test/resources/annotation-projection/README.md). Service tests will reuse the shared inputs and verify their own API/message and persistence/index behavior.

## Verification

From the worktree root:

```sh
mvn -o -B -pl core/annotation,core/annotation-projection test \
  -Dtest=AnnotationProjectionFixtureContractTest,AnnotationUtilsSortSegmentsTest,SegmentValueCompatibilityTest \
  -Dsurefire.failIfNoSpecifiedTests=false -Dspotless.skip=true -Dsort.skip=true
```

`AnnotationProjectionFixtureContractTest` executes existing model parsing, annotation/source validation, binary round trips, optional-presence checks, literal hashes, score formatting, exact-number comparisons and boundary controls. It establishes that quoted nonfinite scores reach typed `Float` values; rejection during preparation remains to be verified. Core checks do not instantiate DataWave Types, run Hadoop readers or construct persistence mutations.

Later integration tests must execute preparation, profile loading, expression evaluation, routed writes, counters, diagnostics, Analyzer tokenization, term-frequency output, queries and storage readback. Both ingest and service paths must use the same final prepared annotation and identities for persistence and projection. Producer-specific output compatibility belongs to independently maintained consumer tests.
