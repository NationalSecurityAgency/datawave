# Annotation projection fixtures

The goal of annotation projection is to take data in the annotation JSON format and project selected values into named DataWave fields. These fields will be used to index both annotations and truthmarks so their contents can be queried in DataWave. Projection profiles define which values become fields, the types used to normalize and index them, and the indexing routes to use.

These synthetic fixtures describe the shared projection contract for warehouse ingest and annotation microservices. They check annotation models and independently authored expected answers without requiring a particular producer, model or algorithm. Core tests exercise existing parsing, validation, hashing and serialization APIs. Consumer tests reuse these inputs and logical answers to check normalization and writes; a complete preparation, projection, indexing and retrieval pipeline remains to be implemented.

## Files and ordinary cases

| Location | Purpose |
| --- | --- |
| `raw/extraction-*.json` | Independently authored single-region and multiple-region text extraction inputs. |
| `accepted/` | Four complete models: two extraction examples, a binary label and a multiclass label. |
| `expected/*.json` | Five small logical-field goldens, including literal normalized terms for consumer checks. |
| `configuration/` | Three neutral profiles with fourteen base-field policies, explicit codecs and literal duplicate-key JSON. |
| `raw/extension-*.json` | Native/stored exact-number, Boolean, mixed-array and opaque-structure inputs. |
| `schema/`, `oracles/` | Pinned field inventory, all-field specimen, presence and literal hash/format answers. |
| `provenance.json` | Synthetic authorship and content hashes for the two raw extraction examples. |

The `TEXT_EXTRACTION` profile declares extracted text and a content kind from POINTS regions. The single-region example uses `PRINTED_TEXT`; the multiple-region example uses `TEXT_IN_SCENE`. The `BINARY_LABEL` and `MULTICLASS_LABEL` profiles declare a selected category, optional score, composed result and source fields. These are synthetic content labels, unrelated to visibility or access control. The multiclass baseline contains one selected category; the schema permits multiple values without imposing classifier-specific probability rules.

Raw inputs represent annotations before event binding. Accepted models describe the expected complete state, including event identity, metadata and annotation/source/segment/value identities. A grouped value field uses the source's `analyticHash`, the segment's `segmentHash` and the value's `valueHash` as its three-part name suffix. Source-label fields use separately specified synthetic group identities.

`AnnotationProjectionTestCases` is a public test helper using ordinary Java builders and JUnit argument rows. It and these resources are published in the core module's test JAR for consumer tests; core test classes are excluded, and fixtures are not part of the production JAR. `acceptedAnnotations()` supplies 32 accepted model cases; `scoreCases()` and `sourceReferences()` make small variations explicit. Builders assemble inputs and literal expected states. No production preparation, projection, hashing or normalization generates an expected answer.

`identityInputs()` pairs four unbound inputs with their accepted answers. `partialIdentityInput()` retains the supplied long source hash while clearing annotation, short source, segment and value identities. `unicodeInput()` puts the supplementary segment first and preserves the relative order of the two private-use segments. The tests check that these inputs retain their intended differences. They do not complete identities or sort annotations through the preparation implementation.

`setupFailures()` changes one property of a valid baseline document or uses literal duplicate-key bytes. `preparationFailures()` supplies 22 inputs intended to be rejected by preparation, with labels describing the reason. Constructing these inputs does not verify that registry loading or preparation rejects them; reason strings are not a public error-code API.

## Independent comparisons

Logical fields are occurrences. Compare names, values, groups and multiplicity without assuming global publication order. Expected event and term-frequency names retain groups; ordinary field-index and global-index names use bases. Expected field answers are static. Do not refresh them by running the projector or handler being tested.

Mutation/count snapshots, the independent-route profile and malformed-reader input belong to `warehouse/ingest-annotation`. Its consumer suite checks Type normalization, Hadoop framing and existing persistence serializers using this shared test artifact. Service suites will check their own API/message and coordinated persistence/index behavior.

Native identity literals use the existing seed-zero Murmur3 rules and their specified UTF-8/unencoded-character inputs. Synthetic source/group labels and extraction boundaries/text have independently calculated answers. Supplied opaque identities remain preservation controls.

NATIVE encodes declared JSON once; STORED retains extension strings and decodes declared keys once. Exact specimens distinguish `9007199254740993`, `false`, `12.5000`, `1e-10000` and String "12". Comparisons use BigInteger/BigDecimal nodes to retain exact values and JSON kinds, including every mixed-array element and nested content. Undeclared structures remain opaque text.

Wrapper JSON/binary checks compose a shared all-field annotation. These tests verify schema serialization, not extraction of annotations from wrapper messages. Raw SegmentList ingestion is excluded and masks/removals are not positive ordinary ingest cases. The all-field specimen intentionally combines boundary properties for schema coverage rather than ordinary ingest validation.

## Verification

From the worktree root:

```sh
mvn -o -B -pl core/annotation,core/annotation-projection test \
  -Dtest=AnnotationProjectionFixtureContractTest,AnnotationUtilsSortSegmentsTest,SegmentValueCompatibilityTest \
  -Dsurefire.failIfNoSpecifiedTests=false -Dspotless.skip=true -Dsort.skip=true
```

The core tests execute existing schema/binary/presence validation, literal hashes, score formatting, exact numeric comparisons and boundary controls. They establish that quoted nonfinite input reaches a typed Float, but do not verify its rejection during preparation. Core fixture checks do not instantiate Types, run Hadoop readers or construct persistence mutations.

Separate integration tests are needed for prepared-state reuse, profile loading and expression evaluation, routed writes and mapper counters, diagnostics, Analyzer tokenization and term-frequency output, queries, and persistence/readback through storage. Persistence and projection must use the same final prepared annotation and identities in both ingest and service paths. Producer-specific output compatibility belongs to independently maintained consumer tests.
