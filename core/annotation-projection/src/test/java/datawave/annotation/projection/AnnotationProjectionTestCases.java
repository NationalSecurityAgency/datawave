package datawave.annotation.projection;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.params.provider.Arguments;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.util.JsonFormat;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.annotation.protobuf.v1.BoundaryType;
import datawave.annotation.protobuf.v1.Point;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.protobuf.v1.SegmentBoundary;

/**
 * Supplies reusable annotation test inputs and independently written expected answers. A test fixture is the prepared context for a test: here, built
 * annotation examples, argument rows and resource files that the core and ingest tests can share.
 * <p>
 * Builders assemble inputs and literal accepted-state examples; they do not run preparation, projection, hashing or normalization to calculate an expected
 * answer. This helper and the shared resources are published only in the test artifact, so service tests can reuse them as well.
 */
public final class AnnotationProjectionTestCases {
    public static final String ROOT = "/annotation-projection/";
    public static final ObjectMapper JSON = new ObjectMapper();
    public static final String NATIVE_SHORT = "228F1657";
    public static final String NATIVE_LONG = "E432F13DBF765D8019951DCADB809334";

    /** Shared configuration resource containing the synthetic extraction, binary-label and multiclass-label profiles. */
    public static final String BASELINE_PROFILES_RESOURCE = "configuration/baseline-profiles.json";

    /** Identifies preparation cases with NaN or infinity scores; this is a test intent label, not a public error code. */
    public static final String NONFINITE_SCORE_INTENT = "NONFINITE_SCORE";

    private AnnotationProjectionTestCases() {}

    /**
     * Reads a test resource as UTF-8 text.
     *
     * @param resource
     *            the path relative to the annotation-projection resource directory, such as {@code raw/extraction-single-region.json}
     * @return the complete resource text
     * @throws IOException
     *             if the resource is missing or cannot be read
     */
    public static String text(String resource) throws IOException {
        try (InputStream stream = AnnotationProjectionTestCases.class.getResourceAsStream(ROOT + resource)) {
            if (stream == null) {
                throw new IOException("Missing fixture: " + resource);
            }
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    /**
     * Reads a JSON test resource into a tree that tests can inspect or modify.
     *
     * @param resource
     *            the path relative to the annotation-projection resource directory
     * @return the parsed JSON tree
     * @throws IOException
     *             if the resource cannot be read or its JSON cannot be parsed
     */
    public static JsonNode read(String resource) throws IOException {
        return JSON.readTree(text(resource));
    }

    /**
     * Parses an annotation test resource with the strict protobuf JSON parser. Unknown annotation properties cause parsing to fail.
     *
     * @param resource
     *            the path relative to the annotation-projection resource directory
     * @return the built annotation represented by the resource
     * @throws IOException
     *             if the resource cannot be read or parsed as AnnotationV1 JSON
     */
    public static Annotation annotation(String resource) throws IOException {
        Annotation.Builder builder = Annotation.newBuilder();
        JsonFormat.parser().merge(text(resource), builder);
        return builder.build();
    }

    /**
     * Copies the complete synthetic BINARY_LABEL fixture into a new builder, then assigns the requested annotation type and ID. Its source, segment, value and
     * ingest metadata provide a common starting point for synthetic cases.
     */
    public static Annotation.Builder content(String type, String id) throws IOException {
        return annotation("accepted/binary-label.json").toBuilder().setAnnotationType(type).setAnnotationId(id);
    }

    /**
     * Copies an accepted model into a builder for input that has not been bound to an ingest event. It removes the shard, datatype, UID, annotation metadata,
     * top-level source reference and source metadata so that later preparation tests can exercise their assignment.
     * <p>
     * The embedded source details, source hashes, segments and values remain intact. The original accepted model is not modified.
     *
     * @param accepted
     *            the complete model to use as the starting point
     * @param suppliedId
     *            true to retain the annotation ID as a supplied input identity; false to clear it for a missing-ID completion case
     * @return a new mutable input builder; this method performs no preparation or identity calculation
     */
    public static Annotation.Builder unbound(Annotation accepted, boolean suppliedId) {
        Annotation.Builder input = accepted.toBuilder().clearShard().clearDataType().clearUid().clearMetadata().clearAnalyticSourceHash();
        input.getSourceBuilder().clearMetadata();
        if (!suppliedId) {
            input.clearAnnotationId();
        }
        return input;
    }

    /**
     * Builds the single-region extraction input with a supplied long source hash but missing annotation, short source, segment and value identities. The source
     * details, boundary and value content remain available for later missing-only completion. This method clears fields; it does not calculate their expected
     * replacements.
     */
    public static Annotation partialIdentityInput() throws IOException {
        Annotation.Builder input = unbound(annotation("accepted/extraction-single-region.json"), false);
        input.getSourceBuilder().clearAnalyticHash();
        input.getSegmentsBuilder(0).clearSegmentHash().getValuesBuilder(0).clearValueHash();
        return input.build();
    }

    /**
     * Supplies the identity-completion and segment-ordering inputs alongside their independently expected accepted models. Each row contains a case ID, an
     * unbound input and its accepted answer. The partial-identity input has missing child hashes; the Unicode inputs deliberately require segment reordering.
     * <p>
     * The expected answers are built from literal identities and the accepted-order specimen. No preparation, sorting or hashing produces those answers.
     */
    public static Stream<Arguments> identityInputs() throws IOException {
        Annotation extraction = annotation("accepted/extraction-single-region.json");
        Annotation completedExtraction = extraction.toBuilder().setAnnotationId("F6B9F68B").build();
        //@formatter:off
        return Stream.of(
                Arguments.of("extraction-missing-annotation-id", unbound(extraction, false).build(), completedExtraction),
                Arguments.of("identity-partial-source-and-children", partialIdentityInput(), completedExtraction),
                Arguments.of("ordering-unicode-supplied-id", unicodeInput(true), unicode(true)),
                Arguments.of("ordering-unicode-missing-id", unicodeInput(false), unicode(false))
        );
        //@formatter:on
    }

    /**
     * Builds a synthetic BINARY_LABEL annotation with the requested optional score on its first value. A null score removes the field; zero and negative zero
     * remain explicitly present. The value text and supplied source, segment and value identities are preserved.
     *
     * @param suffix
     *            the case label appended to {@code Score-} to form the annotation ID
     * @param score
     *            the score to assign, or null to represent an absent score
     * @return the complete model for the score case, without projection or normalization
     */
    public static Annotation scored(String suffix, Float score) throws IOException {
        Annotation.Builder builder = content("BINARY_LABEL", "Score-" + suffix);
        if (score == null) {
            builder.getSegmentsBuilder(0).getValuesBuilder(0).clearScore();
        } else {
            builder.getSegmentsBuilder(0).getValuesBuilder(0).setScore(score);
        }
        return builder.build();
    }

    /**
     * Supplies score cases with independently written formatting and normalization answers. Each row contains the case suffix, input Float, expected score
     * text, expected RESULT text and expected normalized score. Null answers belong to the absent-score case.
     */
    public static Stream<Arguments> scoreCases() {
        //@formatter:off
        return Stream.of(
                Arguments.of("absent", null, null, null, null),
                Arguments.of("positive-zero", 0.0f, "0.0", "present:0.0", "+AE0"),
                Arguments.of("negative-zero", -0.0f, "-0.0", "present:-0.0", "+AE0"),
                Arguments.of("negative", -1.25f, "-1.25", "present:-1.25", "!ZE8.75"),
                Arguments.of("scaled", 75.0f, "75.0", "present:75.0", "+bE7.5")
        );
        //@formatter:on
    }

    /**
     * Describes which source identities are supplied and which identities should be present after completion. Each row contains the variant name, supplied
     * short source hash, supplied long source hash, top-level source reference, expected short hash and expected long hash, in that order.
     * <p>
     * Null input columns mean that the identity is missing. Expected columns are literal answers; this table does not calculate hashes or reconcile references.
     */
    public static Stream<Arguments> sourceReferences() {
        //@formatter:off
        return Stream.of(
                Arguments.of("supplied-equal", "aSource", "source-long-v1", "source-long-v1", "aSource", "source-long-v1"),
                Arguments.of("embedded-only", "aSource", "source-long-v1", null, "aSource", "source-long-v1"),
                Arguments.of("top-only-equal", "aSource", null, NATIVE_LONG, "aSource", NATIVE_LONG),
                Arguments.of("both-long-missing", "aSource", null, null, "aSource", NATIVE_LONG),
                Arguments.of("source-pair-missing", null, null, null, NATIVE_SHORT, NATIVE_LONG),
                Arguments.of("short-missing", null, "source-long-v1", null, NATIVE_SHORT, "source-long-v1")
        );
        //@formatter:on
    }

    /**
     * Builds the independently expected accepted state for a source-reference case. It assigns the supplied expected hashes to the embedded source and makes
     * the annotation's source reference agree with the expected long hash. It does not perform source completion.
     */
    public static Annotation sourceAccepted(String variant, String shortHash, String longHash) throws IOException {
        Annotation.Builder builder = content("FUTURE", "Source-" + variant).setAnalyticSourceHash(longHash);
        builder.getSourceBuilder().setAnalyticHash(shortHash).setAnalyticSourceHash(longHash);
        return builder.build();
    }

    /**
     * Builds an input for testing source completion and reference validation before event binding. The annotation ID is retained, but source hashes and the
     * top-level reference are set exactly as requested rather than being reconciled.
     * <p>
     * A null identity argument becomes the protobuf-default empty string, representing a missing identity. The JSON printer normally omits those empty fields.
     * Later preparation must decide which missing identities to complete and whether the supplied reference agrees.
     */
    public static Annotation sourceInput(String variant, String shortHash, String longHash, String reference) throws IOException {
        Annotation.Builder builder = unbound(content("FUTURE", "Source-" + variant).build(), true);
        AnnotationSource.Builder source = builder.getSourceBuilder();
        source.setAnalyticHash(shortHash == null ? "" : shortHash).setAnalyticSourceHash(longHash == null ? "" : longHash);
        return builder.setAnalyticSourceHash(reference == null ? "" : reference).build();
    }

    /**
     * Builds the accepted model for the Unicode segment-ordering control. Two segments use U+E000 as their supplied segment hash, followed by a segment whose
     * hash is U+10000. This is unsigned UTF-8 order, as used by Accumulo; Java string order places U+10000 first.
     * <p>
     * The two equal-hash segments retain their relative order and distinct values. The model already has an annotation ID in both variants: one retains an
     * opaque supplied ID, and the other uses the literal expected native ID for the original missing-ID input. This helper does not sort or calculate that ID.
     *
     * @param suppliedId
     *            true for the supplied-ID control; false for the accepted result of the missing-ID control
     * @return the complete model in the expected accepted segment order
     */
    public static Annotation unicode(boolean suppliedId) throws IOException {
        Annotation.Builder builder = content("FUTURE", suppliedId ? "Opaque.Annotation:ID" : "91BA0DDF");
        Segment template = builder.getSegments(0);
        builder.clearSegments();
        for (String label : List.of("private-use-first", "private-use-second", "supplementary")) {
            Segment.Builder segment = template.toBuilder().setSegmentHash(label.equals("supplementary") ? "\ud800\udc00" : "\ue000");
            segment.getValuesBuilder(0).setValue(label).setValueHash("v" + label);
            builder.addSegments(segment);
        }
        return builder.build();
    }

    /**
     * Builds the Unicode ordering input with U+10000 first, followed by the two U+E000 segments in their original relative order. Preparation must move the
     * supplementary segment after both private-use segments without losing or exchanging their distinct payloads.
     *
     * @param suppliedId
     *            true to retain the opaque supplied annotation ID; false to leave the ID missing for completion
     * @return an unbound input in the original received order, without sorting or identity calculation
     */
    public static Annotation unicodeInput(boolean suppliedId) throws IOException {
        Annotation accepted = unicode(suppliedId);
        //@formatter:off
        return unbound(accepted, suppliedId)
                .clearSegments()
                .addSegments(accepted.getSegments(2))
                .addSegments(accepted.getSegments(0))
                .addSegments(accepted.getSegments(1))
                .build();
        //@formatter:on
    }

    /**
     * Builds a complete FUTURE annotation and adds the provided stored extension entries to its first value. Each entry is retained as the supplied string,
     * including any JSON text. This helper does not encode native structures, decode declared keys or project extension fields.
     *
     * @param id
     *            the annotation ID for this synthetic case
     * @param extensions
     *            stored string entries to add to the first value's extension map
     * @return the complete model containing those entries
     */
    public static Annotation extended(String id, Map<String,String> extensions) throws IOException {
        Annotation.Builder builder = content("FUTURE", id);
        builder.getSegmentsBuilder(0).getValuesBuilder(0).putAllExtension(extensions);
        return builder.build();
    }

    /**
     * Loads the stored extension strings from the exact-number fixture without decoding or rewriting their numeric spellings.
     */
    public static Map<String,String> exactStoredExtensions() throws IOException {
        return annotation("raw/extension-exact-stored.json").getSegments(0).getValues(0).getExtensionMap();
    }

    /**
     * Supplies the complete accepted models used by the baseline validation and round-trip tests. Each row contains a stable case ID and its built Annotation.
     * Complete synthetic examples are loaded from resources; variants are assembled with the helpers in this class.
     * <p>
     * Source-reference variants use the expected hash columns from {@link #sourceReferences()}, not its supplied-input columns. These rows describe expected
     * accepted state; constructing them does not execute ingest preparation, projection or identity completion.
     */
    public static Stream<Arguments> acceptedAnnotations() throws IOException {
        Annotation extraction = annotation("accepted/extraction-single-region.json");
        //@formatter:off
        Stream<Arguments> fixed = Stream.of(
                Arguments.of("extraction-single-region", extraction),
                Arguments.of("extraction-missing-annotation-id", extraction.toBuilder().setAnnotationId("F6B9F68B").build()),
                Arguments.of("extraction-multiple-regions", annotation("accepted/extraction-multiple-regions.json")),
                Arguments.of("binary-label", annotation("accepted/binary-label.json")),
                Arguments.of("multiclass-label", annotation("accepted/multiclass-label.json")),
                Arguments.of("score-absent", scored("absent", null)),
                Arguments.of("score-positive-zero", scored("positive-zero", 0.0f)),
                Arguments.of("score-negative-zero", scored("negative-zero", -0.0f)),
                Arguments.of("score-negative", scored("negative", -1.25f)),
                Arguments.of("score-scaled", scored("scaled", 75.0f)),
                Arguments.of("format-compose-before-trim", paddedLabel()),
                Arguments.of("format-source-hash-before-trim", paddedSource()),
                Arguments.of("profile-miss", content("FUTURE", "Future-v1").build()),
                Arguments.of("identity-partial-source-and-children", extraction.toBuilder().setAnnotationId("F6B9F68B").build()),
                Arguments.of("ordering-unicode-supplied-id", unicode(true)),
                Arguments.of("ordering-unicode-missing-id", unicode(false)),
                Arguments.of("extension-exact-native", extended("Extension-v1", Map.of(
                        "stats", "{\"count\":9007199254740993,\"enabled\":false,\"values\":[12.5,1E-10000,\"12\"]}",
                        "opaque", "{\"enabled\":false}"
                ))),
                Arguments.of("extension-exact-stored", extended("Extension-v1", exactStoredExtensions())),
                Arguments.of("extension-native-mixed-array", extended("Array-v1", Map.of("stats", "[12,false,\"12\",{\"nested\":[true]}]"))),
                Arguments.of("extension-undeclared-opaque-structures", extended("Opaque-v1", Map.of(
                        "properties", "{\"nested\":{\"key\":\"value\"}}",
                        "items", "[1,false,\"1\"]"
                ))),
                Arguments.of("extension-numeric-boolean-projection", extended("Extension-v1", exactStoredExtensions())),
                Arguments.of("boundary-all", bounded(BoundaryType.ALL)),
                Arguments.of("boundary-points", bounded(BoundaryType.POINTS)),
                Arguments.of("boundary-time_milli", bounded(BoundaryType.TIME_MILLI)),
                Arguments.of("boundary-text_char", bounded(BoundaryType.TEXT_CHAR)),
                Arguments.of("independent-value-routes", extraction)
        );
        //@formatter:on
        List<Arguments> references = new ArrayList<>();
        for (Arguments row : (Iterable<Arguments>) sourceReferences()::iterator) {
            Object[] values = row.get();
            references.add(Arguments.of("source-reference-" + values[0], sourceAccepted((String) values[0], (String) values[4], (String) values[5])));
        }
        return Stream.concat(fixed, references.stream());
    }

    /**
     * Keeps padding around the value label and supplies an explicit zero score. The case distinguishes composing RESULT before trimming from trimming each
     * operand first: the expected result is {@code label :0.0}, with the space before the colon retained.
     */
    public static Annotation paddedLabel() throws IOException {
        Annotation.Builder builder = content("BINARY_LABEL", "Padded-label");
        builder.getSegmentsBuilder(0).getValuesBuilder(0).setValue(" label ").setScore(0.0f);
        return builder.build();
    }

    /**
     * Supplies a padded model name and a blank platform. SOURCE should skip the platform, compose {@code " model :engine"}, hash that untrimmed label, then
     * trim the emitted text to {@code "model :engine"}. The source details remain padded in the annotation.
     */
    public static Annotation paddedSource() throws IOException {
        Annotation.Builder builder = content("BINARY_LABEL", "Padded-source");
        builder.getSourceBuilder().setModel(" model ").setEngine("engine").setPlatform("   ");
        return builder.build();
    }

    /**
     * Builds a FUTURE annotation with a representative boundary. POINTS contains one point with default zero coordinates; TIME_MILLI and TEXT_CHAR contain
     * explicit zero starts and nonzero ends. ALL has no span or points. No boundary or spatial fields are projected here.
     */
    public static Annotation bounded(BoundaryType type) throws IOException {
        SegmentBoundary.Builder boundary = SegmentBoundary.newBuilder().setBoundaryType(type);
        if (type == BoundaryType.POINTS) {
            boundary.addPoints(Point.getDefaultInstance());
        } else if (type == BoundaryType.TIME_MILLI || type == BoundaryType.TEXT_CHAR) {
            boundary.setStart(0).setEnd(type == BoundaryType.TIME_MILLI ? 10 : 1);
        }
        Annotation.Builder builder = content("FUTURE", "Boundary-" + type.name());
        builder.getSegmentsBuilder(0).setBoundary(boundary);
        return builder.build();
    }

    /**
     * Supplies invalid configuration inputs for later setup-rejection tests. Each row contains a case ID, the JSON document and an expected failure category.
     * Changes to valid baseline profiles isolate a policy conflict, protected destination or unsupported selector; duplicate-key syntax is kept as literal
     * text.
     * <p>
     * Building these inputs does not run the registry loader or establish that it rejects them.
     */
    public static Stream<Arguments> setupFailures() throws IOException {
        ObjectNode conflict = (ObjectNode) read(BASELINE_PROFILES_RESOURCE);
        ((ObjectNode) conflict.path("profiles").get(1).path("datawave").path("fields").path("ANALYTIC_HASH")).put("reverseIndex", false);
        ObjectNode protectedField = (ObjectNode) read(BASELINE_PROFILES_RESOURCE);
        ((ObjectNode) protectedField.path("profiles").get(0).path("semantic").path("bindings").get(0)).put("field", "LOAD_DATE");
        ObjectNode policies = (ObjectNode) protectedField.path("profiles").get(0).path("datawave").path("fields");
        policies.set("LOAD_DATE", policies.remove("EXTRACTED_TEXT"));
        ObjectNode selector = (ObjectNode) read(BASELINE_PROFILES_RESOURCE);
        ((ObjectNode) selector.path("profiles").get(0).path("semantic").path("bindings").get(0).path("expression")).put("path", "$..value");
        return Stream.of(Arguments.of("setup-shared-policy-conflict", conflict.toString(), "SHARED_POLICY_CONFLICT"),
                        Arguments.of("setup-protected-field", protectedField.toString(), "PROTECTED_DESTINATION"),
                        Arguments.of("setup-duplicate-json-key", text("configuration/duplicate-key-profiles.json"), "DUPLICATE_JSON_KEY"),
                        Arguments.of("setup-unsupported-selector", selector.toString(), "UNSUPPORTED_SELECTOR"));
    }

    /**
     * Creates a mutable JSON input for a synthetic annotation of the requested type, before event binding. It retains the explicit
     * {@code Supplied-failure-control} annotation ID and the embedded source details, but removes the event identity, ingest metadata and top-level source
     * reference through {@link #unbound(Annotation, boolean)}.
     * <p>
     * Rejection cases modify this JSON tree so that they can represent unknown properties or other inputs that a built protobuf model cannot retain.
     *
     * @param type
     *            the exact annotation type to put in the input
     * @return a new mutable JSON tree with the shared synthetic segment and value
     */
    public static ObjectNode rawContent(String type) throws IOException {
        return (ObjectNode) JSON.readTree(JsonFormat.printer().print(unbound(content(type, "Supplied-failure-control").build(), true)));
    }

    /**
     * Returns the mutable JSON object for the first value in the first segment. This is a child of the supplied tree, so changes to it also change the raw
     * input. The method assumes the segment/value structure created by {@link #rawContent(String)}; it does not create missing arrays or entries.
     */
    public static ObjectNode value(ObjectNode raw) {
        return (ObjectNode) raw.path("segments").get(0).path("values").get(0);
    }

    /**
     * Supplies malformed or incompatible annotation inputs for preparation-rejection tests. Each row contains a stable case ID, raw JSON and the expected
     * failure category. Cases cover nonfinite scores on profiled and unprofiled types, source references, identities, required content, boundaries and
     * extensions.
     * <p>
     * The category describes the test's intent rather than a public error-code contract. Constructing these rows does not execute preparation or verify
     * rejection.
     */
    public static Stream<Arguments> preparationFailures() throws IOException {
        List<Arguments> rows = new ArrayList<>();
        for (String score : List.of("NaN", "Infinity", "-Infinity")) {
            for (String type : List.of("BINARY_LABEL", "FUTURE")) {
                ObjectNode raw = rawContent(type);
                value(raw).put("score", score);
                //@formatter:off
                rows.add(
                        Arguments.of(
                                "score-nonfinite-"
                                        + score.toLowerCase(Locale.ROOT).replace("-", "negative-")
                                        + "-" + type.toLowerCase(Locale.ROOT),
                                raw.toString(),
                                NONFINITE_SCORE_INTENT
                        )
                );
                //@formatter:on
            }
        }
        //@formatter:off
        List<String> identifiers = List.of(
                "source-conflict",
                "group-dot",
                "group-colon",
                "id-whitespace",
                "id-control",
                "missing-source",
                "blank-source-engine",
                "unknown-property",
                "mask-empty-values",
                "unknown-boundary",
                "invalid-all-span"
        );
        //@formatter:on
        for (String id : identifiers) {
            ObjectNode raw = rawContent("BINARY_LABEL");
            String reason;
            switch (id) {
                case "source-conflict":
                    raw.put("analyticSourceHash", "different-source");
                    reason = "SOURCE_REFERENCE_CONFLICT";
                    break;
                case "group-dot":
                    ((ObjectNode) raw.path("segments").get(0)).put("segmentHash", "b.bad");
                    reason = "INVALID_GROUP_ID";
                    break;
                case "group-colon":
                    value(raw).put("valueHash", "v:bad");
                    reason = "INVALID_GROUP_ID";
                    break;
                case "id-whitespace":
                case "id-control":
                    raw.put("annotationId", id.equals("id-whitespace") ? "bad id" : "bad\0id");
                    reason = "INVALID_ID";
                    break;
                case "missing-source":
                    raw.remove("source");
                    reason = "COMPLETE_SOURCE_REQUIRED";
                    break;
                case "blank-source-engine":
                    ((ObjectNode) raw.path("source")).put("engine", "");
                    reason = "COMPLETE_SOURCE_REQUIRED";
                    break;
                case "unknown-property":
                    raw.put("unrecognized", "not allowed");
                    reason = "UNKNOWN_SCHEMA_PROPERTY";
                    break;
                case "mask-empty-values":
                    ((ObjectNode) raw.path("segments").get(0)).putArray("values");
                    reason = "COMPLETE_SEGMENT_VALUES_REQUIRED";
                    break;
                default:
                    ObjectNode boundary = (ObjectNode) raw.path("segments").get(0).path("boundary");
                    boundary.put("boundaryType", id.equals("unknown-boundary") ? "UNKNOWN" : "ALL");
                    if (id.equals("invalid-all-span")) {
                        boundary.put("start", 0);
                    }
                    reason = "INVALID_BOUNDARY";
            }
            rows.add(Arguments.of(id, raw.toString(), reason));
        }
        ObjectNode topOnly = rawContent("FUTURE");
        ((ObjectNode) topOnly.path("source")).remove("analyticSourceHash");
        topOnly.put("analyticSourceHash", "top-reference-does-not-match-native-source");
        rows.add(Arguments.of("source-reference-top-only-conflict", topOnly.toString(), "SOURCE_REFERENCE_CONFLICT"));
        for (String id : List.of("extension-null-entry", "extension-null-element", "extension-malformed")) {
            ObjectNode raw = rawContent("FUTURE");
            ObjectNode extension = value(raw).putObject("extension");
            if (id.equals("extension-null-entry")) {
                extension.putNull("stats");
            } else {
                extension.put("stats", id.equals("extension-null-element") ? "[1,null]" : "{bad");
            }
            rows.add(Arguments.of(id, raw.toString(), "INVALID_DECLARED_EXTENSION"));
        }
        ObjectNode vector = rawContent("FUTURE");
        value(vector).putObject("vector").put("format", "test-format").put("data", "AA==");
        rows.add(Arguments.of("baseline-vector-member-rejected", vector.toString(), "UNKNOWN_SCHEMA_PROPERTY"));
        return rows.stream();
    }
}
