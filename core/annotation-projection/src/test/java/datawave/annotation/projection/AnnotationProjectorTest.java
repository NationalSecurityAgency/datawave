package datawave.annotation.projection;

import static datawave.annotation.projection.AnnotationProjectionTestCases.BASELINE_PROFILES_RESOURCE;
import static datawave.annotation.projection.AnnotationProjectionTestCases.JSON;
import static datawave.annotation.projection.AnnotationProjectionTestCases.annotation;
import static datawave.annotation.projection.AnnotationProjectionTestCases.bounded;
import static datawave.annotation.projection.AnnotationProjectionTestCases.content;
import static datawave.annotation.projection.AnnotationProjectionTestCases.exactStoredExtensions;
import static datawave.annotation.projection.AnnotationProjectionTestCases.extended;
import static datawave.annotation.projection.AnnotationProjectionTestCases.paddedLabel;
import static datawave.annotation.projection.AnnotationProjectionTestCases.paddedSource;
import static datawave.annotation.projection.AnnotationProjectionTestCases.read;
import static datawave.annotation.projection.AnnotationProjectionTestCases.scored;
import static datawave.annotation.projection.AnnotationProjectionTestCases.unicode;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.PathNotFoundException;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.BoundaryType;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.protobuf.v1.SegmentValue;

/**
 * Executes {@link AnnotationProjector} with the semantic rules in {@code configuration/baseline-profiles.json} and compares its fields with independently
 * authored answers under {@code expected/}. Each field comparison includes the base name, grouping components, event value and number of repeated results.
 * <p>
 * Additional cases isolate optional-property presence, formatting, traversal order, grouping and resource limits. Test helpers construct inputs and collect
 * actual output; they do not calculate expected field values or hashes. DataWave Type normalization is checked separately in the ingest module.
 */
class AnnotationProjectorTest {
    private static final String TEST_FIELD = "TEST_FIELD";
    private static final ObjectMapper EXACT_JSON = new ObjectMapper().enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private final AnnotationProjector projector = new AnnotationProjector();

    /**
     * Projects each accepted extraction or content-label annotation with its configured profile. Actual fields must match the corresponding expected JSON,
     * including duplicate counts. This compares field content independently of traversal order, which has separate tests.
     */
    @ParameterizedTest
    @ValueSource(strings = {"extraction-single-region", "extraction-multiple-regions", "binary-label", "multiclass-label"})
    void baselineProfilesMatchIndependentFieldMultisets(String name) throws Exception {
        Annotation input = annotation("accepted/" + name + ".json");
        List<ProjectedField> actual = project(input, baseline(input.getAnnotationType()));
        assertEquals(expectedFields(read("expected/" + name + ".json").path("logicalFields")), fieldMultiset(actual));
        assertTrue(actual.stream().allMatch(field -> !field.bindingId().isEmpty()));
    }

    /**
     * Projects score cases from {@link AnnotationProjectionTestCases#scoreCases()}. An absent score produces neither SCORE nor RESULT; present scores retain
     * their Float spelling and are composed with the label from the same value. Type normalization is left to the ingest test using these same rows.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("datawave.annotation.projection.AnnotationProjectionTestCases#scoreCases")
    void scoresPreservePresenceFloatSpellingAndValuePairing(String name, Float score, String scoreText, String resultText, String normalized) throws Exception {
        List<ProjectedField> fields = project(scored(name, score), baseline("BINARY_LABEL"));
        assertEquals(scoreText == null ? List.of() : List.of(scoreText), values(fields, "BINARY_SCORE"));
        assertEquals(resultText == null ? List.of() : List.of(resultText), values(fields, "BINARY_RESULT"));
        assertEquals(List.of("present"), values(fields, "BINARY_CATEGORY"));
    }

    /**
     * Builds two values in the same segment: the first has no score, and the second has score 0.5. Both categories must be emitted, but only the second value
     * may produce SCORE and RESULT fields, with its own grouping identity.
     * <p>
     * This checks that each binding reads properties from the current annotation value. Collecting labels and scores into separate lists could incorrectly
     * associate the second score with the first label.
     */
    @Test
    void absentScoreDoesNotBorrowTheNextValuesScore() throws Exception {
        Annotation.Builder input = scored("absent", null).toBuilder();
        input.getSegmentsBuilder(0).addValues(SegmentValue.newBuilder().setValue("later").setValueHash("vLater").setScore(0.5f));
        List<ProjectedField> fields = project(input.build(), baseline("BINARY_LABEL"));
        assertEquals(List.of("present", "later"), values(fields, "BINARY_CATEGORY"));
        assertEquals(List.of("0.5"), values(fields, "BINARY_SCORE"));
        assertEquals(List.of("later:0.5"), values(fields, "BINARY_RESULT"));
        assertEquals(List.of("aSource", "bAll", "vLater"),
                        fields.stream().filter(field -> field.baseFieldName().equals("BINARY_SCORE")).findFirst().orElseThrow().groupingComponents());
    }

    /**
     * Projects padded labels and source properties. RESULT must preserve operand padding during composition, and SOURCE must hash its composed text before
     * final trimming. The independently written hashes also check that projection leaves the original source properties unchanged.
     */
    @Test
    void compositionAndSyntheticHashPrecedeFinalTrim() throws Exception {
        List<ProjectedField> padded = project(paddedLabel(), baseline("BINARY_LABEL"));
        assertEquals(List.of("label"), values(padded, "BINARY_CATEGORY"));
        assertEquals(List.of("label :0.0"), values(padded, "BINARY_RESULT"));
        Annotation input = paddedSource();
        List<ProjectedField> source = project(input, baseline("BINARY_LABEL"));
        ProjectedField field = source.stream().filter(value -> value.baseFieldName().equals("BINARY_SOURCE")).findFirst().orElseThrow();
        assertEquals("model :engine", field.eventValue());
        assertEquals(List.of("aSource", "B809141A", "6F0883EE"), field.groupingComponents());
        assertEquals(" model ", input.getSource().getModel());
    }

    /**
     * Supplies input text and its independently expected field value. A null answer means the input is blank and should produce no field. An empty answer means
     * a present control character survives the blank check but becomes empty after String.trim. Em-space is blank; nonbreaking space is retained.
     */
    static Stream<Arguments> whitespace() {
        //@formatter:off
        return Stream.of(
                Arguments.of("", null),
                Arguments.of(" \t\r\n", null),
                Arguments.of("\u2003", null),
                Arguments.of("\u00a0", "\u00a0"),
                Arguments.of("\u2003text\u2003", "\u2003text\u2003"),
                Arguments.of("\u0000", "")
        );
        //@formatter:on
    }

    /**
     * Uses a required value binding to distinguish property presence from blank output. Every supplied string satisfies the required selection; the whitespace
     * rules decide whether it produces a field before final String.trim changes the emitted text.
     */
    @ParameterizedTest
    @MethodSource("whitespace")
    void blankOmissionPrecedesJavaTrimAndRequiredMeansPresent(String input, String expected) throws Exception {
        Annotation.Builder annotation = content("FUTURE", "whitespace");
        annotation.getSegmentsBuilder(0).getValuesBuilder(0).setValue(input);
        Map<String,Object> binding = binding("value", "value", "$.value", "text", true);
        List<ProjectedField> fields = project(annotation.build(), profile("FUTURE", binding));
        assertEquals(expected == null ? List.of() : List.of(expected), values(fields, TEST_FIELD));
    }

    /**
     * Supplies an empty label with an explicitly present zero score. CATEGORY is omitted as blank, but RESULT remains {@code :0.0} because composition reads
     * the present empty string rather than treating the label as an absent property.
     */
    @Test
    void emptyLabelStillAllowsResultComposition() throws Exception {
        Annotation.Builder input = scored("zero", 0.0f).toBuilder();
        input.getSegmentsBuilder(0).getValuesBuilder(0).clearValue();
        List<ProjectedField> fields = project(input.build(), baseline("BINARY_LABEL"));
        assertTrue(values(fields, "BINARY_CATEGORY").isEmpty());
        assertEquals(List.of(":0.0"), values(fields, "BINARY_RESULT"));
    }

    /**
     * Projects prepared {@code stats} content with {@code configuration/exact-extension-profile.json}. Fields must match the independent expected file,
     * including a large exact integer and Boolean false, while the original annotation retains its encoded extension strings.
     */
    @Test
    void declaredExtensionsRetainExactNumbersAndFalse() throws Exception {
        Annotation input = extended("Extension-v1", exactStoredExtensions());
        // Supply a prepared exact value, keeping JSON parsing outside the selector-state callback.
        Object preparedStats = EXACT_JSON.readValue(exactStoredExtensions().get("stats"), Object.class);
        ProjectionEvaluationState state = preparedState(input, "stats", preparedStats, new CountingGuard());
        List<ProjectedField> fields = new ArrayList<>();
        projector.project(state, configured("configuration/exact-extension-profile.json", "FUTURE"), fields::add);
        assertSame(input, state.acceptedAnnotation());
        assertEquals(expectedFields(read("expected/extension-numeric-boolean-projection.json").path("logicalFields")), fieldMultiset(fields));
        assertEquals(exactStoredExtensions(), input.getSegments(0).getValues(0).getExtensionMap());
    }

    /**
     * Reads independently authored scalar-format answers. Each row contains the format, prepared Java value, expected text and whether conversion must fail.
     * Exact numeric parsing preserves the test input's mathematical value rather than passing it through Float/Double.
     */
    static Stream<Arguments> scalarVectors() throws Exception {
        List<Arguments> rows = new ArrayList<>();
        for (JsonNode vector : read("oracles/scalar-format-vectors.json").path("vectors")) {
            Object input = vector.has("inputJson") ? EXACT_JSON.readValue(vector.path("inputJson").textValue(), Object.class)
                            : EXACT_JSON.convertValue(vector.path("input"), Object.class);
            rows.add(Arguments.of(vector.path("format").textValue(), input, vector.path("expected").textValue(), vector.has("failure")));
        }
        return rows.stream();
    }

    /**
     * Passes each prepared scalar from {@code oracles/scalar-format-vectors.json} through an actual projection binding. Valid values must produce the literal
     * expected text; a value with the wrong type must fail before any field is delivered.
     */
    @ParameterizedTest
    @MethodSource("scalarVectors")
    void scalarFormatsExecuteIndependentVectors(String format, Object input, String expected, boolean failure) throws Exception {
        Annotation annotation = extended("scalar-vector", Map.of("test", "stored"));
        ProjectionEvaluationState state = preparedState(annotation, "test", input, new CountingGuard());
        CompiledSemanticProfile profile = profile("FUTURE", binding("test", "value", "$.extension.test", format, false));
        List<ProjectedField> fields = new ArrayList<>();
        if (failure) {
            assertThrows(ProjectionException.class, () -> projector.project(state, profile, fields::add));
            assertTrue(fields.isEmpty());
        } else {
            projector.project(state, profile, fields::add);
            assertEquals(List.of(expected), values(fields, TEST_FIELD));
        }
    }

    /**
     * Projects an undeclared JSON-looking string as text, then supplies a prepared string that also looks like JSON. Both remain strings. A selector that tries
     * to traverse the undeclared text as an object must fail rather than triggering implicit or repeated decoding.
     */
    @Test
    void opaqueStringsAreNotDecodedAndDeclaredStringsAreNotDecodedTwice() throws Exception {
        Annotation input = extended("opaque", Map.of("test", "{\"number\":12}"));
        Map<String,Object> binding = binding("test", "value", "$.extension.test", "text", true);
        assertEquals(List.of("{\"number\":12}"), values(project(input, profile("FUTURE", binding)), TEST_FIELD));
        ProjectionEvaluationState state = preparedState(input, "test", "[1,false]", new CountingGuard());
        List<ProjectedField> fields = new ArrayList<>();
        projector.project(state, profile("FUTURE", binding), fields::add);
        assertEquals(List.of("[1,false]"), values(fields, TEST_FIELD));
        assertThrows(ProjectionException.class, () -> project(input, profile("FUTURE", binding("test", "value", "$.extension.test.number", "text", false))));
    }

    /**
     * Visits prepared maps whose insertion order differs from Java String order, including supplementary and private-use Unicode keys. Map output must follow
     * String.compareTo, while an array retains its original order and repeated elements.
     */
    @Test
    void wildcardMapOrderUsesStringCompareToAndArrayOrderIsRetained() throws Exception {
        Map<String,Object> map = new LinkedHashMap<>();
        map.put("a", "last");
        map.put("2", "second");
        map.put("A", "third");
        map.put("10", "first");
        assertEquals(List.of("first", "second", "third", "last"), decodedValues(map, "$.extension.test[*]", "text"));
        assertEquals(List.of("second", "first", "second"), decodedValues(List.of("second", "first", "second"), "$.extension.test[*]", "text"));
        Map<String,Object> unicode = new LinkedHashMap<>();
        unicode.put("\ue000", "private");
        unicode.put("\ud800\udc00", "supplementary");
        assertEquals(List.of("supplementary", "private"), decodedValues(unicode, "$.extension.test.*", "text"));
    }

    /**
     * Selects a prepared array both directly and through explicit traversal. Direct selection is one array result and cannot use the text format. A wildcard
     * emits its elements, an index selects one element, and an out-of-range index produces no match.
     */
    @Test
    void aSelectedArrayIsOneWrongKindMatchRatherThanSeveralScalars() throws Exception {
        assertThrows(ProjectionException.class, () -> decodedValues(List.of("one", "two"), "$.extension.test", "text"));
        assertEquals(List.of("one", "two"), decodedValues(List.of("one", "two"), "$.extension.test[*]", "text"));
        assertEquals(List.of("two"), decodedValues(List.of("one", "two"), "$.extension.test[1]", "text"));
        assertTrue(decodedValues(List.of("one"), "$.extension.test[9]", "text").isEmpty());
    }

    /**
     * Pairs an actual map key with the configured selector that must address it exactly. Java string escaping is separate from selector escaping; the rows
     * exercise dots, quotes, backslashes, a control character and a supplementary Unicode code point without interpreting them as traversal syntax.
     */
    static Stream<Arguments> quotedKeys() {
        //@formatter:off
        return Stream.of(
                Arguments.of("key.with.dot", "$.extension['key.with.dot']"),
                Arguments.of("a'b", "$.extension['a\\'b']"),
                Arguments.of("a\\b", "$.extension['a\\\\b']"),
                Arguments.of("a\"b", "$.extension[\"a\\\"b\"]"),
                Arguments.of("line\nbreak", "$.extension['line\\nbreak']"),
                Arguments.of("\ud800\udc00", "$.extension['\\uD800\\uDC00']")
        );
        //@formatter:on
    }

    /**
     * Stores a value under each literal key from {@link #quotedKeys()} and addresses it with the configured escaped path. Punctuation and escape sequences must
     * identify that key exactly rather than introduce additional traversal.
     */
    @ParameterizedTest
    @MethodSource("quotedKeys")
    void quotedMapKeysUseExactDefinedEscapes(String key, String path) throws Exception {
        Annotation input = extended("key", Map.of(key, "selected"));
        assertEquals(List.of("selected"), values(project(input, profile("FUTURE", binding("key", "value", path, "text", true))), TEST_FIELD));
    }

    /**
     * Projects two bindings over an already ordered annotation containing equal repeated values and distinct values in the same group. Each binding must retain
     * all values in annotation order, preserve the supplied identities, and count every emitted field against the budget.
     */
    @Test
    void frameIdentitiesAndRepeatedOccurrencesSurviveInBindingOrder() throws Exception {
        Annotation.Builder input = unicode(true).toBuilder();
        Segment first = input.getSegments(0);
        input.getSegmentsBuilder(0).addValues(first.getValues(0)).addValues(first.getValues(0).toBuilder().setValue("distinct"));
        Annotation accepted = input.build();
        Map<String,Object> one = binding("one", "value", "$.value", "text", true);
        one.put("grouping", Map.of("kind", "source-segment-value"));
        Map<String,Object> two = new LinkedHashMap<>(one);
        two.put("id", "two");
        CountingGuard work = new CountingGuard();
        List<ProjectedField> fields = new ArrayList<>();
        projector.project(accepted, profile("FUTURE", one, two), work, fields::add);
        List<String> traversal = List.of("private-use-first", "private-use-first", "distinct", "private-use-second", "supplementary");
        assertEquals(Stream.concat(traversal.stream(), traversal.stream()).collect(Collectors.toList()), values(fields, TEST_FIELD));
        assertEquals(List.of("aSource", "\ue000", "vprivate-use-first"), fields.get(2).groupingComponents());
        assertEquals("one", fields.get(0).bindingId());
        assertEquals("two", fields.get(5).bindingId());
        assertEquals(10, work.candidates);
        assertEquals("Opaque.Annotation:ID", accepted.getAnnotationId());
        assertEquals(accepted, input.build());
        assertThrows(UnsupportedOperationException.class, () -> fields.get(0).groupingComponents().add("mutated"));
    }

    /**
     * Requires a score on an existing value and checks that its absence fails with binding/scope context. Removing all segments or the entire source produces
     * no corresponding scope objects, so a required binding there has nothing to evaluate and does not manufacture a value.
     */
    @Test
    void requiredMissingScoreFailsButAbsentFramesDoNotManufactureValues() throws Exception {
        CompiledSemanticProfile required = profile("BINARY_LABEL", binding("required-score", "value", "$.score", "java-float-text", true));
        ProjectionException failure = assertThrows(ProjectionException.class, () -> project(scored("absent", null), required));
        assertTrue(failure.getMessage().contains("required-score"));
        assertTrue(failure.getMessage().contains("scope value"));
        assertTrue(project(content("BINARY_LABEL", "no-values").clearSegments().build(), required).isEmpty());
        Annotation missingSource = content("FUTURE", "missing-source").clearSource().build();
        assertTrue(project(missingSource, profile("FUTURE", binding("model", "source", "$.model", "text", true))).isEmpty());
    }

    /**
     * Compares implicit protobuf defaults with optional-property presence. An unset value string is present but blank, an absent platform fails a required
     * selection, and an explicitly empty platform satisfies it. Default point coordinates and explicitly present zero span starts both produce zero text.
     */
    @Test
    void typedDefaultsAndOptionalPresenceRemainDifferent() throws Exception {
        Annotation.Builder input = content("FUTURE", "presence");
        input.getSegmentsBuilder(0).getValuesBuilder(0).clearValue().clearScore();
        assertTrue(project(input.build(), profile("FUTURE", binding("implicit-value", "value", "$.value", "text", true))).isEmpty());
        CompiledSemanticProfile platform = profile("FUTURE", binding("platform", "source", "$.platform", "text", true));
        input.getSourceBuilder().clearPlatform();
        assertThrows(ProjectionException.class, () -> project(input.build(), platform));
        input.getSourceBuilder().setPlatform("");
        assertTrue(project(input.build(), platform).isEmpty());
        assertEquals(List.of("0"), values(
                        project(bounded(BoundaryType.POINTS), profile("FUTURE", binding("x", "segment", "$.boundary.points[0].x", "json-number-text", true))),
                        TEST_FIELD));
        CompiledSemanticProfile span = profile("FUTURE", binding("start", "segment", "$.boundary.start", "json-number-text", true));
        assertThrows(ProjectionException.class, () -> project(bounded(BoundaryType.ALL), span));
        assertEquals(List.of("0"), values(project(bounded(BoundaryType.TEXT_CHAR), span), TEST_FIELD));
    }

    /**
     * Places NaN or infinity in a score while the binding selects only the value text. Building the property view must reject the score before projection can
     * ignore it. Common preparation's rejection on all write routes remains a separate acceptance requirement.
     */
    @ParameterizedTest
    @ValueSource(floats = {Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY})
    void nonfiniteScoresFailEvenWhenTheBindingDoesNotSelectScore(float score) throws Exception {
        assertThrows(ProjectionException.class,
                        () -> project(scored("nonfinite", score), profile("BINARY_LABEL", binding("value", "value", "$.value", "text", false))));
    }

    /**
     * Compiles unsupported syntax, incomplete paths, invalid escapes and paths incompatible with known protobuf properties. Every case must fail at setup
     * rather than become an optional missing property during projection.
     */
    @ParameterizedTest(name = "unsupported-selector-{0}")
    //@formatter:off
    @ValueSource(strings = {
            "$..value",
            "$.value[",
            "$.value.",
            "$['extension'",
            "$.extension[?(@.x)]",
            "$.extension[0:2]",
            "$.extension[-1]",
            "$.extension[0,1]",
            "$.extension.keys()",
            "$.extension['x' 'y']",
            "$.extension['\\q']",
            "$.extension['\\uZZZZ']",
            "$.extension['\\uD800']",
            "$.extension['\\u０000']",
            "$.extension[2147483648]",
            "$.unknown",
            "$.value.child",
            "$.*"
    })
    //@formatter:on
    void unsupportedOrIncompleteSelectorsFailCompilation(String path) {
        assertThrows(ProjectionException.class, () -> profile("FUTURE", binding("bad-path", "value", path, "text", false)));
    }

    /**
     * Supplies incompatible selector/format combinations as a case name, selector path and format name. Each path addresses a property whose type is known from
     * the value's protobuf schema, allowing the compiler to reject the format before projection.
     */
    static Stream<Arguments> incompatibleScalarFormats() {
        //@formatter:off
        return Stream.of(
                Arguments.of("extension-container-as-text", "$.extension", "text"),
                Arguments.of("score-as-text", "$.score", "text"),
                Arguments.of("value-as-boolean", "$.value", "boolean-text"),
                Arguments.of("score-as-exact-number", "$.score", "json-number-text")
        );
        //@formatter:on
    }

    /**
     * Applies each incompatible format from {@link #incompatibleScalarFormats()} to its selected property. Every case must fail during compilation, before any
     * annotation is projected.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("incompatibleScalarFormats")
    void knownContainersAndWrongScalarFormatsFailCompilation(String id, String path, String format) {
        assertThrows(ProjectionException.class, () -> profile("FUTURE", binding(id, "value", path, format, false)));
    }

    /**
     * Tries to compose text with an unformatted Float score and to use wildcard results as composition or format operands. Compilation must require explicit
     * score formatting and operands known to produce at most one result, preventing accidental pairing of unrelated matches.
     */
    @Test
    void compositionRequiresDefiniteOperandsAndExplicitFloatFormatting() {
        Map<String,Object> binding = binding("compose", "value", "$.value", "text", false);
        binding.put("expression", Map.of("op", "concat", "delimiter", ":", "operands", List.of(select("$.value"), select("$.score"))));
        assertThrows(ProjectionException.class, () -> profile("FUTURE", binding));
        binding.put("expression", Map.of("op", "concat", "delimiter", ":", "operands", List.of(select("$.extension[*]"))));
        assertThrows(ProjectionException.class, () -> profile("FUTURE", binding));
        binding.put("expression", Map.of("op", "format", "format", "text", "operand", select("$.extension[*]")));
        assertThrows(ProjectionException.class, () -> profile("FUTURE", binding));
    }

    /**
     * Changes a binding's original map after compilation and verifies that the compiled field still uses its original name and selector. Separate invalid
     * definitions check unknown keys, wrong Java value types, unsupported operations and duplicate binding IDs.
     */
    @Test
    void semanticDefinitionsAreClosedStrictAndCompiledWithoutCallerAliases() throws Exception {
        Map<String,Object> binding = binding("value", "value", "$.value", "text", false);
        CompiledSemanticProfile compiled = profile("FUTURE", binding);
        binding.put("field", "OTHER");
        binding.put("expression", select("$.missing"));
        assertEquals(List.of("present"), values(project(content("FUTURE", "alias").build(), compiled), TEST_FIELD));
        binding.put("unknown", true);
        assertThrows(ProjectionException.class, () -> profile("FUTURE", binding));
        Map<String,Object> strict = binding("value", "value", "$.value", "text", false);
        strict.put("required", "false");
        assertThrows(ProjectionException.class, () -> profile("FUTURE", strict));
        strict.put("required", false);
        strict.put("expression", Map.of("op", "literal", "value", 12));
        assertThrows(ProjectionException.class, () -> profile("FUTURE", strict));
        strict.put("expression", Map.of("op", "coalesce", "value", "constant"));
        assertThrows(ProjectionException.class, () -> profile("FUTURE", strict));
        assertThrows(ProjectionException.class, () -> profile("FUTURE", binding("duplicate", "value", "$.value", "text", false),
                        binding("duplicate", "value", "$.value", "text", false)));
    }

    /**
     * Supplies base names with invalid case, punctuation or padding. The compiler must reject them as authored instead of silently rewriting them into another
     * field name.
     */
    @ParameterizedTest
    @ValueSource(strings = {"lowercase", "_FIELD", "FIELD.group", " FIELD", "FIELD ", "FIELD\u0000"})
    void configuredBaseNamesAreNotRepaired(String name) {
        Map<String,Object> binding = binding("name", "value", "$.value", "text", false);
        binding.put("field", name);
        assertThrows(ProjectionException.class, () -> profile("FUTURE", binding));
    }

    /**
     * Supplies a case name and grouping declaration for a source-scoped text binding. The rows isolate value grouping on the wrong scope, missing or unknown
     * source-label hash strategies, and an extra strategy key on an ungrouped field.
     */
    static Stream<Arguments> invalidGroupingDefinitions() {
        //@formatter:off
        return Stream.of(
                Arguments.of("value-grouping-on-source", Map.of("kind", "source-segment-value")),
                Arguments.of("source-label-without-strategy", Map.of("kind", "source-all-label")),
                Arguments.of("source-label-with-unknown-strategy", Map.of("kind", "source-all-label", "hashStrategy", "unknown")),
                Arguments.of("ungrouped-field-with-strategy", Map.of("kind", "none", "hashStrategy", "murmur3-32-unencoded-chars-v1"))
        );
        //@formatter:on
    }

    /** Compiles each grouping declaration from {@link #invalidGroupingDefinitions()} and requires its configuration error to be rejected during setup. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidGroupingDefinitions")
    void invalidGroupingDefinitionsFailCompilation(String id, Map<String,Object> grouping) {
        Map<String,Object> binding = binding(id, "source", "$.model", "text", false);
        binding.put("grouping", grouping);
        assertThrows(ProjectionException.class, () -> profile("FUTURE", binding));
    }

    /**
     * Projects a value using supplied source, segment and value hashes containing mixed case and punctuation. Their text must be retained exactly. Removing the
     * required segment identity must fail during projection rather than calculate a replacement.
     */
    @Test
    void valueGroupingPreservesSuppliedHashesAndRequiresSegmentIdentity() throws Exception {
        Map<String,Object> value = binding("value", "value", "$.value", "text", false);
        value.put("grouping", Map.of("kind", "source-segment-value"));
        Annotation.Builder input = content("FUTURE", "opaque-identity");
        input.getSourceBuilder().setAnalyticHash("Mixed.A");
        input.getSegmentsBuilder(0).setSegmentHash("opaque:B").getValuesBuilder(0).setValueHash("c.d");
        assertEquals(List.of("Mixed.A", "opaque:B", "c.d"), project(input.build(), profile("FUTURE", value)).get(0).groupingComponents());
        input.getSegmentsBuilder(0).clearSegmentHash();
        assertThrows(ProjectionException.class, () -> project(input.build(), profile("FUTURE", value)));
    }

    /**
     * Applies a BINARY_LABEL profile to a FUTURE annotation. The type mismatch must fail before building the property view or producing fields, so another
     * annotation type's bindings cannot serve as a fallback.
     */
    @Test
    void aDifferentEffectiveTypeFailsRatherThanUsingAnotherProfilesBindings() throws Exception {
        CountingGuard work = new CountingGuard();
        assertThrows(ProjectionException.class, () -> projector.project(content("FUTURE", "miss").build(), baseline("BINARY_LABEL"), work, field -> {}));
        assertEquals(0, work.nodes);
        assertEquals(0, work.candidates);
    }

    /**
     * Builds prepared extension state from mutable maps and lists, then clears those original containers. Two projection calls must still produce the copied
     * values in order, and the extension callback must have run only once when the state was built.
     */
    @Test
    void preparedExtensionTreesOwnTheirMapsAndListsAndDecodeOnlyOnce() throws Exception {
        Annotation input = extended("owned", Map.of("test", "stored"));
        List<Object> children = new ArrayList<>(List.of("one", "two"));
        Map<String,Object> decoded = new LinkedHashMap<>(Map.of("children", children));
        AtomicInteger calls = new AtomicInteger();
        ProjectionEvaluationState state = ProjectionEvaluationState.from(input, (key, stored) -> {
            if (key.equals("test")) {
                calls.incrementAndGet();
                return decoded;
            }
            return stored;
        }, new CountingGuard());
        children.clear();
        decoded.clear();
        CompiledSemanticProfile profile = profile("FUTURE", binding("children", "value", "$.extension.test.children[*]", "text", true));
        List<ProjectedField> fields = new ArrayList<>();
        projector.project(state, profile, fields::add);
        projector.project(state, profile, fields::add);
        assertEquals(List.of("one", "two", "one", "two"), values(fields, TEST_FIELD));
        assertEquals(1, calls.get());
    }

    /**
     * Supplies nulls, a map with a non-string key, inexact floating-point numbers and oversized exact numbers as prepared extension values. State construction
     * must reject them before they can be selected and formatted into fields.
     */
    @Test
    void nullNonStringKeysAndInexactNumbersAreNotAcceptedAsDecodedValues() throws Exception {
        for (Object invalid : Arrays.asList(null, Arrays.asList("one", null), Map.of(1, "value"), 12.5d, 12.5f)) {
            assertThrows(ProjectionException.class, () -> preparedState(extended("bad", Map.of("test", "stored")), "test", invalid, new CountingGuard()));
        }
        assertThrows(ProjectionException.class, () -> decodedValues(new BigDecimal(BigInteger.ONE, Integer.MAX_VALUE), "$.extension.test", "json-number-text"));
        assertThrows(ProjectionException.class, () -> decodedValues(BigInteger.ONE.shiftLeft(30000), "$.extension.test", "json-number-text"));
    }

    /**
     * First attempts a missing property and verifies that it consumes traversal work. Then restricts the same budget so a three-child wildcard cannot fit;
     * expansion must fail before any child field is delivered or counted as a candidate.
     */
    @Test
    void selectorBudgetChargesMissesAndRejectsWildcardWorkBeforeOutputs() throws Exception {
        CountingGuard work = new CountingGuard();
        Annotation input = extended("work", Map.of("test", "stored"));
        ProjectionEvaluationState state = preparedState(input, "test", List.of("a", "b", "c"), work);
        List<ProjectedField> fields = new ArrayList<>();
        projector.project(state, profile("FUTURE", binding("missing", "value", "$.extension.missing", "text", false)), fields::add);
        assertTrue(work.steps > 0);
        assertTrue(fields.isEmpty());
        // The scope/path needs seven ordinary steps; nine leaves insufficient room for all three wildcard children.
        work.selectorLimit = work.steps + 9;
        assertSame(work.failure, assertThrows(IllegalStateException.class,
                        () -> projector.project(state, profile("FUTURE", binding("wildcard", "value", "$.extension.test[*]", "text", false)), fields::add)));
        assertTrue(fields.isEmpty());
        assertEquals(0, work.candidates);
        assertEquals(3, work.lastSelectorCharge);
    }

    /**
     * Projects three equal array elements with a limit of two output candidates. The first two fields remain delivered, and the third must fail at the budget
     * check. Equal values cannot bypass candidate counting through duplicate removal.
     */
    @Test
    void candidateBudgetCountsExactRepeatsBeforeConsumerAdmission() throws Exception {
        CountingGuard work = new CountingGuard();
        work.candidateLimit = 2;
        Annotation input = extended("candidates", Map.of("test", "stored"));
        ProjectionEvaluationState state = preparedState(input, "test", List.of("same", "same", "same"), work);
        List<ProjectedField> fields = new ArrayList<>();
        assertSame(work.failure, assertThrows(IllegalStateException.class,
                        () -> projector.project(state, profile("FUTURE", binding("candidate", "value", "$.extension.test[*]", "text", false)), fields::add)));
        assertEquals(List.of("same", "same"), values(fields, TEST_FIELD));
        assertEquals(2, work.candidates);
    }

    /**
     * Combines two three-character literals with a colon under a five-character temporary-string limit. Each operand fits individually, but the proposed
     * seven-character result must fail before it is appended and delivered.
     */
    @Test
    void compositionChecksProspectiveAllocationBeforeAppending() throws Exception {
        CountingGuard work = new CountingGuard();
        ProjectionEvaluationState state = ProjectionEvaluationState.from(content("FUTURE", "composition").build(), work);
        work.stringLimit = 5;
        Map<String,Object> binding = binding("composed", "value", "$.value", "text", false);
        binding.put("expression", Map.of("op", "concat", "delimiter", ":", "operands",
                        List.of(Map.of("op", "literal", "value", "123"), Map.of("op", "literal", "value", "456"))));
        List<ProjectedField> fields = new ArrayList<>();
        assertSame(work.failure, assertThrows(IllegalStateException.class, () -> projector.project(state, profile("FUTURE", binding), fields::add)));
        assertTrue(fields.isEmpty());
    }

    /**
     * Restricts the node count while copying an annotation, then restricts depth while copying a nested prepared extension object. Both must propagate the
     * original budget failure as the view is built, before projection begins.
     */
    @Test
    void selectorTreeChecksDepthAndNodesWhileCopying() throws Exception {
        CountingGuard nodes = new CountingGuard();
        nodes.nodeLimit = 1;
        assertSame(nodes.failure, assertThrows(IllegalStateException.class, () -> ProjectionEvaluationState.from(content("FUTURE", "nodes").build(), nodes)));
        CountingGuard depth = new CountingGuard();
        depth.depthLimit = 7;
        // The value's extension map is level six; the prepared object is level seven and its nested object would be level eight.
        assertSame(depth.failure, assertThrows(IllegalStateException.class,
                        () -> preparedState(extended("depth", Map.of("test", "stored")), "test", Map.of("nested", Map.of("more", "value")), depth)));
    }

    /**
     * Throws from the output consumer after the first field has been charged. The projector must propagate that same exception and retain the tree, traversal
     * and candidate work already counted against the annotation's budget.
     */
    @Test
    void consumerFailuresPropagateAndDoNotResetAlreadyChargedWork() throws Exception {
        CountingGuard work = new CountingGuard();
        RuntimeException failure = new IllegalStateException("consumer");
        Annotation input = content("FUTURE", "consumer").build();
        assertSame(failure, assertThrows(IllegalStateException.class,
                        () -> projector.project(input, profile("FUTURE", binding("value", "value", "$.value", "text", false)), work, field -> {
                            throw failure;
                        })));
        assertEquals(1, work.candidates);
        assertTrue(work.nodes > 0);
        assertTrue(work.steps > 0);
    }

    /**
     * Throws Jayway's PathNotFoundException from the output consumer. Only a missing selector lookup represents absence; this consumer exception must propagate
     * unchanged instead of being caught as an optional selection miss.
     */
    @Test
    void aConsumersLibraryExceptionCannotBecomeAnOptionalMiss() throws Exception {
        PathNotFoundException failure = new PathNotFoundException("consumer");
        assertSame(failure, assertThrows(PathNotFoundException.class, () -> projector.project(content("FUTURE", "consumer-path").build(),
                        profile("FUTURE", binding("value", "value", "$.value", "text", false)), new CountingGuard(), field -> {
                            throw failure;
                        })));
    }

    /**
     * Finds the requested annotationType in an authored profile resource and compiles its semantic section. This helper parses test configuration directly; it
     * does not exercise a production registry loader or install the enclosing DataWave execution policies.
     */
    private static CompiledSemanticProfile configured(String resource, String type) throws Exception {
        for (JsonNode profile : read(resource).path("profiles")) {
            if (profile.path("annotationType").asText().equals(type)) {
                return CompiledSemanticProfile.compile(profile.path("id").asText(), type, semanticMap(profile.path("semantic")));
            }
        }
        throw new AssertionError("Missing test profile: " + type);
    }

    /** Reads the requested extraction or content-label profile from {@code configuration/baseline-profiles.json}. */
    private static CompiledSemanticProfile baseline(String type) throws Exception {
        return configured(BASELINE_PROFILES_RESOURCE, type);
    }

    /** Converts a test profile's semantic JSON object to the Map input accepted by the compiler. */
    @SuppressWarnings("unchecked")
    private static Map<String,Object> semanticMap(JsonNode definition) {
        return JSON.convertValue(definition, Map.class);
    }

    /** Assembles a small explicit semantic definition for one behavior under test; it calculates no expected field value or hash. */
    @SafeVarargs
    private static CompiledSemanticProfile profile(String type, Map<String,Object>... bindings) {
        return CompiledSemanticProfile.compile("test-profile", type, Map.of("selectorDialect", "scoped-paths-v1", "output",
                        Map.of("trim", "java-string-trim", "omitBlank", true), "bindings", List.of(bindings)));
    }

    /**
     * Builds a mutable, explicit binding to TEST_FIELD with no grouping. Tests alter individual declarations to isolate a compiler rule; the helper supplies no
     * inferred field policy, expected output or hash.
     */
    private static Map<String,Object> binding(String id, String scope, String path, String format, boolean required) {
        Map<String,Object> result = new LinkedHashMap<>();
        result.put("id", id);
        result.put("scope", scope);
        result.put("field", TEST_FIELD);
        result.put("required", required);
        result.put("expression", select(path));
        result.put("format", format);
        result.put("grouping", Map.of("kind", "none"));
        return result;
    }

    /** Describes an authored selector without compiling or evaluating it. */
    private static Map<String,Object> select(String path) {
        return Map.of("op", "select", "path", path);
    }

    /** Runs the projector with a fresh finite test budget and collects every actual field in delivery order, including duplicates. */
    private List<ProjectedField> project(Annotation input, CompiledSemanticProfile profile) {
        List<ProjectedField> result = new ArrayList<>();
        projector.project(input, profile, new CountingGuard(), result::add);
        return result;
    }

    /**
     * Supplies one prepared extension interpretation while retaining the stored strings for other keys. This is a test input adapter; it does not decode JSON,
     * select a codec or derive expected projection output.
     */
    private static ProjectionEvaluationState preparedState(Annotation input, String extensionKey, Object decoded, ProjectionWorkGuard work) {
        return ProjectionEvaluationState.from(input, (key, stored) -> key.equals(extensionKey) ? decoded : stored, work);
    }

    /** Projects one explicitly prepared extension value and returns actual text in match order. The supplied path must opt into array/map traversal. */
    private List<String> decodedValues(Object decoded, String path, String format) throws Exception {
        Annotation input = extended("decoded", Map.of("test", "stored"));
        ProjectionEvaluationState state = preparedState(input, "test", decoded, new CountingGuard());
        List<ProjectedField> fields = new ArrayList<>();
        projector.project(state, profile("FUTURE", binding("decoded", "value", path, format, false)), fields::add);
        return values(fields, TEST_FIELD);
    }

    /** Returns emitted event values for the requested base field name in delivery order, retaining equal repeated values. */
    private static List<String> values(List<ProjectedField> fields, String base) {
        return fields.stream().filter(field -> field.baseFieldName().equals(base)).map(ProjectedField::eventValue).collect(Collectors.toList());
    }

    /**
     * Counts the fields in an independent expected-answer file. Each comparison key contains the base name, complete grouping list and event text, in that
     * order; its count preserves repeated equal fields. Binding IDs are not part of these answers. Traversal order is checked separately, so the files retain
     * their independently authored order.
     */
    private static Map<List<Object>,Long> expectedFields(JsonNode fields) {
        Map<List<Object>,Long> result = new HashMap<>();
        for (JsonNode field : fields) {
            List<String> group = new ArrayList<>();
            field.path("group").forEach(component -> group.add(component.textValue()));
            result.merge(List.of(field.path("base").textValue(), group, field.path("value").textValue()), 1L, Long::sum);
        }
        return result;
    }

    /** Counts actual fields by base name, grouping list and event text so comparisons retain the number of equal repeated results. */
    private static Map<List<Object>,Long> fieldMultiset(List<ProjectedField> fields) {
        return fields.stream().collect(
                        Collectors.groupingBy(field -> List.of(field.baseFieldName(), field.groupingComponents(), field.eventValue()), Collectors.counting()));
    }

    /**
     * Supplies finite limits and counters for tree construction, selector traversal, temporary strings and emitted fields. Tests lower individual limits to
     * check that a failure happens before the protected work or field delivery. Reusing one exception instance verifies that failures propagate unchanged.
     */
    private static final class CountingGuard implements ProjectionWorkGuard {
        final IllegalStateException failure = new IllegalStateException("Test projection budget exceeded");
        long steps;
        long lastSelectorCharge;
        long candidates;
        long nodes;
        long selectorLimit = 1_000_000;
        long candidateLimit = 100_000;
        long nodeLimit = 250_000;
        long stringLimit = 16 * 1024 * 1024;
        int depthLimit = 32;

        @Override
        public void chargeSelectorSteps(long additionalSteps) {
            lastSelectorCharge = additionalSteps;
            if (additionalSteps > selectorLimit - steps) {
                throw failure;
            }
            steps += additionalSteps;
        }

        @Override
        public void chargeCandidate() {
            if (candidates == candidateLimit) {
                throw failure;
            }
            candidates++;
        }

        @Override
        public void checkStringLength(long prospectiveLength) {
            if (prospectiveLength > stringLimit) {
                throw failure;
            }
        }

        @Override
        public void chargeTreeNode(int containerDepth) {
            if (containerDepth > depthLimit || nodes == nodeLimit) {
                throw failure;
            }
            nodes++;
        }
    }
}
