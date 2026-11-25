package datawave.annotation.projection;

import static datawave.annotation.projection.AnnotationProjectionTestCases.JSON;
import static datawave.annotation.projection.AnnotationProjectionTestCases.NATIVE_LONG;
import static datawave.annotation.projection.AnnotationProjectionTestCases.NATIVE_SHORT;
import static datawave.annotation.projection.AnnotationProjectionTestCases.acceptedAnnotations;
import static datawave.annotation.projection.AnnotationProjectionTestCases.annotation;
import static datawave.annotation.projection.AnnotationProjectionTestCases.bounded;
import static datawave.annotation.projection.AnnotationProjectionTestCases.content;
import static datawave.annotation.projection.AnnotationProjectionTestCases.exactStoredExtensions;
import static datawave.annotation.projection.AnnotationProjectionTestCases.preparationFailures;
import static datawave.annotation.projection.AnnotationProjectionTestCases.read;
import static datawave.annotation.projection.AnnotationProjectionTestCases.scored;
import static datawave.annotation.projection.AnnotationProjectionTestCases.text;
import static datawave.annotation.projection.AnnotationProjectionTestCases.unicode;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.Hashing;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationList;
import datawave.annotation.protobuf.v1.AnnotationMessage;
import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.annotation.protobuf.v1.AnnotationV1;
import datawave.annotation.protobuf.v1.BoundaryType;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.protobuf.v1.SegmentBoundary;
import datawave.annotation.protobuf.v1.SegmentList;
import datawave.annotation.protobuf.v1.SegmentValue;
import datawave.annotation.util.v1.AnnotationUtils;
import datawave.annotation.util.v1.AnnotationValidators;

/**
 * Checks shared fixture models and independently written expected answers against existing annotation parsing, validation, hashing and serialization APIs.
 * These checks use the shared annotation model and do not execute consumer normalization, readers, persistence or index writes.
 * <p>
 * The production preparation and projection implementation is still pending. Input and expected-answer consistency is not acceptance of those later features.
 */
class AnnotationProjectionFixtureContractTest {
    // Exact numeric nodes keep the fixture comparison from rounding integers or underflowing tiny decimals.
    private static final ObjectMapper EXACT_JSON = new ObjectMapper().enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    /**
     * Checks that the complete accepted models retain their content through protobuf JSON and binary round trips. It also checks existing annotation/source
     * validation, agreement of source references and finite values for every present score.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("datawave.annotation.projection.AnnotationProjectionTestCases#acceptedAnnotations")
    void acceptedModelsAreValidAndRoundTrip(String id, Annotation accepted) throws Exception {
        Annotation.Builder parsed = Annotation.newBuilder();
        JsonFormat.parser().merge(JsonFormat.printer().print(accepted), parsed);
        assertEquals(accepted, parsed.build());
        assertEquals(accepted, Annotation.parseFrom(accepted.toByteArray()));
        assertTrue(AnnotationValidators.checkAnnotation(accepted).isValid());
        assertTrue(AnnotationValidators.checkAnnotationSource(accepted.getSource()).isValid());
        assertEquals(accepted.getSource().getAnalyticSourceHash(), accepted.getAnalyticSourceHash());
        //@formatter:off
        accepted.getSegmentsList().forEach(
                segment -> segment.getValuesList().forEach(
                        value -> assertTrue(!value.hasScore() || Float.isFinite(value.getScore()))
                )
        );
        //@formatter:on
    }

    /**
     * Checks that the positive preparation inputs still require the work named by their cases. Partial identities remain missing, supplied identities remain
     * intact and Unicode segments arrive in a different order from the accepted answer. Content and equal-hash relative order are retained.
     * <p>
     * These checks compare inputs and expected answers. Separate preparation tests must run the inputs through actual preparation and compare its result with
     * the accepted model.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("datawave.annotation.projection.AnnotationProjectionTestCases#identityInputs")
    void identityInputsRetainCompletionAndOrderingWork(String id, Annotation input, Annotation accepted) {
        assertEquals("", input.getShard());
        assertEquals("", input.getDataType());
        assertEquals("", input.getUid());
        assertEquals(0, input.getMetadataCount());
        assertEquals(0, input.getSource().getMetadataCount());
        assertEquals("", input.getAnalyticSourceHash());
        assertEquals(accepted.getAnnotationType(), input.getAnnotationType());
        assertEquals(accepted.getDocumentId(), input.getDocumentId());
        assertEquals(accepted.getSource().getAnalyticSourceHash(), input.getSource().getAnalyticSourceHash());
        assertEquals(accepted.getSource().toBuilder().clearMetadata().setAnalyticHash(input.getSource().getAnalyticHash()).build(), input.getSource());

        if (id.startsWith("ordering-unicode-")) {
            assertEquals(id.equals("ordering-unicode-supplied-id") ? "Opaque.Annotation:ID" : "", input.getAnnotationId());
            //@formatter:off
            assertEquals(
                    List.of("supplementary", "private-use-first", "private-use-second"),
                    input.getSegmentsList().stream()
                            .map(segment -> segment.getValues(0).getValue())
                            .collect(java.util.stream.Collectors.toList())
            );
            assertEquals(
                    List.of(accepted.getSegments(2), accepted.getSegments(0), accepted.getSegments(1)),
                    input.getSegmentsList()
            );
            //@formatter:on
            assertEquals("aSource", input.getSource().getAnalyticHash());
            assertEquals(id.equals("ordering-unicode-supplied-id") ? "Opaque.Annotation:ID" : "91BA0DDF", accepted.getAnnotationId());
        } else {
            boolean partial = id.equals("identity-partial-source-and-children");
            assertEquals("", input.getAnnotationId());
            assertEquals("F6B9F68B", accepted.getAnnotationId());
            assertEquals(partial ? "" : "58F48CA4", input.getSource().getAnalyticHash());
            assertEquals(partial ? "" : "F22F0767", input.getSegments(0).getSegmentHash());
            assertEquals(partial ? "" : "F9AFA70A", input.getSegments(0).getValues(0).getValueHash());
            assertEquals(1, input.getSegmentsCount());
            assertEquals(1, input.getSegments(0).getValuesCount());
            assertEquals(accepted.getSegments(0).getBoundary(), input.getSegments(0).getBoundary());
            assertEquals(accepted.getSegments(0).getMetadataMap(), input.getSegments(0).getMetadataMap());
            assertEquals(accepted.getSegments(0).getValues(0).toBuilder().setValueHash(input.getSegments(0).getValues(0).getValueHash()).build(),
                            input.getSegments(0).getValues(0));
        }
    }

    /**
     * Checks score presence and Float bits after JSON parsing, including the distinction between absent, zero and negative-zero scores. Present score text is
     * compared with a literal formatting answer. Normalization and RESULT composition are checked separately by their owning consumers.
     */
    @ParameterizedTest(name = "score-{0}")
    @MethodSource("datawave.annotation.projection.AnnotationProjectionTestCases#scoreCases")
    void scorePresenceAndLiteralFormatting(String id, Float score, String expectedText, String expectedResult, String normalized) throws Exception {
        Annotation accepted = scored(id, score);
        Annotation.Builder parsed = Annotation.newBuilder();
        JsonFormat.parser().merge(JsonFormat.printer().print(accepted), parsed);
        SegmentValue value = parsed.getSegments(0).getValues(0);
        assertEquals(score != null, value.hasScore());
        if (score != null) {
            assertEquals(expectedText, Float.toString(value.getScore()));
            assertEquals(Float.floatToRawIntBits(score), Float.floatToRawIntBits(value.getScore()));
            // expectedResult is an independently written answer for result-composition tests.
        }
    }

    /**
     * Checks the two authored synthetic inputs against their recorded content hashes. These provenance checks apply to these authored examples, rather than to
     * every authored fixture file.
     */
    @Test
    void syntheticInputBytesMatchRecordedProvenance() throws Exception {
        for (JsonNode source : read("provenance.json").path("syntheticInputs")) {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(text(source.path("input").asText()).getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (byte b : digest) {
                hex.append(String.format("%02x", b & 255));
            }
            assertEquals(source.path("inputSha256").asText(), hex.toString());
        }
    }

    /**
     * Compares existing native identity functions and the unencoded-character synthetic grouping strategy with literal hash answers. The expected values are
     * not calculated from the implementation under test.
     */
    @Test
    void nativeMissingIdentityAndSyntheticHashAnswersStayLiteral() throws Exception {
        Annotation extraction = annotation("accepted/extraction-single-region.json");
        assertEquals("F6B9F68B", AnnotationUtils.calculateAnnotationHash(extraction));
        assertEquals("0BC9FDC2", AnnotationUtils.calculateAnnotationHash(annotation("accepted/extraction-multiple-regions.json")));
        assertEquals("58F48CA4", AnnotationUtils.calculateSourceAnalyticHash(extraction.getSource()));
        assertEquals("318DDF5130C30E298696DCB40191552E", AnnotationUtils.calculateSourceAnalyticSourceHash(extraction.getSource()));
        assertEquals("F22F0767", AnnotationUtils.calculateSegmentHash(extraction.getSegments(0)));
        assertEquals("F9AFA70A", AnnotationUtils.calculateSegmentValueHash(extraction.getSegments(0).getValues(0)));
        AnnotationSource source = content("FUTURE", "source-oracle").getSource();
        assertEquals(NATIVE_SHORT, AnnotationUtils.calculateSourceAnalyticHash(source));
        assertEquals(NATIVE_LONG, AnnotationUtils.calculateSourceAnalyticSourceHash(source));
        for (JsonNode vector : read("oracles/hash-vectors.json").path("vectors")) {
            //@formatter:off
            assertEquals(
                    vector.path("expected").asText(),
                    Hashing.murmur3_32_fixed()
                            .hashUnencodedChars(vector.path("input").asText())
                            .toString()
                            .toUpperCase(java.util.Locale.ROOT)
            );
            //@formatter:on
        }
    }

    /**
     * Demonstrates why accepted Accumulo ordering and native annotation-ID hash ordering need separate views for these Unicode hashes. Equal-key segments
     * remain in their expected relative order; the missing-ID answer stays literal, and the supplied opaque ID remains unchanged.
     */
    @Test
    @SuppressWarnings("ConstantConditions") // for the Unicode sort order demonstration below.
    void unicodeAcceptedAndNativeHashOrdersRemainDistinct() throws Exception {
        Annotation accepted = unicode(false);

        //@formatter:off
        assertEquals(
                List.of("private-use-first", "private-use-second", "supplementary"),
                accepted.getSegmentsList().stream()
                        .map(s -> s.getValues(0).getValue())
                        .collect(java.util.stream.Collectors.toList())
        );
        //@formatter:on

        // These strings represent U+E000 (private use) and U+10000 (a supplementary character).
        // Accumulo compares unsigned UTF-8 bytes, so U+E000 sorts before U+10000.
        // Java String comparison uses UTF-16 code units: U+10000 starts with U+D800,
        // which sorts before U+E000, reversing the order used by Accumulo.
        assertTrue(java.util.Arrays.compareUnsigned("\ue000".getBytes(StandardCharsets.UTF_8), "\ud800\udc00".getBytes(StandardCharsets.UTF_8)) < 0);
        assertTrue("\ue000".compareTo("\ud800\udc00") > 0);
        List<Segment> nativeOrder = new ArrayList<>(accepted.getSegmentsList());
        nativeOrder.sort(Comparator.comparing(Segment::getSegmentHash));
        assertEquals("91BA0DDF", AnnotationUtils.calculateAnnotationHash(accepted.toBuilder().clearSegments().addAllSegments(nativeOrder).build()));
        assertEquals("Opaque.Annotation:ID", unicode(true).getAnnotationId());
    }

    /**
     * Selects the nonfinite-score inputs from the preparation failure cases. Each row contains a case ID, raw JSON and a future failure category. Filtering
     * keeps both profiled and unprofiled annotation types without claiming that preparation has rejected either one.
     */
    static Stream<Arguments> nonfiniteScores() throws Exception {
        return preparationFailures().filter(row -> "NONFINITE_SCORE".equals(row.get()[2]));
    }

    /**
     * Establishes that quoted NaN and infinity values are accepted by the protobuf JSON parser as present nonfinite Floats. Common preparation must reject
     * these typed values in a separate test.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("nonfiniteScores")
    void quotedNonfiniteInputReachesTypedValidation(String id, String raw, String futureFailure) throws Exception {
        Annotation.Builder parsed = Annotation.newBuilder();
        JsonFormat.parser().merge(raw, parsed);
        assertTrue(parsed.getSegments(0).getValues(0).hasScore());
        assertFalse(Float.isFinite(parsed.getSegments(0).getValues(0).getScore()));
        // Rejection is future common-preparation acceptance, not established by parsing.
    }

    /**
     * Compares native and stored structured content with the independent decoded oracle using exact numeric nodes. Literal checks pin the large integer, tiny
     * decimal, Boolean false and numeric-looking String. Numeric spellings may differ without changing their mathematical values or JSON kinds. This does not
     * run the extension codec or a projection binding.
     */
    @Test
    void exactExtensionSpecimensKeepNumberAndBooleanKinds() throws Exception {
        String nativeInput = text("raw/extension-exact-native.json");
        assertTrue(nativeInput.contains("12.5000"));
        assertTrue(nativeInput.contains("1e-10000"));
        JsonNode expected = EXACT_JSON.readTree(text("oracles/decoded-stats.json"));
        assertTrue(expected.isObject());
        assertEquals(3, expected.size());
        assertTrue(expected.path("count").isIntegralNumber());
        assertEquals(new BigInteger("9007199254740993"), expected.path("count").bigIntegerValue());
        assertTrue(expected.path("enabled").isBoolean());
        assertFalse(expected.path("enabled").booleanValue());
        JsonNode values = expected.path("values");
        assertTrue(values.isArray());
        assertEquals(3, values.size());
        assertTrue(values.get(0).isNumber());
        assertEquals(0, new BigDecimal("12.5").compareTo(values.get(0).decimalValue()));
        assertTrue(values.get(1).isNumber());
        assertEquals(0, new BigDecimal("1E-10000").compareTo(values.get(1).decimalValue()));
        assertTrue(values.get(2).isTextual());
        assertEquals("12", values.get(2).textValue());

        JsonNode nativeExtension = EXACT_JSON.readTree(nativeInput).at("/segments/0/values/0/extension");
        assertEquals(expected, nativeExtension.path("stats"));
        assertEquals(expected, EXACT_JSON.readTree(exactStoredExtensions().get("stats")));
        assertTrue(nativeExtension.path("opaque").isTextual());
        assertEquals("{\"enabled\":false}", nativeExtension.path("opaque").textValue());
        assertEquals("{\"enabled\":false}", exactStoredExtensions().get("opaque"));
    }

    /**
     * Checks every element of the mixed-array specimen against its independent decoded answer. Numbers, Booleans, Strings and nested object/array content
     * remain distinct, and their list order is significant.
     */
    @Test
    void mixedArraySpecimensKeepKindsAndOrder() throws Exception {
        JsonNode expected = EXACT_JSON.readTree(text("oracles/decoded-array.json"));
        assertTrue(expected.isArray());
        assertEquals(4, expected.size());
        assertTrue(expected.get(0).isIntegralNumber());
        assertEquals(BigInteger.valueOf(12), expected.get(0).bigIntegerValue());
        assertTrue(expected.get(1).isBoolean());
        assertFalse(expected.get(1).booleanValue());
        assertTrue(expected.get(2).isTextual());
        assertEquals("12", expected.get(2).textValue());
        assertTrue(expected.get(3).isObject());
        JsonNode nested = expected.get(3).path("nested");
        assertTrue(nested.isArray());
        assertEquals(1, nested.size());
        assertTrue(nested.get(0).isBoolean());
        assertTrue(nested.get(0).booleanValue());
        assertEquals(expected, EXACT_JSON.readTree(text("raw/extension-native-mixed-array.json")).at("/segments/0/values/0/extension/stats"));
    }

    /**
     * Selects the accepted exact-number and mixed-array models for comparisons with the independent decoded specimens. Each row retains its case ID and
     * complete accepted Annotation; no codec or projector constructs an expected answer.
     */
    static Stream<Arguments> structuredExtensionModels() throws IOException {
        Set<String> ids = Set.of("extension-exact-native", "extension-exact-stored", "extension-numeric-boolean-projection", "extension-native-mixed-array");
        return acceptedAnnotations().filter(row -> ids.contains(row.get()[0]));
    }

    /**
     * Checks that stored JSON strings in all structured accepted models carry the same exact values and JSON kinds as their independent decoded answers.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("structuredExtensionModels")
    void acceptedStructuredExtensionsMatchDecodedAnswers(String id, Annotation accepted) throws Exception {
        String oracle = id.equals("extension-native-mixed-array") ? "oracles/decoded-array.json" : "oracles/decoded-stats.json";
        assertEquals(EXACT_JSON.readTree(text(oracle)), EXACT_JSON.readTree(accepted.getSegments(0).getValues(0).getExtensionOrThrow("stats")));
    }

    /**
     * Checks the exact-number formatting answers against BigDecimal's trailing-zero removal and text representation. Only explicit numeric input vectors are
     * considered; this does not authorize converting String values to numbers or verify the future formatting operator.
     */
    @Test
    void scalarExactNumberAnswersHaveIndependentMathematicalMeaning() throws Exception {
        for (JsonNode vector : read("oracles/scalar-format-vectors.json").path("vectors")) {
            if (vector.has("inputJson")) {
                //@formatter:off
                assertEquals(
                        vector.path("expected").asText(),
                        new BigDecimal(vector.path("inputJson").asText())
                                .stripTrailingZeros()
                                .toString()
                );
                //@formatter:on
            }
        }
    }

    /**
     * Exercises the existing segment validator with the supported boundary shapes and its UNKNOWN rejection control. No new spatial range policy or indexed
     * boundary fields are asserted.
     */
    @ParameterizedTest
    @ValueSource(strings = {"ALL", "POINTS", "TIME_MILLI", "TEXT_CHAR", "UNKNOWN"})
    void baselineBoundaryValidation(String name) throws Exception {
        BoundaryType type = BoundaryType.valueOf(name);
        //@formatter:off
        Segment segment = content("FUTURE", "boundary-control")
                .getSegments(0)
                .toBuilder()
                .setBoundary(
                        type == BoundaryType.UNKNOWN ?
                                SegmentBoundary.getDefaultInstance() :
                                bounded(type).getSegments(0).getBoundary()
                )
                .build();
        //@formatter:on
        if (type == BoundaryType.UNKNOWN) {
            assertThrows(IllegalArgumentException.class, () -> AnnotationValidators.checkSegment(segment));
        } else {
            assertTrue(AnnotationValidators.checkSegment(segment).isValid());
        }
    }

    /**
     * Checks the declared scalar field inventory and verifies JSON/binary round trips for wrappers assembled from the same all-field specimen. Wrapper coverage
     * is limited to schema serialization; it does not establish wrapper extraction or ordinary ingest acceptance.
     */
    @Test
    void schemaSpecimenAndWrappersCoverThePinnedInventory() throws Exception {
        JsonNode inventory = read("schema/inventory.json");
        Set<String> covered = new HashSet<>();
        cover(Annotation.getDescriptor(), read("schema/all-fields-annotation.json"), covered);
        Annotation full = annotation("schema/all-fields-annotation.json");

        //@formatter:off
        List<Message> wrappers = List.of(
                full,
                AnnotationList.newBuilder()
                        .addAnnotations(full)
                        .setNextPageToken("annotation-page")
                        .build(),
                SegmentList.newBuilder()
                        .addAllSegments(full.getSegmentsList())
                        .setNextPageToken("segment-page")
                        .build(),
                AnnotationMessage.newBuilder()
                        .setAnnotationMessageId("messageId")
                        .addAnnotations(full)
                        .setSource("upstream")
                        .putParameters("testParameter", "value")
                        .build()
        );
        //@formatter:on

        for (Message wrapper : wrappers) {
            String json = JsonFormat.printer().print(wrapper);
            Message.Builder parsed = wrapper.newBuilderForType();
            JsonFormat.parser().merge(json, parsed);
            assertEquals(wrapper, parsed.build());
            assertEquals(wrapper, wrapper.getParserForType().parseFrom(wrapper.toByteArray()));
            cover(wrapper.getDescriptorForType(), JSON.readTree(json), covered);
        }
        int fields = 0;
        assertEquals(9, AnnotationV1.getDescriptor().getMessageTypes().size());
        for (Descriptors.Descriptor descriptor : AnnotationV1.getDescriptor().getMessageTypes()) {
            assertEquals(inventory.path("messages").path(descriptor.getName()).size(), descriptor.getFields().size());
            for (Descriptors.FieldDescriptor field : descriptor.getFields()) {
                assertEquals(inventory.path("messages").path(descriptor.getName()).path(field.getName()).asInt(), field.getNumber());
                assertTrue(covered.contains(field.getFullName()), field.getFullName());
                fields++;
            }
        }
        assertEquals(40, fields);
        assertEquals(40, covered.size());
        for (Descriptors.EnumValueDescriptor boundary : BoundaryType.getDescriptor().getValues()) {
            assertEquals(inventory.path("boundaryTypes").path(boundary.getName()).asInt(-1), boundary.getNumber());
        }
    }

    /**
     * Records the fully qualified protobuf field names that are explicitly represented in a JSON message. Nested messages are visited recursively, including
     * every element of a repeated message field. The shared set counts each declared field once even when several specimens contain it.
     * <p>
     * A map counts as its declared field; its arbitrary keys and generated map-entry fields are not separate schema fields. Coverage here means that a JSON
     * property is present, not that its value passes validation or that an optional field remains present after protobuf parsing.
     *
     * @param descriptor
     *            the descriptor for the message represented by the JSON object
     * @param node
     *            the message's JSON object
     * @param covered
     *            the mutable set that accumulates declared field names across specimens
     */
    private static void cover(Descriptors.Descriptor descriptor, JsonNode node, Set<String> covered) {
        for (Descriptors.FieldDescriptor field : descriptor.getFields()) {
            if (!node.has(field.getJsonName())) {
                continue;
            }
            covered.add(field.getFullName());
            if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE && !field.isMapField()) {
                JsonNode child = node.path(field.getJsonName());
                if (field.isRepeated()) {
                    child.forEach(item -> cover(field.getMessageType(), item, covered));
                } else {
                    cover(field.getMessageType(), child, covered);
                }
            }
        }
    }

    /**
     * Supplies the literal presence/default vectors as rows containing a diagnostic case ID and the complete vector object. Each vector includes its input JSON
     * and only the expected properties relevant to that case, such as score presence, platform presence or default point coordinates.
     * <p>
     * These intentionally small inputs describe protobuf parsing behavior; they need not be complete annotations suitable for ingest.
     */
    static Stream<Arguments> presenceVectors() throws IOException {
        List<Arguments> rows = new ArrayList<>();
        //@formatter:off
        read("schema/presence-vectors.json")
                .path("vectors")
                .forEach(vector -> rows.add(
                        Arguments.of(vector.path("id").asText(), vector)
                ));
        //@formatter:on
        return rows.stream();
    }

    /**
     * Checks only the presence/default expectations declared by each vector and verifies binary round-trip preservation. It distinguishes optional absence from
     * explicit values without requiring the incomplete schema specimen to pass ordinary annotation validation.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("presenceVectors")
    void optionalPresenceAndDefaults(String id, JsonNode vector) throws Exception {
        Annotation.Builder builder = Annotation.newBuilder();
        JsonFormat.parser().merge(vector.path("json").toString(), builder);
        Annotation typed = builder.build();
        assertEquals(typed, Annotation.parseFrom(typed.toByteArray()));
        if (vector.has("scorePresent")) {
            assertEquals(vector.path("scorePresent").asBoolean(), typed.getSegments(0).getValues(0).hasScore());
            if (vector.has("floatText")) {
                assertEquals(vector.path("floatText").asText(), Float.toString(typed.getSegments(0).getValues(0).getScore()));
            }
        }
        if (vector.has("sourcePresent")) {
            assertEquals(vector.path("sourcePresent").asBoolean(), typed.hasSource());
        }
        if (vector.has("platformPresent")) {
            assertEquals(vector.path("platformPresent").asBoolean(), typed.getSource().hasPlatform());
        }
        if (vector.has("boundaryPresent")) {
            assertEquals(vector.path("boundaryPresent").asBoolean(), typed.getSegments(0).hasBoundary());
        }
        if (vector.has("startPresent")) {
            assertEquals(vector.path("startPresent").asBoolean(), typed.getSegments(0).getBoundary().hasStart());
            assertEquals(vector.path("endPresent").asBoolean(), typed.getSegments(0).getBoundary().hasEnd());
        }
        if (vector.has("pointX")) {
            assertEquals(vector.path("pointX").asInt(), typed.getSegments(0).getBoundary().getPoints(0).getX());
            assertEquals(vector.path("pointY").asInt(), typed.getSegments(0).getBoundary().getPoints(0).getY());
        }
    }

}
