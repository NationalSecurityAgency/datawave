package datawave.ingest.annotation.projection;

import static datawave.annotation.projection.AnnotationProjectionTestCases.JSON;
import static datawave.annotation.projection.AnnotationProjectionTestCases.annotation;
import static datawave.annotation.projection.AnnotationProjectionTestCases.content;
import static datawave.annotation.projection.AnnotationProjectionTestCases.read;
import static datawave.annotation.projection.AnnotationProjectionTestCases.sourceAccepted;
import static datawave.annotation.projection.AnnotationProjectionTestCases.text;
import static datawave.annotation.projection.AnnotationProjectionTestCases.unbound;
import static datawave.annotation.projection.AnnotationProjectionTestCases.unicode;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;

import datawave.annotation.data.v1.AccumuloAnnotationSerializer;
import datawave.annotation.data.v1.AccumuloAnnotationSourceSerializer;
import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.annotation.protobuf.v1.Segment;
import datawave.data.type.NumberType;
import datawave.data.type.Type;
import datawave.ingest.annotation.mapreduce.input.SimpleAnnotationRecordReader;
import datawave.ingest.data.TypeRegistry;

/**
 * Checks ingest normalization, reader framing and existing persistence serializers using the shared core projection fixtures and independent consumer answers.
 * <p>
 * These checks do not execute the planned preparation, projector, routed indexing or retrieval implementation. Reader and mutation resources belong to this
 * consumer; shared annotation inputs and logical-field answers come from the core projection test artifact.
 */
class IngestAnnotationProjectionFixtureContractTest {
    @TempDir
    java.nio.file.Path temp;

    /**
     * Checks the consumer's NumberType against the literal normalized score answers supplied by the shared cases. Parsing and score presence are covered in the
     * core fixture suite; this check owns normalization only. An absent score has no normalized term.
     */
    @ParameterizedTest(name = "score-normalization-{0}")
    @MethodSource("datawave.annotation.projection.AnnotationProjectionTestCases#scoreCases")
    void scoreLiteralNormalization(String id, Float score, String expectedText, String expectedResult, String normalized) {
        if (score == null) {
            assertEquals(null, normalized);
        } else {
            assertEquals(normalized, new NumberType().normalize(expectedText));
        }
    }

    /**
     * Checks the normalized terms in the independent logical-field goldens using the Types declared in their profiles. The values are already formatted
     * expected answers; this test does not run the projector or assert routed table output.
     */
    @ParameterizedTest(name = "{0}")
    //@formatter:off
    @ValueSource(strings = {
            "extraction-single-region",
            "extraction-multiple-regions",
            "binary-label",
            "multiclass-label",
            "extension-numeric-boolean-projection"
    })
    //@formatter:on
    void independentLogicalGoldenNormalization(String id) throws Exception {
        //@formatter:off
        assertFieldNormalization(
                read("expected/" + id + ".json").path("logicalFields"),
                id.startsWith("extension-") ?
                        "configuration/exact-extension-profile.json" :
                        "configuration/baseline-profiles.json"
        );
        //@formatter:on
    }

    /**
     * Applies the same normalization checks to the small score and padding variants, keeping their field answers independent of the future projector.
     */
    @Test
    void variantFieldAnswersStayIndependentAndNormalize() throws Exception {
        for (JsonNode row : read("oracles/field-format-cases.json").path("cases")) {
            assertFieldNormalization(row.path("logicalFields"), "configuration/baseline-profiles.json");
        }
    }

    /**
     * Looks up each expected field's configured Type, creates that Type and normalizes the field's literal value. The result must match the independently
     * stored normalized term.
     * <p>
     * Only Type compatibility is checked here. Reading the profile as JSON does not exercise registry loading, policy validation, expression evaluation or
     * storage flags.
     *
     * @param fields
     *            expected logical-field entries containing base, value and normalized properties
     * @param resource
     *            the profile document path relative to the fixture resource directory
     */
    private static void assertFieldNormalization(JsonNode fields, String resource) throws Exception {
        JsonNode profiles = read(resource);
        Map<String,String> classes = new HashMap<>();

        //@formatter:off
        profiles.path("profiles").forEach(
                profile -> profile.path("datawave").path("fields").fields().forEachRemaining(
                        field -> classes.put(field.getKey(), field.getValue().path("typeClass").asText())
                )
        );
        //@formatter:on

        for (JsonNode field : fields) {
            Type<?> type = (Type<?>) Class.forName(classes.get(field.path("base").asText())).getConstructor().newInstance();
            assertEquals(field.path("normalized").asText(), type.normalize(field.path("value").asText()), field.toString());
        }
    }

    /**
     * Checks the stored source key and decoded payload for the independently expected source identity in each reference case. The supplied-input columns
     * identify the future completion scenario; this test serializes its accepted state and does not perform completion or reference reconciliation.
     */
    @ParameterizedTest(name = "source-reference-{0}")
    @MethodSource("datawave.annotation.projection.AnnotationProjectionTestCases#sourceReferences")
    void sourceRowMatchesLiteralIdentity(String variant, String suppliedShort, String suppliedLong, String reference, String expectedShort, String expectedLong)
                    throws Exception {
        // This checks persistence of the independently expected source. Separate preparation tests
        // must run sourceInput(...) through source completion and reference validation.
        Annotation accepted = sourceAccepted(variant, expectedShort, expectedLong);
        Map.Entry<Key,Value> entry = new AccumuloAnnotationSourceSerializer().serialize(accepted.getSource()).next();
        //@formatter:off
        assertEquals(
                new Key(expectedLong, "d", "test-engine\0test-model-v1\0" + expectedShort, "PUBLIC", 1795392000000L),
                entry.getKey()
        );
        //@formatter:on
        assertEquals(accepted.getSource(), AnnotationSource.parseFrom(entry.getValue().get()));
    }

    /**
     * Runs the actual baseline reader against temporary files using single-object, NDJSON, adjacent-object and root-array framing. A malformed root must fail
     * during initialization before an event is read. The test closes its input and resets the shared type registry after every case.
     * <p>
     * This establishes framing behavior, not the future byte/node/depth guards or downstream preparation and indexing.
     */
    @ParameterizedTest
    @ValueSource(strings = {"single-object", "ndjson", "adjacent-objects", "root-array", "malformed-root"})
    void actualBaselineReaderFraming(String framing) throws Exception {
        String first = text("raw/extraction-single-region.json");
        String second = JsonFormat.printer().print(unbound(content("FUTURE", "Future-v1").build(), true));
        String data;
        switch (framing) {
            case "single-object":
                data = first;
                break;
            case "ndjson":
                data = first + "\n" + second + "\n";
                break;
            case "adjacent-objects":
                data = first + second;
                break;
            case "root-array":
                data = "[" + first + "," + second + "]";
                break;
            default:
                data = text("raw/reader-malformed-root.json");
        }
        java.nio.file.Path file = temp.resolve(framing + ".json");
        Files.writeString(file, data);
        Configuration configuration = new Configuration();
        configuration.addResource(getClass().getResource("/config/all-config.xml"));
        configuration.addResource(getClass().getResource("/config/test-annotation-ingest-config.xml"));
        TypeRegistry.reset();
        TypeRegistry.getInstance(configuration);
        SimpleAnnotationRecordReader recordReader = new SimpleAnnotationRecordReader() {
            @Override
            public synchronized void close() throws IOException {
                try {
                    if (reader != null) {
                        reader.close();
                    }
                } finally {
                    super.close();
                }
            }
        };
        try {
            FileSplit split = new FileSplit(new org.apache.hadoop.fs.Path(file.toUri()), 0, Files.size(file), null);
            TaskAttemptContextImpl context = new TaskAttemptContextImpl(configuration, new TaskAttemptID());
            if (framing.equals("malformed-root")) {
                assertThrows(RuntimeException.class, () -> recordReader.initialize(split, context));
                return;
            }
            recordReader.initialize(split, context);
            recordReader.setInputDate(1795392000000L);
            List<String> types = new ArrayList<>();
            while (recordReader.nextKeyValue()) {
                types.add(JSON.readTree(recordReader.getEvent().getRawData()).path("annotationType").asText());
            }
            //@formatter:off
            assertEquals(
                    framing.equals("single-object") ? List.of("TEXT_EXTRACTION") : List.of("TEXT_EXTRACTION", "FUTURE"),
                    types
            );
            //@formatter:on
        } finally {
            recordReader.close();
            TypeRegistry.reset();
        }
    }

    /**
     * Compares the existing annotation/source serializers with complete independent persistence keys, typed values and submission counts. Matching as a
     * multiset retains duplicates while allowing incidental iteration order to differ.
     * <p>
     * Event and index entries may be present in the same golden file, but this test compares only annotation/source persistence. It does not verify the
     * coordinating handler, mapper counters, storage retention or readback.
     */
    @ParameterizedTest
    //@formatter:off
    @ValueSource(strings = {
            "extraction-single-region",
            "extraction-multiple-regions",
            "binary-label",
            "profile-miss",
            "ordering-unicode-missing-id"
    })
    //@formatter:on
    void persistenceMatchesIndependentGoldenAsMultiset(String id) throws Exception {
        Annotation accepted;
        switch (id) {
            case "profile-miss":
                accepted = content("FUTURE", "Future-v1").build();
                break;
            case "ordering-unicode-missing-id":
                accepted = unicode(false);
                break;
            default:
                accepted = annotation("accepted/" + id + ".json");
        }
        Map<List<Object>,Integer> actual = new HashMap<>(), expected = new HashMap<>();
        //@formatter:off
        new AccumuloAnnotationSerializer()
                .serialize(accepted)
                .forEachRemaining(entry -> add(
                        actual,
                        "datawave.annotation",
                        entry.getKey(),
                        entry.getValue().getSize() == 0 ? ByteString.EMPTY : parseSegment(entry.getValue())
                ));
        new AccumuloAnnotationSourceSerializer()
                .serialize(accepted.getSource())
                .forEachRemaining(entry -> add(
                        actual,
                        "datawave.annotationSource",
                        entry.getKey(),
                        parseSource(entry.getValue())
                ));
        //@formatter:on
        JsonNode golden = read("expected/" + id + "-mutations.json");
        for (JsonNode mutation : golden.path("mutations")) {
            if (!mutation.path("table").asText().startsWith("datawave.annotation")) {
                continue;
            }
            Object value = ByteString.EMPTY;
            switch (mutation.path("value").path("kind").asText()) {
                case "protobuf-source":
                    value = accepted.getSource();
                    break;
                case "protobuf-segment":
                    value = accepted.getSegments(mutation.path("value").path("acceptedSegment").asInt());
                    break;
                default:
                    break;
            }
            //@formatter:off
            Key key = new Key(
                    mutation.path("row").asText(),
                    mutation.path("columnFamily").asText(),
                    mutation.path("columnQualifier").asText(),
                    mutation.path("visibility").asText(),
                    mutation.path("timestamp").asLong()
            );
            //@formatter:on
            add(expected, mutation.path("table").asText(), key, value);
        }
        assertEquals(expected, actual);
        assertEquals(golden.path("counts").path("persistence").asInt(), actual.values().stream().mapToInt(Integer::intValue).sum());
    }

    /**
     * Decodes a serialized segment for comparison by protobuf content rather than raw bytes. Invalid bytes fail the test with the parsing exception retained as
     * the cause.
     */
    private static Segment parseSegment(Value value) {
        try {
            return Segment.parseFrom(value.get());
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Decodes a serialized source for comparison by protobuf content, including its maps. Invalid bytes fail the test with the parsing exception retained as
     * the cause.
     */
    private static AnnotationSource parseSource(Value value) {
        try {
            return AnnotationSource.parseFrom(value.get());
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Records one serialized submission in an expected or actual multiset. Its identity is the destination table, complete Accumulo key and decoded value; an
     * identical submission increments the existing count rather than disappearing.
     * <p>
     * Decoded protobuf values compare by message content, so differences in map wire order do not affect the comparison. This helper only accumulates test
     * observations; it does not write data or model table compaction.
     *
     * @param rows
     *            the mutable map from submission identity to occurrence count
     * @param table
     *            the destination table name
     * @param key
     *            the complete key, including visibility and timestamp
     * @param value
     *            a decoded protobuf message, or {@link ByteString#EMPTY} for an empty value
     */
    private static void add(Map<List<Object>,Integer> rows, String table, Key key, Object value) {
        rows.merge(List.of(table, key, value), 1, Integer::sum);
    }

}
