package datawave.annotation.util.v1;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;

import datawave.annotation.data.v1.AccumuloAnnotationSerializer;
import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.protobuf.v1.SegmentValue;
import datawave.annotation.protobuf.v1.VectorPayload;
import datawave.annotation.test.v1.AnnotationTestDataUtil;

class EmbeddingAnnotationTest {
    @ParameterizedTest
    @CsvSource({"upstream-embedding-f16.json,FLOAT16,2130,0D5A3770", "upstream-embedding-f32.json,FLOAT32,3682,0D5A3770"})
    void upstreamVectorsRoundTripThroughJsonProtobufAndAccumulo(String fixture, String elementType, int byteCount, String valueHash) throws Exception {
        Annotation input = readFixture(fixture);
        SegmentValue value = input.getSegments(0).getValues(0);
        VectorPayload vector = value.getVector();

        assertTrue(value.hasVector());
        assertFalse(value.hasValue());
        assertEquals(VectorPayload.Format.ARROW_FILE, vector.getFormat());
        assertEquals(VectorPayload.ElementType.valueOf(elementType), vector.getElementType());
        assertEquals(List.of(768), vector.getShapeList());
        assertEquals("embed", vector.getFieldName());
        assertEquals("IMAGE", vector.getModality());
        assertEquals("image-space-v1", vector.getEmbeddingSpaceId());
        assertEquals(byteCount, vector.getData().size());
        assertTrue(vector.hasNormalized());
        assertFalse(vector.getNormalized());
        assertEquals(valueHash, value.getValueHash());
        assertEquals(valueHash, AnnotationUtils.calculateSegmentValueHash(value));
        assertTrue(AnnotationValidators.checkSegment(input.getSegments(0)).isValid());

        assertEquals(input, Annotation.parseFrom(input.toByteArray()));
        String json = AnnotationJsonUtils.getPrinter().print(input);
        assertTrue(json.contains("\"normalized\": false"));
        Annotation.Builder jsonRoundTrip = Annotation.newBuilder();
        JsonFormat.parser().merge(json, jsonRoundTrip);
        assertEquals(input, jsonRoundTrip.build());

        Annotation context = AnnotationTestDataUtil.generateTestAnnotation();
        Annotation prepared = input.toBuilder().setShard(context.getShard()).setDataType(context.getDataType()).setUid(context.getUid())
                        .putAllMetadata(context.getMetadataMap()).setAnalyticSourceHash(input.getSource().getAnalyticSourceHash())
                        .setSource(input.getSource().toBuilder().putAllMetadata(context.getSource().getMetadataMap())).build();
        prepared = AnnotationUtils.injectAnnotationHash(prepared);

        AccumuloAnnotationSerializer serializer = new AccumuloAnnotationSerializer();
        Annotation restored = serializer.deserialize(serializer.serialize(prepared));
        assertEquals(prepared.getSegmentsList(), restored.getSegmentsList());
        assertEquals(valueHash, restored.getSegments(0).getValues(0).getValueHash());
        assertEquals(vector, restored.getSegments(0).getValues(0).getVector());
        assertEquals(prepared.getAnalyticSourceHash(), restored.getAnalyticSourceHash());
    }

    @Test
    void vectorHashesFollowEmbeddingSpaceIdentity() throws Exception {
        SegmentValue first = readFixture("upstream-embedding-f16.json").getSegments(0).getValues(0);
        SegmentValue sameSpace = readFixture("upstream-embedding-f32.json").getSegments(0).getValues(0);
        assertNotEquals(first.getVector().getData(), sameSpace.getVector().getData());
        assertEquals("0D5A3770", AnnotationUtils.calculateSegmentValueHash(first));
        assertEquals(first.getValueHash(), AnnotationUtils.calculateSegmentValueHash(sameSpace));

        SegmentValue otherSpace = first.toBuilder().setVector(first.getVector().toBuilder().setEmbeddingSpaceId("image-space-v2")).build();
        assertNotEquals(first.getValueHash(), AnnotationUtils.calculateSegmentValueHash(otherSpace));
        SegmentValue scalar = SegmentValue.newBuilder().setValue(first.getVector().getEmbeddingSpaceId()).build();
        assertNotEquals(first.getValueHash(), AnnotationUtils.calculateSegmentValueHash(scalar));
    }

    @ParameterizedTest
    @ValueSource(strings = {"upstream-embedding-f16.json", "upstream-embedding-f32.json"})
    void annotationToJsonWithIdsPreservesProducerVectorIdentity(String fixture) throws Exception {
        Annotation input = readFixture(fixture);
        Annotation restored = AnnotationJsonUtils.annotationFromJson(AnnotationJsonUtils.annotationToJsonWithIds(input));
        assertEquals(input.getSegmentsList(), restored.getSegmentsList());
    }

    @Test
    void rejectsAmbiguousScalarAndVectorValuesIncludingAnEmptyScalar() throws Exception {
        Segment original = readFixture("upstream-embedding-f16.json").getSegments(0);
        SegmentValue vector = original.getValues(0);
        for (String scalar : List.of("", "scalar")) {
            Segment both = original.toBuilder().setValues(0, vector.toBuilder().setValue(scalar)).build();
            assertFalse(AnnotationValidators.checkSegment(both).isValid());
        }
        Segment neither = original.toBuilder().setValues(0, vector.toBuilder().clearVector()).build();
        assertFalse(AnnotationValidators.checkSegment(neither).isValid());
    }

    @Test
    void validatesVectorDescriptorsAndProvidedChecksum() throws Exception {
        Segment original = readFixture("upstream-embedding-f16.json").getSegments(0);
        SegmentValue value = original.getValues(0);
        VectorPayload vector = value.getVector();
        assertTrue(AnnotationValidators.checkSegment(original).isValid());

        List<VectorPayload> invalid = List.of(vector.toBuilder().clearFormat().build(), vector.toBuilder().clearData().build(),
                        vector.toBuilder().clearShape().build(), vector.toBuilder().setShape(0, 0).build(), vector.toBuilder().setShape(0, -1).build(),
                        vector.toBuilder().clearElementType().build(), vector.toBuilder().setFieldName(" ").build(), vector.toBuilder().clearModality().build(),
                        vector.toBuilder().setModality(" ").build(), vector.toBuilder().clearNormalized().build(),
                        vector.toBuilder().clearEmbeddingSpaceId().build(), vector.toBuilder().setEmbeddingSpaceId(" ").build(),
                        vector.toBuilder().setSha256("0".repeat(64)).build(), vector.toBuilder().setData(ByteString.copyFromUtf8("changed payload")).build());
        for (VectorPayload bad : invalid) {
            Segment segment = original.toBuilder().setValues(0, value.toBuilder().setVector(bad)).build();
            assertFalse(AnnotationValidators.checkSegment(segment).isValid());
        }

        // Upstream accepts absent checksums and verifies them when supplied.
        Segment withoutChecksum = original.toBuilder().setValues(0, value.toBuilder().setVector(vector.toBuilder().clearSha256())).build();
        assertTrue(AnnotationValidators.checkSegment(withoutChecksum).isValid());
    }

    @Test
    void preservesLegacyScalarValidationAndBinaryValues() throws Exception {
        Segment original = AnnotationTestDataUtil.generateMultiTestSegment();
        assertTrue(AnnotationValidators.checkSegment(original).isValid());
        for (SegmentValue value : original.getValuesList()) {
            assertTrue(value.hasValue());
            assertFalse(value.hasVector());
            assertEquals(value.getValue(), SegmentValue.parseFrom(value.toByteString()).getValue());
        }
        Segment blank = original.toBuilder().setValues(0, SegmentValue.newBuilder().setValue(" ")).build();
        assertFalse(AnnotationValidators.checkSegment(blank).isValid());
    }

    private static Annotation readFixture(String filename) throws Exception {
        try (InputStream stream = EmbeddingAnnotationTest.class.getResourceAsStream("/embedding/" + filename)) {
            assertNotNull(stream, filename);
            Annotation.Builder builder = Annotation.newBuilder();
            JsonFormat.parser().merge(new String(stream.readAllBytes(), StandardCharsets.UTF_8), builder);
            return builder.build();
        }
    }
}
