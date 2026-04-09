package datawave.annotation.protobuf.v1;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Base64;

import org.junit.jupiter.api.Test;

import datawave.annotation.util.v1.AnnotationUtils;

public class SegmentValueCompatibilityTest {

    @Test
    /**
     * {@code legacySegmentValueBase64} is the Base64-encoded protobuf binary produced by the previous version, where the {@code score} field in
     * {@code SegmentValue} was not explicitly declared {@code optional}.
     *
     * <p>
     * The following two statements produced the same serialized output because the default score value {@code 0.0f} was not written to the protobuf:
     *
     * <pre>{@code
     * SegmentValue segmentValue = SegmentValue.newBuilder().setValue("horse").setScore(0.0f).build();
     * SegmentValue segmentValue = SegmentValue.newBuilder().setValue("horse").build();
     * segmentValue = AnnotationUtils.injectSegmentValueHash(segmentValue);
     * String legacySegmentValueBase64 = Base64.getEncoder().encodeToString(segmentValue.toByteArray());
     * }</pre>
     */
    public void testLegacyZeroScoreDeserializationWithOptionalScore() throws Exception {
        final String legacySegmentValueBase64 = "Cgg3N0JGQjVCNxIFaG9yc2U=";
        final String legacySegmentValueHash = "77BFB5B7";

        SegmentValue segmentValue = SegmentValue.parseFrom(Base64.getDecoder().decode(legacySegmentValueBase64));
        segmentValue = AnnotationUtils.injectSegmentValueHash(segmentValue);

        // Without presence bit a 0.0 value will always deserialize as not having a score.
        assertEquals("horse", segmentValue.getValue());
        assertEquals(0.0f, segmentValue.getScore(), 0.0f);
        assertEquals(legacySegmentValueHash, segmentValue.getValueHash());
        assertFalse(segmentValue.hasScore());

        // With the score explicitly optional we can distinguish between 0.0 values being explicitly set or not
        SegmentValue segmentValueWithOutPresence = SegmentValue.newBuilder().setValue("horse").build();
        segmentValueWithOutPresence = AnnotationUtils.injectSegmentValueHash(segmentValueWithOutPresence);
        assertFalse(segmentValueWithOutPresence.hasScore());
        assertEquals(legacySegmentValueHash, segmentValueWithOutPresence.getValueHash());

        SegmentValue segmentValueWithPresence = SegmentValue.newBuilder().setValue("horse").setScore(0.0f).build();
        segmentValueWithPresence = AnnotationUtils.injectSegmentValueHash(segmentValueWithPresence);
        assertTrue(segmentValueWithPresence.hasScore());
        assertEquals(legacySegmentValueHash, segmentValueWithPresence.getValueHash());
    }
}
