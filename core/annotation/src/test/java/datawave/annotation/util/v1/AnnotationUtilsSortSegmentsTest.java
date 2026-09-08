package datawave.annotation.util.v1;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.test.v1.AnnotationTestDataUtil;

/** Tests {@link AnnotationUtils#sortSegments(List)} and its use from {@link AnnotationUtils#injectAnnotationHash(Annotation)}. */
public class AnnotationUtilsSortSegmentsTest {

    @Test
    public void testSortSegmentsOrdersByHash() {
        // audio segments are generated unhashed and in a non-hash-sorted (temporal) order.
        List<Segment> unsortedSegments = AnnotationTestDataUtil.generateAudioSegments("20250405", "123");
        assertTrue(unsortedSegments.size() > 1, "test requires more than one segment to validate ordering");

        List<Segment> sortedSegments = AnnotationUtils.sortSegments(unsortedSegments);
        List<String> segmentHashes = sortedSegments.stream().map(Segment::getSegmentHash).collect(Collectors.toList());

        // every segment must have had its hash calculated and injected as a side effect of sorting.
        segmentHashes.forEach(hash -> assertFalse(hash.isEmpty()));

        List<String> expectedSortedHashes = new ArrayList<>(segmentHashes);
        Collections.sort(expectedSortedHashes);
        assertEquals(expectedSortedHashes, segmentHashes);

        // the set of hashes should be unaffected by sorting - only the order should change.
        List<String> unsortedHashes = unsortedSegments.stream().map(AnnotationUtils::calculateSegmentHash).sorted().collect(Collectors.toList());
        assertEquals(unsortedHashes, expectedSortedHashes);
    }

    @Test
    public void testSortSegmentsIsStableAndIdempotent() {
        List<Segment> segments = AnnotationTestDataUtil.generateAudioSegments("20250405", "123");

        List<Segment> sortedOnce = AnnotationUtils.sortSegments(segments);
        List<Segment> sortedTwice = AnnotationUtils.sortSegments(sortedOnce);

        assertEquals(sortedOnce, sortedTwice);
    }

    @Test
    public void testSortSegmentsDoesNotRecalculateHashWhenAlreadyPresent() {
        // build segments whose stored hash is deliberately stale/incorrect, and confirm sortSegments trusts an
        // already-assigned (non-blank) hash rather than redundantly recalculating it - avoiding unnecessary hash
        // computation whenever a caller (e.g. injectAllHashes(Segment)) has already injected the segment's hash.
        Segment segmentA = AnnotationUtils.injectSegmentHash(AnnotationTestDataUtil.generateTestSegment());
        Segment segmentAWithStaleHash = segmentA.toBuilder().setSegmentHash("NOT_A_REAL_HASH").build();

        Segment segmentB = AnnotationUtils.injectSegmentHash(AnnotationTestDataUtil.generateMultiTestSegment());

        List<Segment> sorted = AnnotationUtils.sortSegments(List.of(segmentAWithStaleHash, segmentB));
        List<String> hashes = sorted.stream().map(Segment::getSegmentHash).collect(Collectors.toList());

        // the stale hash is trusted as-is (not recalculated) since it was already non-blank.
        assertTrue(hashes.contains("NOT_A_REAL_HASH"));

        List<String> expectedSortedHashes = new ArrayList<>(List.of("NOT_A_REAL_HASH", segmentB.getSegmentHash()));
        Collections.sort(expectedSortedHashes);
        assertEquals(expectedSortedHashes, hashes);
    }

    @Test
    public void testInjectAnnotationHashSortsSegmentsAndIsOrderIndependent() {
        List<Segment> audioSegments = AnnotationTestDataUtil.generateAudioSegments("20250405", "123");
        assertTrue(audioSegments.size() > 1, "test requires more than one segment to validate order independence");

        Annotation baseAnnotation = AnnotationTestDataUtil.generateTestAnnotation().toBuilder().clearSegments().addAllSegments(audioSegments).build();

        List<Segment> segments = new ArrayList<>(baseAnnotation.getSegmentsList());
        Collections.reverse(segments);
        Annotation reorderedAnnotation = baseAnnotation.toBuilder().clearSegments().addAllSegments(segments).build();

        Annotation injectedFromBase = AnnotationUtils.injectAnnotationHash(baseAnnotation);
        Annotation injectedFromReordered = AnnotationUtils.injectAnnotationHash(reorderedAnnotation);

        // regardless of input segment order, the resulting segment order and annotation id must match.
        assertEquals(injectedFromBase.getSegmentsList(), injectedFromReordered.getSegmentsList());
        assertEquals(injectedFromBase.getAnnotationId(), injectedFromReordered.getAnnotationId());
        assertFalse(injectedFromBase.getAnnotationId().isEmpty());
    }
}
