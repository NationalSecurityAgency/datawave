package datawave.query.transformer.annotation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import datawave.annotation.protobuf.v1.SegmentValue;

public class SegmentValueByScoreComparatorTest {
    private SegmentValueByScoreComparator comparator;

    private static final SegmentValue A_1_0 = SegmentValue.newBuilder().setValue("a").setScore(1.0f).build();
    private static final SegmentValue B_1_0 = SegmentValue.newBuilder().setValue("b").setScore(1.0f).build();
    private static final SegmentValue A_0_5 = SegmentValue.newBuilder().setValue("a").setScore(0.5f).build();
    private static final SegmentValue C_0_5 = SegmentValue.newBuilder().setValue("c").setScore(0.5f).build();

    @Before
    public void setup() {
        comparator = new SegmentValueByScoreComparator();
    }

    @Test
    public void lowerScoreSortsFirstTest() {
        assertTrue(comparator.compare(A_1_0, A_0_5) > 0);
        assertTrue(comparator.compare(A_0_5, A_1_0) < 0);
        assertTrue(comparator.compare(A_1_0, C_0_5) > 0);
        assertTrue(comparator.compare(C_0_5, A_1_0) < 0);
    }

    @Test
    public void lowerValueSortsFirstWithEqualScoresTest() {
        assertTrue(comparator.compare(A_1_0, B_1_0) < 0);
        assertTrue(comparator.compare(B_1_0, A_1_0) > 0);
    }

    @Test
    public void sameIsSameTest() {
        assertEquals(0, comparator.compare(A_1_0, A_1_0));
    }
}
