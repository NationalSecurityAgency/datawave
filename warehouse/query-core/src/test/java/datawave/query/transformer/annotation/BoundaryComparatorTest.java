package datawave.query.transformer.annotation;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.annotation.protobuf.v1.BoundaryType;
import datawave.annotation.protobuf.v1.SegmentBoundary;

public class BoundaryComparatorTest {
    private BoundaryComparator comparator;

    private static final SegmentBoundary UNKNOWN_2_3 = SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.UNKNOWN).setStart(2).setEnd(3).build();
    private static final SegmentBoundary UNRECOGNIZED_2_3 = SegmentBoundary.newBuilder().setBoundaryTypeValue(-1).setStart(2).setEnd(3).build();
    private static final SegmentBoundary ALL_2_3 = SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.ALL).setStart(2).setEnd(3).build();
    private static final SegmentBoundary TIME_MILLI_1_2 = SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(1).setEnd(2).build();
    private static final SegmentBoundary TIME_MILLI_2_3 = SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(2).setEnd(3).build();
    private static final SegmentBoundary TIME_MILLI_2_4 = SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(2).setEnd(4).build();

    @BeforeEach
    public void setup() {
        comparator = new BoundaryComparator();
    }

    @Test
    public void unknownAfterKnownTest() {
        assertTrue(comparator.compare(UNKNOWN_2_3, TIME_MILLI_2_3) > 0);
        assertTrue(comparator.compare(TIME_MILLI_2_3, UNKNOWN_2_3) < 0);
    }

    @Test
    public void unrecognizedBeforeNothingTest() {
        assertTrue(comparator.compare(UNKNOWN_2_3, UNRECOGNIZED_2_3) < 0);
        assertTrue(comparator.compare(UNRECOGNIZED_2_3, UNKNOWN_2_3) > 0);
        assertTrue(comparator.compare(ALL_2_3, UNRECOGNIZED_2_3) < 0);
        assertTrue(comparator.compare(UNRECOGNIZED_2_3, ALL_2_3) > 0);
        assertTrue(comparator.compare(TIME_MILLI_2_3, UNRECOGNIZED_2_3) < 0);
        assertTrue(comparator.compare(UNRECOGNIZED_2_3, TIME_MILLI_2_3) > 0);
    }

    @Test
    public void allBeforeTimeTest() {
        assertTrue(comparator.compare(ALL_2_3, TIME_MILLI_2_3) < 0);
        assertTrue(comparator.compare(TIME_MILLI_2_3, ALL_2_3) > 0);
    }

    @Test
    public void boundaryBeforeStartTest() {
        assertTrue(comparator.compare(ALL_2_3, TIME_MILLI_1_2) < 0);
        assertTrue(comparator.compare(TIME_MILLI_1_2, ALL_2_3) > 0);
    }

    @Test
    public void equalBoundaryThenStartTest() {
        assertTrue(comparator.compare(TIME_MILLI_1_2, TIME_MILLI_2_3) < 0);
        assertTrue(comparator.compare(TIME_MILLI_2_3, TIME_MILLI_1_2) > 0);
    }

    @Test
    public void equalBoundaryAndStartThenEndTest() {
        assertTrue(comparator.compare(TIME_MILLI_2_4, TIME_MILLI_2_3) > 0);
        assertTrue(comparator.compare(TIME_MILLI_2_3, TIME_MILLI_2_4) < 0);
    }
}
