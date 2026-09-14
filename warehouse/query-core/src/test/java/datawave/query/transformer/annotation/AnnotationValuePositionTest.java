package datawave.query.transformer.annotation;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collection;
import java.util.Collections;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

import datawave.annotation.protobuf.v1.BoundaryType;
import datawave.annotation.protobuf.v1.SegmentBoundary;
import datawave.annotation.protobuf.v1.SegmentValue;
import datawave.data.normalizer.Normalizer;
import datawave.query.transformer.annotation.model.AllHits;

class AnnotationValuePositionTest {
    private static SegmentBoundary boundary(long start) {
        return SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart((int) start).setEnd((int) start + 1).build();
    }

    @Test
    void viewUsesBoundaryOrdinalsAndScoreSortedValueIndexes() {
        TreeMap<SegmentBoundary,java.util.List<SegmentValue>> sorted = new TreeMap<>(new BoundaryComparator());
        SegmentBoundary first = boundary(10);
        SegmentBoundary second = boundary(20);
        sorted.put(second, java.util.List.of(SegmentValue.newBuilder().setValue("late").setScore(.1f).build(),
                        SegmentValue.newBuilder().setValue("best").setScore(.9f).build()));
        sorted.put(first, java.util.List.of(SegmentValue.newBuilder().setValue("first").setScore(.5f).build()));

        AnnotationPositionView view = AnnotationPositionView.of(sorted, Normalizer.NOOP_NORMALIZER);

        assertEquals(first, view.getBoundaries().get(0));
        assertEquals(0, view.getValues(0).get(0).getBoundaryIndex());
        assertEquals(1, view.getValues(1).get(1).getValueIndex());
        assertEquals("best", view.getValues(1).get(1).getValue().getValue());
    }

    @Test
    void matcherIndexResolvesAgainstFinalScoreSortedValues() throws AllHitsException {
        SegmentBoundary boundary = boundary(0);
        TreeMap<SegmentBoundary,java.util.List<SegmentValue>> sorted = new TreeMap<>(new BoundaryComparator());
        // This is the final list consumed by AllHitsFactory: score ascending.
        sorted.put(boundary, java.util.List.of(SegmentValue.newBuilder().setValue("low").setScore(.1f).build(),
                        SegmentValue.newBuilder().setValue("high").setScore(.9f).build()));
        AnnotationPositionView view = AnnotationPositionView.of(sorted, Normalizer.NOOP_NORMALIZER);

        java.util.List<SegmentHit> hits = new StandaloneAnnotationMatcher(Collections.singleton(Pattern.compile("high"))).match(view, 0, 0);
        AllHits result = new AllHitsFactory().create("annotation", hits, sorted);

        assertEquals(1, hits.size());
        assertEquals(1, hits.get(0).getValueHitIndex());
        assertEquals("high", result.getKeywordResultList().get(0).getContext().get(0).getHits().iterator().next().getLabel());
    }

    @Test
    void normalizationOccursOncePerValue() {
        AtomicInteger calls = new AtomicInteger();
        Normalizer<String> normalizer = new Normalizer<>() {
            public String normalize(String value) {
                calls.incrementAndGet();
                return value.toLowerCase();
            }

            public String normalizeDelegateType(String value) {
                return normalize(value);
            }

            public String denormalize(String value) {
                return value;
            }

            public String normalizeRegex(String value) {
                return value;
            }

            public boolean normalizedRegexIsLossy(String value) {
                return false;
            }

            public Collection<String> expand(String value) {
                return Collections.singleton(value);
            }
        };
        TreeMap<SegmentBoundary,java.util.List<SegmentValue>> sorted = new TreeMap<>(new BoundaryComparator());
        sorted.put(boundary(0), java.util.List.of(SegmentValue.newBuilder().setValue("One").build(), SegmentValue.newBuilder().setValue("Two").build()));

        AnnotationPositionView view = AnnotationPositionView.of(sorted, normalizer);
        new StandaloneAnnotationMatcher(Collections.singleton(Pattern.compile("one"))).match(view, 0, 0);

        assertEquals(2, calls.get());
    }
}
