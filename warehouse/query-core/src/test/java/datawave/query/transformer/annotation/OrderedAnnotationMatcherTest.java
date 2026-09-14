package datawave.query.transformer.annotation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

import datawave.annotation.protobuf.v1.BoundaryType;
import datawave.annotation.protobuf.v1.SegmentBoundary;
import datawave.annotation.protobuf.v1.SegmentValue;
import datawave.data.normalizer.Normalizer;

class OrderedAnnotationMatcherTest {
    private static SegmentBoundary boundary(long start) {
        return SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart((int) start).setEnd((int) start + 1).build();
    }

    private static AnnotationPositionView view(Object... values) {
        TreeMap<SegmentBoundary,List<SegmentValue>> map = new TreeMap<>(new BoundaryComparator());
        for (int i = 0; i < values.length; i++) {
            List<SegmentValue> boundaryValues = new ArrayList<>();
            for (String value : ((String) values[i]).split(",", -1)) {
                boundaryValues.add(SegmentValue.newBuilder().setValue(value).setScore(.9f).build());
            }
            map.put(boundary(i * 1000), boundaryValues);
        }
        return AnnotationPositionView.of(map, Normalizer.NOOP_NORMALIZER);
    }

    private static OrderedAnnotationMatcher matcher(String... terms) {
        List<Pattern> patterns = new ArrayList<>();
        for (String term : terms) {
            patterns.add(Pattern.compile(term));
        }
        return new OrderedAnnotationMatcher(patterns);
    }

    @Test
    void matchesAdjacentBoundariesAndIgnoresTimestampGaps() {
        assertEquals(1, matcher("new", "york").match(view("new", "york"), .5f, false).size());
        assertEquals(0, matcher("york", "new").match(view("new", "york"), .5f, false).size());
        TreeMap<SegmentBoundary,List<SegmentValue>> gap = new TreeMap<>(new BoundaryComparator());
        gap.put(boundary(0), Collections.singletonList(value("new", .9f)));
        gap.put(boundary(100000), Collections.singletonList(value("york", .9f)));
        assertEquals(1, matcher("new", "york").match(AnnotationPositionView.of(gap, Normalizer.NOOP_NORMALIZER), .5f, false).size());
    }

    @Test
    void flatteningAllowsDistinctValuesInEitherStoredOrder() {
        assertEquals(0, matcher("new", "york").match(view("new,york"), .5f, false).size());
        assertEquals(1, matcher("new", "york").match(view("york,new"), .5f, true).size());
        assertEquals(1, matcher("york", "new").match(view("york,new"), .5f, true).size());
        assertEquals(1, matcher("new", "york").match(view("new", "x,york"), .5f, true).size());
    }

    @Test
    void valuesAreAlternativesAndEveryConstituentMeetsScore() {
        TreeMap<SegmentBoundary,List<SegmentValue>> map = new TreeMap<>(new BoundaryComparator());
        map.put(boundary(0), Arrays.asList(value("new", .2f), value("old", .9f)));
        map.put(boundary(1000), Collections.singletonList(value("york", .9f)));
        assertEquals(0, matcher("new", "york").match(AnnotationPositionView.of(map, Normalizer.NOOP_NORMALIZER), .5f, false).size());
    }

    @Test
    void repeatedTermsNeedTwoValueIdentitiesAndOverlappingOccurrencesSurvive() {
        assertEquals(0, matcher("x", "x").match(view("x"), .5f, false).size());
        assertEquals(0, matcher("x", "x").match(view("x,x"), .5f, false).size());
        assertEquals(2, matcher("x", "x").match(view("x,x"), .5f, true).size());

        AnnotationPositionView overlapping = view("a", "b", "a", "b");
        List<AnnotationPhraseOccurrence> occurrences = matcher("a", "b").match(overlapping, .5f, false);
        assertEquals(2, occurrences.size());
        assertEquals(Arrays.asList(0, 1), Arrays.asList(occurrences.get(0).getConstituents().get(0).getBoundaryIndex(),
                        occurrences.get(0).getConstituents().get(1).getBoundaryIndex()));
        assertEquals(Arrays.asList(2, 3), Arrays.asList(occurrences.get(1).getConstituents().get(0).getBoundaryIndex(),
                        occurrences.get(1).getConstituents().get(1).getBoundaryIndex()));
    }

    @Test
    void duplicateCriteriaAndIdentitySequencesAreEmittedOnce() {
        List<Pattern> duplicate = Arrays.asList(Pattern.compile("a"), Pattern.compile("a"));
        assertEquals(1, new OrderedAnnotationMatcher(duplicate).match(view("a", "a"), .5f, false).size());
        assertTrue(matcher("a", "b").match(view("a", "b"), .5f, false).get(0).getConstituents().size() == 2);
    }

    @Test
    void proximityConstructorPreservesOrderAndFlagsAndRequiresExactDistanceOne() {
        List<StandalonePatternExpression> components = Arrays.asList(new StandalonePatternExpression("new"), new StandalonePatternExpression("york"));
        ProximityExpression expression = new ProximityExpression(true, components, 1);
        AnnotationPhraseOccurrence occurrence = new OrderedAnnotationMatcher(expression).match(view("new", "york"), .5f, false).get(0);
        assertEquals(Arrays.asList(0, 1),
                        Arrays.asList(occurrence.getConstituents().get(0).getBoundaryIndex(), occurrence.getConstituents().get(1).getBoundaryIndex()));

        ProximityExpression caseInsensitive = new ProximityExpression(true, Arrays.asList(new StandalonePatternExpression("NEW")), 1);
        assertEquals(1, new OrderedAnnotationMatcher(caseInsensitive).match(view("new"), .5f, false).size());
        assertThrows(IllegalArgumentException.class, () -> new OrderedAnnotationMatcher(new ProximityExpression(true, components, 2)));
        assertThrows(IllegalArgumentException.class, () -> new OrderedAnnotationMatcher(new ProximityExpression(false, components, 1)));
    }

    @Test
    void prunesManyAlternativesWhenARequiredSuccessorIsAbsent() {
        TreeMap<SegmentBoundary,List<SegmentValue>> map = new TreeMap<>(new BoundaryComparator());
        for (int i = 0; i < 30; i++) {
            List<SegmentValue> values = new ArrayList<>();
            for (int j = 0; j < 30; j++) {
                values.add(value("a", .9f));
            }
            map.put(boundary(i * 1000), values);
        }
        AnnotationPositionView positionView = AnnotationPositionView.of(map, Normalizer.NOOP_NORMALIZER);
        assertTimeout(Duration.ofSeconds(1), () -> assertEquals(0, matcher("a", "missing", "a", "a").match(positionView, .5f, true).size()));
    }

    private static SegmentValue value(String value, float score) {
        return SegmentValue.newBuilder().setValue(value).setScore(score).build();
    }
}
