package datawave.query.transformer.annotation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

import org.junit.jupiter.api.Test;

import datawave.annotation.protobuf.v1.BoundaryType;
import datawave.annotation.protobuf.v1.SegmentBoundary;
import datawave.annotation.protobuf.v1.SegmentValue;
import datawave.data.normalizer.Normalizer;

class UnorderedAnnotationMatcherTest {
    private static SegmentBoundary boundary(int start) {
        return SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(start).setEnd(start + 1).build();
    }

    private static SegmentValue value(String text) {
        return SegmentValue.newBuilder().setValue(text).setScore(.9f).build();
    }

    private static AnnotationPositionView view(String... boundaryValues) {
        TreeMap<SegmentBoundary,List<SegmentValue>> map = new TreeMap<>(new BoundaryComparator());
        for (int i = 0; i < boundaryValues.length; i++) {
            List<SegmentValue> values = new ArrayList<>();
            for (String text : boundaryValues[i].split(",", -1)) {
                values.add(value(text));
            }
            map.put(boundary(i * 1000), values);
        }
        return AnnotationPositionView.of(map, Normalizer.NOOP_NORMALIZER);
    }

    private static UnorderedAnnotationMatcher matcher(int distance, String... terms) {
        List<StandalonePatternExpression> components = new ArrayList<>();
        for (String term : terms) {
            components.add(new StandalonePatternExpression(term, 0));
        }
        return new UnorderedAnnotationMatcher(new ProximityExpression(false, components, distance));
    }

    @Test
    void reversedTermsAndOrdinalWindows() {
        assertEquals(1, matcher(1, "new", "york").match(view("york", "new"), .5f, false).size());
        assertEquals(0, matcher(1, "new", "york").match(view("new", "x", "york"), .5f, false).size());
        // The large timestamp gap is irrelevant; these are adjacent sorted boundaries.
        assertEquals(1, matcher(1, "new", "york").match(view("new", "york"), .5f, false).size());
    }

    @Test
    void flattenedVirtualSpansAreExplicitForMixedAssignments() {
        // The rows are: selected boundary ordinals, then minimum virtual span.
        List<Object[]> cases = Arrays.asList(new Object[][] {{new String[] {"a,b,c"}, 2, true, 1}, // three same-boundary values need two virtual steps
                {new String[] {"a,b", "c"}, 2, true, 1}, // two same-boundary plus the next boundary
                {new String[] {"a", "b,c"}, 2, true, 1}, {new String[] {"a", "b", "c"}, 2, true, 1}, {new String[] {"a,b", "x", "c"}, 2, false, 0},
                {new String[] {"a,b", "x", "c"}, 2, true, 0}, // boundary span 2 plus one virtual step
        });
        for (Object[] testCase : cases) {
            String[] rows = (String[]) testCase[0];
            int distance = (Integer) testCase[1];
            boolean flatten = (Boolean) testCase[2];
            int expected = (Integer) testCase[3];
            assertEquals(expected, matcher(distance, "a", "b", "c").match(view(rows), .5f, flatten).size(), Arrays.toString(rows) + " flatten=" + flatten);
        }
    }

    @Test
    void flattenFalseUsesOneComponentPerBoundaryAndFlattenTrueDeduplicatesPermutations() {
        assertEquals(0, matcher(1, "a", "b").match(view("a,b"), .5f, false).size());
        assertEquals(1, matcher(1, "a", "b").match(view("b,a"), .5f, true).size());
        assertEquals(1, matcher(1, "a", "b").match(view("a,b", "x,y"), .5f, true).size());
        assertEquals(1, matcher(1, "x", "x").match(view("x,x"), .5f, true).size());
    }

    @Test
    void configuredLongerDistanceAllowsFourComponentSpan() {
        UnorderedAnnotationMatcher fourComponents = matcher(4, "a", "b", "c", "d");

        assertEquals(1, fourComponents.match(view("a", "b", "c", "x", "d"), .5f, false).size());
        assertEquals(0, matcher(3, "a", "b", "c", "d").match(view("a", "b", "c", "x", "d"), .5f, false).size());
    }

    @Test
    void aggressivelyPrunesManyAlternativesOutsideTheAllowedWindow() {
        TreeMap<SegmentBoundary,List<SegmentValue>> map = new TreeMap<>(new BoundaryComparator());
        for (int boundaryIndex = 0; boundaryIndex < 40; boundaryIndex++) {
            List<SegmentValue> values = new ArrayList<>();
            for (int valueIndex = 0; valueIndex < 20; valueIndex++) {
                values.add(value("a"));
                values.add(value("b"));
                values.add(value("c"));
                values.add(value("d"));
            }
            map.put(boundary(boundaryIndex), values);
        }
        AnnotationPositionView positionView = AnnotationPositionView.of(map, Normalizer.NOOP_NORMALIZER);
        assertTimeout(Duration.ofSeconds(2), () -> assertEquals(0, matcher(1, "a", "b", "c", "d").match(positionView, .5f, false).size()));
    }

    @Test
    void everyConstituentMeetsMinScoreAndOverlappingAssignmentsSurvive() {
        TreeMap<SegmentBoundary,List<SegmentValue>> map = new TreeMap<>(new BoundaryComparator());
        map.put(boundary(0), Arrays.asList(value("a"), SegmentValue.newBuilder().setValue("b").setScore(.1f).build()));
        map.put(boundary(1), Collections.singletonList(SegmentValue.newBuilder().setValue("b").setScore(.1f).build()));
        assertEquals(0, matcher(1, "a", "b").match(AnnotationPositionView.of(map, Normalizer.NOOP_NORMALIZER), .5f, false).size());
        assertEquals(3, matcher(1, "a", "b").match(view("a", "b", "a", "b"), .5f, false).size());
    }
}
