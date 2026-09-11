package datawave.query.transformer.annotation;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

import datawave.annotation.protobuf.v1.BoundaryType;
import datawave.annotation.protobuf.v1.SegmentBoundary;
import datawave.annotation.protobuf.v1.SegmentValue;
import datawave.data.normalizer.Normalizer;

class StandaloneAnnotationMatcherTest {
    @Test
    void matchesWholeNormalizedValuesAndHonorsScoreThreshold() {
        SegmentBoundary boundary = SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(0).setEnd(1).build();
        TreeMap<SegmentBoundary,List<SegmentValue>> sorted = new TreeMap<>(new BoundaryComparator());
        sorted.put(boundary, List.of(SegmentValue.newBuilder().setValue("alphabet").setScore(.2f).build(),
                        SegmentValue.newBuilder().setValue("alpha").setScore(.8f).build()));
        AnnotationPositionView view = AnnotationPositionView.of(sorted, Normalizer.NOOP_NORMALIZER);

        assertEquals(1, new StandaloneAnnotationMatcher(Collections.singleton(Pattern.compile("alpha"))).match(view, 0, .5f).size());
        assertEquals(0, new StandaloneAnnotationMatcher(Collections.singleton(Pattern.compile("alpha"))).match(view, 0, .9f).size());
    }
}
