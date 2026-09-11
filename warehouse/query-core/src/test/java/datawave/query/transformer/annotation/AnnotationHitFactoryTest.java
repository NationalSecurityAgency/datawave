package datawave.query.transformer.annotation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import datawave.annotation.protobuf.v1.BoundaryType;
import datawave.annotation.protobuf.v1.SegmentBoundary;
import datawave.annotation.protobuf.v1.SegmentValue;
import datawave.query.transformer.annotation.model.AllHits;

public class AnnotationHitFactoryTest {
    private static SegmentBoundary b(int start) {
        return SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(start).setEnd(start + 1).build();
    }

    @Test
    public void phraseContextIsOutsideCompletePhrase() throws Exception {
        TreeMap<SegmentBoundary,List<SegmentValue>> segments = new TreeMap<>(new BoundaryComparator());
        for (int i = 1; i <= 6; i++) {
            segments.put(b(i), List.of(SegmentValue.newBuilder().setValue("v" + i).setScore(.5f).build()));
        }
        SegmentHit first = new SegmentHit(b(2), b(3), 0);
        SegmentHit last = new SegmentHit(b(4), b(5), 0);
        PhraseHit phrase = new PhraseHit(List.of(first, last), segments, 1);

        AllHits result = new AllHitsFactory().createFromHits("a", List.of(phrase), segments, TimeUnit.MILLISECONDS);
        assertEquals(5, result.getKeywordResultList().get(0).getContext().size());
        assertEquals("v2", result.getKeywordResultList().get(0).getContext().get(0).getLabel());
        assertEquals("v6", result.getKeywordResultList().get(0).getContext().get(4).getLabel());
    }

    @Test
    public void configurableFactoryStillIntercepts() {
        TreeMap<SegmentBoundary,List<SegmentValue>> segments = new TreeMap<>(new BoundaryComparator());
        segments.put(b(1), List.of(SegmentValue.newBuilder().setValue("x").setScore(.5f).build()));
        SegmentHit hit = new SegmentHit(b(1), b(1), 0);
        hit.setContextEnd(b(1));
        assertThrows(AllHitsException.class, () -> new AllHitsFactoryErrorOnly().createFromHits("a", List.of(hit), segments));
    }
}
