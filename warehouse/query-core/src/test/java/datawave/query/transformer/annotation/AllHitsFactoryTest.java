package datawave.query.transformer.annotation;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.TreeMap;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import datawave.annotation.protobuf.v1.BoundaryType;
import datawave.annotation.protobuf.v1.SegmentBoundary;
import datawave.annotation.protobuf.v1.SegmentValue;
import datawave.query.transformer.annotation.model.AllHits;

public class AllHitsFactoryTest {
    private AllHitsFactory factory;
    private ObjectMapper objectMapper;

    @Before
    public void setup() {
        factory = new AllHitsFactory();
        objectMapper = new ObjectMapper();
    }

    @Test
    public void noHitsTest() throws JsonProcessingException, AllHitsException {
        AllHits result = factory.create("123", List.of(), new TreeMap<>(new BoundaryComparator()));
        assertEquals(objectMapper.writeValueAsString(new AllHits()), objectMapper.writeValueAsString(result));
    }

    @Test
    public void hitWithNoContextTest() throws JsonProcessingException, AllHitsException {
        SegmentBoundary boundary = SegmentBoundary.newBuilder().build();
        AnnotationHitsTransformer.SegmentHit hit = new AnnotationHitsTransformer.SegmentHit(boundary, boundary, 0);
        AllHits result = factory.create("123", List.of(hit), new TreeMap<>(new BoundaryComparator()));
        assertEquals(objectMapper.writeValueAsString(new AllHits()), objectMapper.writeValueAsString(result));
    }

    @Test(expected = AllHitsException.class)
    public void contextWithNoValuesOnHitTest() throws AllHitsException {
        SegmentBoundary boundary = SegmentBoundary.newBuilder().build();
        AnnotationHitsTransformer.SegmentHit hit = new AnnotationHitsTransformer.SegmentHit(boundary, boundary, 0);
        TreeMap<SegmentBoundary,List<SegmentValue>> context = new TreeMap<>(new BoundaryComparator());
        context.put(SegmentBoundary.newBuilder().build(), List.of());
        factory.create("123", List.of(hit), context);
    }

    @Test(expected = AllHitsException.class)
    public void contextWithNoValuesOnContextTest() throws AllHitsException {
        SegmentBoundary startBoundary = SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(2).setEnd(3).build();
        SegmentBoundary hitBoundary = SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(3).setEnd(4).build();
        AnnotationHitsTransformer.SegmentHit hit = new AnnotationHitsTransformer.SegmentHit(startBoundary, hitBoundary, 0);
        TreeMap<SegmentBoundary,List<SegmentValue>> context = new TreeMap<>(new BoundaryComparator());
        context.put(startBoundary, List.of());
        context.put(hitBoundary, List.of(SegmentValue.newBuilder().setValue("hit").setScore(.3f).build()));
        factory.create("123", List.of(hit), context);
    }

    @Test(expected = AllHitsException.class)
    public void hitWithInvalidHitIndexTest() throws AllHitsException {
        SegmentBoundary hitBoundary = SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(3).setEnd(4).build();
        AnnotationHitsTransformer.SegmentHit hit = new AnnotationHitsTransformer.SegmentHit(hitBoundary, hitBoundary, 1);
        TreeMap<SegmentBoundary,List<SegmentValue>> context = new TreeMap<>(new BoundaryComparator());
        context.put(hitBoundary, List.of(SegmentValue.newBuilder().setValue("hit").setScore(.3f).build()));
        factory.create("123", List.of(hit), context);
    }
}
