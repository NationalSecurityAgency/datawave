package datawave.query.transformer.annotation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.TreeMap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import datawave.annotation.protobuf.v1.BoundaryType;
import datawave.annotation.protobuf.v1.SegmentBoundary;
import datawave.annotation.protobuf.v1.SegmentValue;
import datawave.query.transformer.annotation.model.AllHit;
import datawave.query.transformer.annotation.model.AllHits;
import datawave.query.transformer.annotation.model.Term;
import datawave.query.transformer.annotation.model.TermHit;

public class AllHitsFactoryTest {
    private AllHitsFactory factory;
    private ObjectMapper objectMapper;

    @BeforeEach
    public void setup() {
        factory = new AllHitsFactory();
        objectMapper = new ObjectMapper();
    }

    @Test
    public void noHitsTest() throws JsonProcessingException, AllHitsException {
        AllHits result = factory.create("123", List.of(), new TreeMap<>(new BoundaryComparator()));
        assertNull(result);
    }

    @Test
    public void hitWithNoContextTest() throws JsonProcessingException, AllHitsException {
        SegmentBoundary boundary = SegmentBoundary.newBuilder().build();
        AnnotationHitsTransformer.SegmentHit hit = new AnnotationHitsTransformer.SegmentHit(boundary, boundary, 0);
        hit.setContextEnd(boundary);
        AllHits result = factory.create("123", List.of(hit), new TreeMap<>(new BoundaryComparator()));
        assertNull(result);
    }

    @Test
    public void contextWithNoValuesOnHitTest() throws AllHitsException {
        SegmentBoundary boundary = SegmentBoundary.newBuilder().build();
        AnnotationHitsTransformer.SegmentHit hit = new AnnotationHitsTransformer.SegmentHit(boundary, boundary, 0);
        TreeMap<SegmentBoundary,List<SegmentValue>> context = new TreeMap<>(new BoundaryComparator());
        context.put(SegmentBoundary.newBuilder().build(), List.of());
        assertThrows(AllHitsException.class, () -> factory.create("123", List.of(hit), context));
    }

    @Test
    public void contextWithNoValuesOnContextTest() throws AllHitsException {
        SegmentBoundary startBoundary = SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(2).setEnd(3).build();
        SegmentBoundary hitBoundary = SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(3).setEnd(4).build();
        AnnotationHitsTransformer.SegmentHit hit = new AnnotationHitsTransformer.SegmentHit(startBoundary, hitBoundary, 0);
        TreeMap<SegmentBoundary,List<SegmentValue>> context = new TreeMap<>(new BoundaryComparator());
        context.put(startBoundary, List.of());
        context.put(hitBoundary, List.of(SegmentValue.newBuilder().setValue("hit").setScore(.3f).build()));
        assertThrows(AllHitsException.class, () -> factory.create("123", List.of(hit), context));
    }

    @Test
    public void hitWithInvalidHitIndexTest() throws AllHitsException {
        SegmentBoundary hitBoundary = SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(3).setEnd(4).build();
        AnnotationHitsTransformer.SegmentHit hit = new AnnotationHitsTransformer.SegmentHit(hitBoundary, hitBoundary, 1);
        TreeMap<SegmentBoundary,List<SegmentValue>> context = new TreeMap<>(new BoundaryComparator());
        context.put(hitBoundary, List.of(SegmentValue.newBuilder().setValue("hit").setScore(.3f).build()));
        assertThrows(AllHitsException.class, () -> factory.create("123", List.of(hit), context));
    }

    @Test
    public void hitWithNoEndContextTest() throws AllHitsException {
        SegmentBoundary hitBoundary = SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(3).setEnd(4).build();
        AnnotationHitsTransformer.SegmentHit hit = new AnnotationHitsTransformer.SegmentHit(hitBoundary, hitBoundary, 0);
        TreeMap<SegmentBoundary,List<SegmentValue>> context = new TreeMap<>(new BoundaryComparator());
        context.put(hitBoundary, List.of(SegmentValue.newBuilder().setValue("hit").setScore(.3f).build()));
        assertThrows(AllHitsException.class, () -> factory.create("123", List.of(hit), context));
    }

    @Test
    public void segmentWithNoValuesTest() throws AllHitsException {
        SegmentBoundary hitBoundary = SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(3).setEnd(4).build();
        AnnotationHitsTransformer.SegmentHit hit = new AnnotationHitsTransformer.SegmentHit(hitBoundary, hitBoundary, 0);
        hit.setContextEnd(hitBoundary);
        TreeMap<SegmentBoundary,List<SegmentValue>> context = new TreeMap<>(new BoundaryComparator());
        context.put(hitBoundary, List.of());
        assertThrows(AllHitsException.class, () -> factory.create("123", List.of(hit), context));
    }

    @Test
    public void hitWithOutOfBoundsIndexTest() throws AllHitsException {
        SegmentBoundary hitBoundary = SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(3).setEnd(4).build();
        AnnotationHitsTransformer.SegmentHit hit = new AnnotationHitsTransformer.SegmentHit(hitBoundary, hitBoundary, 1);
        hit.setContextEnd(hitBoundary);
        TreeMap<SegmentBoundary,List<SegmentValue>> context = new TreeMap<>(new BoundaryComparator());
        context.put(hitBoundary, List.of(SegmentValue.newBuilder().setValue("hit").setScore(.3f).build()));
        assertThrows(AllHitsException.class, () -> factory.create("123", List.of(hit), context));
    }

    @Test
    public void singleHitOnSingleValueTest() throws AllHitsException, JsonProcessingException {
        SegmentBoundary hitBoundary = SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(3).setEnd(4).build();
        AnnotationHitsTransformer.SegmentHit hit = new AnnotationHitsTransformer.SegmentHit(hitBoundary, hitBoundary, 0);
        hit.setContextEnd(hitBoundary);
        TreeMap<SegmentBoundary,List<SegmentValue>> context = new TreeMap<>(new BoundaryComparator());
        context.put(hitBoundary, List.of(SegmentValue.newBuilder().setValue("hit").setScore(.3f).build()));
        AllHits result = factory.create("123", List.of(hit), context);

        AllHits expected = new AllHits();
        expected.setAnnotationId("123");
        expected.setMaxTermHitConfidence(.3f);
        AllHit hit1 = new AllHit();
        hit1.setConfidence(.3f);
        hit1.getOneBestContext().add(getTerm("hit", .3f, 3f, 4f));
        hit1.getTermHits().add(getTermHit("hit", .3f, 3f, 4f));
        expected.getKeywordResultList().add(hit1);

        assertEquals(objectMapper.writeValueAsString(expected), objectMapper.writeValueAsString(result));
    }

    @Test
    public void multipleHitOnSameTermTest() throws AllHitsException, JsonProcessingException {
        SegmentBoundary hitBoundary = SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(3).setEnd(4).build();
        AnnotationHitsTransformer.SegmentHit segmentHit1 = new AnnotationHitsTransformer.SegmentHit(hitBoundary, hitBoundary, 0);
        segmentHit1.setContextEnd(hitBoundary);
        AnnotationHitsTransformer.SegmentHit segmentHit2 = new AnnotationHitsTransformer.SegmentHit(hitBoundary, hitBoundary, 2);
        segmentHit2.setContextEnd(hitBoundary);
        AnnotationHitsTransformer.SegmentHit segmentHit3 = new AnnotationHitsTransformer.SegmentHit(hitBoundary, hitBoundary, 1);
        segmentHit3.setContextEnd(hitBoundary);
        TreeMap<SegmentBoundary,List<SegmentValue>> context = new TreeMap<>(new BoundaryComparator());
        context.put(hitBoundary, List.of(SegmentValue.newBuilder().setValue("hit1").setScore(.3f).build(),
                        SegmentValue.newBuilder().setValue("hit3").setScore(.4f).build(), SegmentValue.newBuilder().setValue("hit2").setScore(.6f).build()));
        AllHits result = factory.create("123", List.of(segmentHit1, segmentHit2, segmentHit3), context);

        AllHits expected = new AllHits();
        expected.setAnnotationId("123");
        expected.setMaxTermHitConfidence(.6f);
        AllHit hit1 = new AllHit();
        hit1.setConfidence(.6f);
        hit1.getOneBestContext().add(getTerm("hit2", .6f, 3f, 4f));
        hit1.getTermHits().add(getTermHit("hit1", .3f, 3f, 4f));
        hit1.getTermHits().add(getTermHit("hit2", .6f, 3f, 4f));
        hit1.getTermHits().add(getTermHit("hit3", .4f, 3f, 4f));
        expected.getKeywordResultList().add(hit1);

        assertEquals(objectMapper.writeValueAsString(expected), objectMapper.writeValueAsString(result));
    }

    @Test
    public void doubleHitOnMultipleTermsTest() throws AllHitsException, JsonProcessingException {
        SegmentBoundary hitBoundary1 = SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(3).setEnd(4).build();
        SegmentBoundary hitBoundary2 = SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(4).setEnd(5).build();
        AnnotationHitsTransformer.SegmentHit segmentHit1 = new AnnotationHitsTransformer.SegmentHit(hitBoundary1, hitBoundary1, 0);
        segmentHit1.setContextEnd(hitBoundary1);
        AnnotationHitsTransformer.SegmentHit segmentHit2 = new AnnotationHitsTransformer.SegmentHit(hitBoundary2, hitBoundary2, 1);
        segmentHit2.setContextEnd(hitBoundary2);
        TreeMap<SegmentBoundary,List<SegmentValue>> context = new TreeMap<>(new BoundaryComparator());
        context.put(hitBoundary1, List.of(SegmentValue.newBuilder().setValue("hit1").setScore(.3f).build(),
                        SegmentValue.newBuilder().setValue("term1").setScore(.6f).build()));
        context.put(hitBoundary2, List.of(SegmentValue.newBuilder().setValue("term2").setScore(.3f).build(),
                        SegmentValue.newBuilder().setValue("hit2").setScore(.6f).build()));
        AllHits result = factory.create("123", List.of(segmentHit1, segmentHit2), context);

        AllHits expected = new AllHits();
        expected.setAnnotationId("123");
        expected.setMaxTermHitConfidence(.6f);
        AllHit hit1 = new AllHit();
        hit1.setConfidence(.3f);
        hit1.getOneBestContext().add(getTerm("term1", .6f, 3f, 4f));
        hit1.getTermHits().add(getTermHit("hit1", .3f, 3f, 4f));
        AllHit hit2 = new AllHit();
        hit2.setConfidence(.6f);
        hit2.getOneBestContext().add(getTerm("hit2", .6f, 4f, 5f));
        hit2.getTermHits().add(getTermHit("hit2", .6f, 4f, 5f));
        expected.getKeywordResultList().add(hit1);
        expected.getKeywordResultList().add(hit2);

        assertEquals(objectMapper.writeValueAsString(expected), objectMapper.writeValueAsString(result));
    }

    @Test
    public void doubleHitOnMultipleTermsReverseOrderToVerifyAggregateConfidenceTest() throws AllHitsException, JsonProcessingException {
        SegmentBoundary hitBoundary1 = SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(4).setEnd(5).build();
        SegmentBoundary hitBoundary2 = SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(3).setEnd(4).build();
        AnnotationHitsTransformer.SegmentHit segmentHit1 = new AnnotationHitsTransformer.SegmentHit(hitBoundary1, hitBoundary1, 0);
        segmentHit1.setContextEnd(hitBoundary1);
        AnnotationHitsTransformer.SegmentHit segmentHit2 = new AnnotationHitsTransformer.SegmentHit(hitBoundary2, hitBoundary2, 1);
        segmentHit2.setContextEnd(hitBoundary2);
        TreeMap<SegmentBoundary,List<SegmentValue>> context = new TreeMap<>(new BoundaryComparator());
        context.put(hitBoundary1, List.of(SegmentValue.newBuilder().setValue("hit1").setScore(.3f).build(),
                        SegmentValue.newBuilder().setValue("term1").setScore(.6f).build()));
        context.put(hitBoundary2, List.of(SegmentValue.newBuilder().setValue("term2").setScore(.3f).build(),
                        SegmentValue.newBuilder().setValue("hit2").setScore(.6f).build()));
        AllHits result = factory.create("123", List.of(segmentHit2, segmentHit1), context);

        AllHits expected = new AllHits();
        expected.setAnnotationId("123");
        expected.setMaxTermHitConfidence(.6f);
        AllHit hit1 = new AllHit();
        hit1.setConfidence(.3f);
        hit1.getOneBestContext().add(getTerm("term1", .6f, 4f, 5f));
        hit1.getTermHits().add(getTermHit("hit1", .3f, 4f, 5f));
        AllHit hit2 = new AllHit();
        hit2.setConfidence(.6f);
        hit2.getOneBestContext().add(getTerm("hit2", .6f, 3f, 4f));
        hit2.getTermHits().add(getTermHit("hit2", .6f, 3f, 4f));
        expected.getKeywordResultList().add(hit2);
        expected.getKeywordResultList().add(hit1);

        assertEquals(objectMapper.writeValueAsString(expected), objectMapper.writeValueAsString(result));
    }

    @Test
    public void hitWithContextTest() throws AllHitsException, JsonProcessingException {
        SegmentBoundary context1 = SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(1).setEnd(2).build();
        SegmentBoundary context2 = SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(2).setEnd(3).build();
        SegmentBoundary hitBoundary = SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(3).setEnd(4).build();
        SegmentBoundary context3 = SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(4).setEnd(5).build();
        SegmentBoundary context4 = SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(5).setEnd(6).build();

        AnnotationHitsTransformer.SegmentHit hit = new AnnotationHitsTransformer.SegmentHit(context1, hitBoundary, 0);
        hit.setContextEnd(context4);

        TreeMap<SegmentBoundary,List<SegmentValue>> context = new TreeMap<>(new BoundaryComparator());
        context.put(hitBoundary, List.of(SegmentValue.newBuilder().setValue("hit").setScore(.3f).build(),
                        SegmentValue.newBuilder().setValue("max").setScore(.7f).build()));
        context.put(context1, List.of(SegmentValue.newBuilder().setValue("a1").setScore(.1f).build(),
                        SegmentValue.newBuilder().setValue("a2").setScore(.5f).build()));
        context.put(context2, List.of(SegmentValue.newBuilder().setValue("b1").setScore(.2f).build(),
                        SegmentValue.newBuilder().setValue("b2").setScore(.6f).build()));
        context.put(context3, List.of(SegmentValue.newBuilder().setValue("c1").setScore(.1f).build(),
                        SegmentValue.newBuilder().setValue("c2").setScore(.8f).build()));
        context.put(context4, List.of(SegmentValue.newBuilder().setValue("d1").setScore(.2f).build()));

        AllHits result = factory.create("123", List.of(hit), context);

        AllHits expected = new AllHits();
        expected.setAnnotationId("123");
        expected.setMaxTermHitConfidence(.3f);
        AllHit hit1 = new AllHit();
        hit1.setConfidence(.3f);
        hit1.getOneBestContext().add(getTerm("a2", .5f, 1f, 2f));
        hit1.getOneBestContext().add(getTerm("b2", .6f, 2f, 3f));
        hit1.getOneBestContext().add(getTerm("max", .7f, 3f, 4f));
        hit1.getOneBestContext().add(getTerm("c2", .8f, 4f, 5f));
        hit1.getOneBestContext().add(getTerm("d1", .2f, 5f, 6f));

        hit1.getTermHits().add(getTermHit("hit", .3f, 3f, 4f));
        expected.getKeywordResultList().add(hit1);

        assertEquals(objectMapper.writeValueAsString(expected), objectMapper.writeValueAsString(result));
    }

    private Term getTerm(String label, float confidence, float start, float end) {
        Term term = new Term();
        term.setLabel(label);
        term.setConfidence(confidence);
        term.getTimeRange().setStartTime(start);
        term.getTimeRange().setEndTime(end);
        return term;
    }

    private TermHit getTermHit(String label, float confidence, float start, float end) {
        TermHit termHit = new TermHit();
        termHit.setConfidence(confidence);
        termHit.setTermLabel(label);
        termHit.getTimeRange().setStartTime(start);
        termHit.getTimeRange().setEndTime(end);

        return termHit;
    }
}
