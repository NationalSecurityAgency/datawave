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
import datawave.query.transformer.annotation.AnnotationHitsTransformer.SegmentHit;
import datawave.query.transformer.annotation.model.AllHit;
import datawave.query.transformer.annotation.model.AllHits;
import datawave.query.transformer.annotation.model.Term;
import datawave.query.transformer.annotation.model.TermHit;

public class AllHitsFactoryTest {
    private static final SegmentBoundary BOUNDARY1 = SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(1).setEnd(2).build();
    private static final SegmentBoundary BOUNDARY2 = SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(2).setEnd(3).build();
    private static final SegmentBoundary BOUNDARY3 = SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(3).setEnd(4).build();
    private static final SegmentBoundary BOUNDARY4 = SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(4).setEnd(5).build();
    private static final SegmentBoundary BOUNDARY5 = SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(5).setEnd(6).build();

    private AllHitsFactory factory;
    private ObjectMapper objectMapper;
    private TreeMap<SegmentBoundary,List<SegmentValue>> context;

    @BeforeEach
    public void setup() {
        factory = new AllHitsFactory();
        objectMapper = new ObjectMapper();
        context = new TreeMap<>(new BoundaryComparator());
    }

    @Test
    public void noHitsTest() throws AllHitsException {
        AllHits result = factory.create("123", List.of(), new TreeMap<>(new BoundaryComparator()));
        assertNull(result);
    }

    @Test
    public void hitWithNoContextTest() throws AllHitsException {
        SegmentHit hit = createSegmentHit(BOUNDARY1, BOUNDARY1, 0, BOUNDARY1);
        AllHits result = factory.create("123", List.of(hit), new TreeMap<>(new BoundaryComparator()));
        assertNull(result);
    }

    @Test
    public void contextWithNoValuesOnHitTest() {
        context.put(BOUNDARY1, List.of());
        SegmentHit hit = createSegmentHit(BOUNDARY1, BOUNDARY1, 0, BOUNDARY1);
        assertThrows(AllHitsException.class, () -> factory.create("123", List.of(hit), context));
    }

    @Test
    public void contextWithNoValuesOnContextTest() {
        context.put(BOUNDARY2, List.of());
        context.put(BOUNDARY3, List.of(SegmentValue.newBuilder().setValue("hit").setScore(.3f).build()));
        SegmentHit hit = createSegmentHit(BOUNDARY2, BOUNDARY3, 0, BOUNDARY3);
        assertThrows(AllHitsException.class, () -> factory.create("123", List.of(hit), context));
    }

    @Test
    public void hitWithInvalidHitIndexTest() {
        context.put(BOUNDARY3, List.of(SegmentValue.newBuilder().setValue("hit").setScore(.3f).build()));
        SegmentHit hit = createSegmentHit(BOUNDARY3, BOUNDARY3, 1, BOUNDARY3);
        assertThrows(AllHitsException.class, () -> factory.create("123", List.of(hit), context));
    }

    @Test
    public void hitWithNoEndContextTest() {
        context.put(BOUNDARY3, List.of(SegmentValue.newBuilder().setValue("hit").setScore(.3f).build()));
        SegmentHit hit = new SegmentHit(BOUNDARY3, BOUNDARY3, 0);
        assertThrows(AllHitsException.class, () -> factory.create("123", List.of(hit), context));
    }

    @Test
    public void segmentWithNoValuesTest() {
        context.put(BOUNDARY3, List.of());
        SegmentHit hit = createSegmentHit(BOUNDARY3, BOUNDARY3, 0, BOUNDARY3);
        assertThrows(AllHitsException.class, () -> factory.create("123", List.of(hit), context));
    }

    @Test
    public void hitWithOutOfBoundsIndexTest() {
        context.put(BOUNDARY3, List.of(SegmentValue.newBuilder().setValue("hit").setScore(.3f).build()));
        SegmentHit hit = createSegmentHit(BOUNDARY3, BOUNDARY3, 1, BOUNDARY3);
        assertThrows(AllHitsException.class, () -> factory.create("123", List.of(hit), context));
    }

    @Test
    public void singleHitOnSingleValueTest() throws AllHitsException, JsonProcessingException {
        context.put(BOUNDARY3, List.of(SegmentValue.newBuilder().setValue("hit").setScore(.3f).build()));
        SegmentHit hit = createSegmentHit(BOUNDARY3, BOUNDARY3, 0, BOUNDARY3);
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
        context.put(BOUNDARY3, List.of(SegmentValue.newBuilder().setValue("hit1").setScore(.3f).build(),
                        SegmentValue.newBuilder().setValue("hit3").setScore(.4f).build(), SegmentValue.newBuilder().setValue("hit2").setScore(.6f).build()));

        SegmentHit segmentHit1 = createSegmentHit(BOUNDARY3, BOUNDARY3, 0, BOUNDARY3);
        SegmentHit segmentHit2 = createSegmentHit(BOUNDARY3, BOUNDARY3, 2, BOUNDARY3);
        SegmentHit segmentHit3 = createSegmentHit(BOUNDARY3, BOUNDARY3, 1, BOUNDARY3);

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
        context.put(BOUNDARY3, List.of(SegmentValue.newBuilder().setValue("hit1").setScore(.3f).build(),
                        SegmentValue.newBuilder().setValue("term1").setScore(.6f).build()));
        context.put(BOUNDARY4, List.of(SegmentValue.newBuilder().setValue("term2").setScore(.3f).build(),
                        SegmentValue.newBuilder().setValue("hit2").setScore(.6f).build()));

        SegmentHit segmentHit1 = createSegmentHit(BOUNDARY3, BOUNDARY3, 0, BOUNDARY3);
        SegmentHit segmentHit2 = createSegmentHit(BOUNDARY4, BOUNDARY4, 1, BOUNDARY4);

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
        context.put(BOUNDARY4, List.of(SegmentValue.newBuilder().setValue("hit1").setScore(.3f).build(),
                        SegmentValue.newBuilder().setValue("term1").setScore(.6f).build()));
        context.put(BOUNDARY3, List.of(SegmentValue.newBuilder().setValue("term2").setScore(.3f).build(),
                        SegmentValue.newBuilder().setValue("hit2").setScore(.6f).build()));

        SegmentHit segmentHit1 = createSegmentHit(BOUNDARY4, BOUNDARY4, 0, BOUNDARY4);
        SegmentHit segmentHit2 = createSegmentHit(BOUNDARY3, BOUNDARY3, 1, BOUNDARY3);

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
        withFullContext();

        SegmentHit hit = createSegmentHit(BOUNDARY1, BOUNDARY3, 0, BOUNDARY5);

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

    @Test
    public void overlappingHitContextTest() throws AllHitsException, JsonProcessingException {
        withFullContext();

        // 1-5 (hit3)
        SegmentHit hit1 = createSegmentHit(BOUNDARY1, BOUNDARY3, 0, BOUNDARY5);
        // 2-4 (hit4)
        SegmentHit hit2 = createSegmentHit(BOUNDARY2, BOUNDARY4, 1, BOUNDARY4);

        AllHits result = factory.create("123", List.of(hit1, hit2), context);

        AllHits expected = new AllHits();
        expected.setAnnotationId("123");
        expected.setMaxTermHitConfidence(.8f);
        AllHit allHit1 = new AllHit();
        allHit1.setConfidence(.8f);
        allHit1.getOneBestContext().add(getTerm("a2", .5f, 1f, 2f));
        allHit1.getOneBestContext().add(getTerm("b2", .6f, 2f, 3f));
        allHit1.getOneBestContext().add(getTerm("max", .7f, 3f, 4f));
        allHit1.getOneBestContext().add(getTerm("c2", .8f, 4f, 5f));
        allHit1.getOneBestContext().add(getTerm("d1", .2f, 5f, 6f));

        allHit1.getTermHits().add(getTermHit("hit", .3f, 3f, 4f));
        allHit1.getTermHits().add(getTermHit("c2", .8f, 4f, 5f));
        expected.getKeywordResultList().add(allHit1);

        assertEquals(objectMapper.writeValueAsString(expected), objectMapper.writeValueAsString(result));
    }

    @Test
    public void overlappingBoundaryHitContextTest() throws AllHitsException, JsonProcessingException {
        withFullContext();

        // 1-4 (hit3)
        SegmentHit hit1 = createSegmentHit(BOUNDARY1, BOUNDARY3, 0, BOUNDARY4);
        // 4-5 (hit4)
        SegmentHit hit2 = createSegmentHit(BOUNDARY4, BOUNDARY4, 1, BOUNDARY5);

        AllHits result = factory.create("123", List.of(hit1, hit2), context);

        AllHits expected = new AllHits();
        expected.setAnnotationId("123");
        expected.setMaxTermHitConfidence(.8f);
        AllHit allHit1 = new AllHit();
        allHit1.setConfidence(.8f);
        allHit1.getOneBestContext().add(getTerm("a2", .5f, 1f, 2f));
        allHit1.getOneBestContext().add(getTerm("b2", .6f, 2f, 3f));
        allHit1.getOneBestContext().add(getTerm("max", .7f, 3f, 4f));
        allHit1.getOneBestContext().add(getTerm("c2", .8f, 4f, 5f));
        allHit1.getOneBestContext().add(getTerm("d1", .2f, 5f, 6f));

        allHit1.getTermHits().add(getTermHit("hit", .3f, 3f, 4f));
        allHit1.getTermHits().add(getTermHit("c2", .8f, 4f, 5f));
        expected.getKeywordResultList().add(allHit1);

        assertEquals(objectMapper.writeValueAsString(expected), objectMapper.writeValueAsString(result));
    }

    @Test
    public void overlappingContextButNotHitTest() throws AllHitsException, JsonProcessingException {
        withFullContext();

        // 1-4 (hit3)
        SegmentHit hit1 = createSegmentHit(BOUNDARY1, BOUNDARY3, 0, BOUNDARY4);
        // 4-5 (hit5)
        SegmentHit hit2 = createSegmentHit(BOUNDARY4, BOUNDARY5, 0, BOUNDARY5);

        AllHits result = factory.create("123", List.of(hit1, hit2), context);

        AllHits expected = new AllHits();
        expected.setAnnotationId("123");
        expected.setMaxTermHitConfidence(.3f);

        AllHit allHit1 = new AllHit();
        allHit1.setConfidence(.3f);
        allHit1.getOneBestContext().add(getTerm("a2", .5f, 1f, 2f));
        allHit1.getOneBestContext().add(getTerm("b2", .6f, 2f, 3f));
        allHit1.getOneBestContext().add(getTerm("max", .7f, 3f, 4f));
        allHit1.getOneBestContext().add(getTerm("c2", .8f, 4f, 5f));

        allHit1.getTermHits().add(getTermHit("hit", .3f, 3f, 4f));

        expected.getKeywordResultList().add(allHit1);

        AllHit allHit2 = new AllHit();
        allHit2.setConfidence(.2f);
        allHit2.getOneBestContext().add(getTerm("c2", .8f, 4f, 5f));
        allHit2.getOneBestContext().add(getTerm("d1", .2f, 5f, 6f));

        allHit2.getTermHits().add(getTermHit("d1", .2f, 5f, 6f));
        expected.getKeywordResultList().add(allHit2);

        assertEquals(objectMapper.writeValueAsString(expected), objectMapper.writeValueAsString(result));
    }

    // in practice this shouldn't happen, but verify it anyway. Since context is a fixed width this is a completely contrived example
    @Test
    public void overlappingContextAddedAtMostOnceTest() throws AllHitsException, JsonProcessingException {
        withFullContext();

        // 1-4 (hit1)
        SegmentHit hit1 = createSegmentHit(BOUNDARY1, BOUNDARY1, 0, BOUNDARY4);
        // 4-5 (hit4)
        SegmentHit hit2 = createSegmentHit(BOUNDARY4, BOUNDARY5, 0, BOUNDARY5);
        // 3-3 (hit3)
        SegmentHit hit3 = createSegmentHit(BOUNDARY3, BOUNDARY3, 0, BOUNDARY3);

        // even this is contrived, because hit3 should sort in front of hit2 since it ends before hit2
        AllHits result = factory.create("123", List.of(hit1, hit2, hit3), context);

        AllHits expected = new AllHits();
        expected.setAnnotationId("123");
        expected.setMaxTermHitConfidence(.3f);

        AllHit allHit1 = new AllHit();
        allHit1.setConfidence(.3f);
        allHit1.getOneBestContext().add(getTerm("a2", .5f, 1f, 2f));
        allHit1.getOneBestContext().add(getTerm("b2", .6f, 2f, 3f));
        allHit1.getOneBestContext().add(getTerm("max", .7f, 3f, 4f));
        allHit1.getOneBestContext().add(getTerm("c2", .8f, 4f, 5f));

        allHit1.getTermHits().add(getTermHit("a1", .1f, 1f, 2f));
        allHit1.getTermHits().add(getTermHit("hit", .3f, 3f, 4f));

        expected.getKeywordResultList().add(allHit1);

        AllHit allHit2 = new AllHit();
        allHit2.setConfidence(.2f);
        allHit2.getOneBestContext().add(getTerm("c2", .8f, 4f, 5f));
        allHit2.getOneBestContext().add(getTerm("d1", .2f, 5f, 6f));

        allHit2.getTermHits().add(getTermHit("d1", .2f, 5f, 6f));
        expected.getKeywordResultList().add(allHit2);

        assertEquals(objectMapper.writeValueAsString(expected), objectMapper.writeValueAsString(result));
    }

    private SegmentHit createSegmentHit(SegmentBoundary start, SegmentBoundary hit, int hitIndex, SegmentBoundary end) {
        SegmentHit segmentHit = new SegmentHit(start, hit, hitIndex);
        segmentHit.setContextEnd(end);

        return segmentHit;
    }

    private void withFullContext() {
        context.put(BOUNDARY1, List.of(SegmentValue.newBuilder().setValue("a1").setScore(.1f).build(),
                        SegmentValue.newBuilder().setValue("a2").setScore(.5f).build()));
        context.put(BOUNDARY2, List.of(SegmentValue.newBuilder().setValue("b1").setScore(.2f).build(),
                        SegmentValue.newBuilder().setValue("b2").setScore(.6f).build()));
        context.put(BOUNDARY3, List.of(SegmentValue.newBuilder().setValue("hit").setScore(.3f).build(),
                        SegmentValue.newBuilder().setValue("max").setScore(.7f).build()));
        context.put(BOUNDARY4, List.of(SegmentValue.newBuilder().setValue("c1").setScore(.1f).build(),
                        SegmentValue.newBuilder().setValue("c2").setScore(.8f).build()));
        context.put(BOUNDARY5, List.of(SegmentValue.newBuilder().setValue("d1").setScore(.2f).build()));
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
