package datawave.query.transformer.annotation;

import static datawave.query.QueryParameters.INCLUDE_GROUPING_CONTEXT;
import static datawave.query.QueryParameters.RETURN_FIELDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.YieldCallback;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import datawave.annotation.data.v1.AnnotationDataAccess;
import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.annotation.protobuf.v1.BoundaryType;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.protobuf.v1.SegmentBoundary;
import datawave.annotation.protobuf.v1.SegmentValue;
import datawave.data.normalizer.Normalizer;
import datawave.marking.AccessExpressionMarkings;
import datawave.marking.MarkingFunctions;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.QueryParameters;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.iterator.profile.FinalDocumentTrackingIterator;
import datawave.query.iterator.profile.QuerySpan;
import datawave.query.iterator.profile.QuerySpanCollector;
import datawave.query.parser.JavaRegexAnalyzer;
import datawave.query.transformer.annotation.model.AllHits;

@ExtendWith(MockitoExtension.class)
public class AnnotationHitsTransformerTest {

    // @formatter:off
    private static final Segment S1 = Segment.newBuilder()
            .addValues(SegmentValue.newBuilder().setValue("aaaAAAA").setScore(1f).build())
            .addValues(SegmentValue.newBuilder().setValue("bbbBBBB").setScore(.5f).build())
            .setBoundary(SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(0).setEnd(1).build())
            .build();
    private static final Segment S2 = Segment.newBuilder()
            .addValues(SegmentValue.newBuilder().setValue("cccCCCC").setScore(1f).build())
            .addValues(SegmentValue.newBuilder().setValue("dddDDDD").setScore(.5f).build())
            .setBoundary(SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(2).setEnd(5).build())
            .build();
    private static final Segment S3 = Segment.newBuilder()
            .addValues(SegmentValue.newBuilder().setValue("eeeEEEE").setScore(1f).build())
            .addValues(SegmentValue.newBuilder().setValue("fffFFFF").setScore(.5f).build())
            .setBoundary(SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(10).setEnd(15).build())
            .build();
    private static final Segment S4 = Segment.newBuilder()
            .addValues(SegmentValue.newBuilder().setValue("gggGGGG").setScore(1f).build())
            .addValues(SegmentValue.newBuilder().setValue("hhhHHHH").setScore(.5f).build())
            .setBoundary(SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(20).setEnd(25).build())
            .build();
    private static final Segment S5 = Segment.newBuilder()
            .addValues(SegmentValue.newBuilder().setValue("iiiIIII").setScore(1f).build())
            .addValues(SegmentValue.newBuilder().setValue("jjjJJJJ").setScore(.5f).build())
            .setBoundary(SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(30).setEnd(31).build())
            .build();
    private static final Segment S6 = Segment.newBuilder()
            .addValues(SegmentValue.newBuilder().setValue("kkkKKKK").setScore(1f).build())
            .addValues(SegmentValue.newBuilder().setValue("lllLLLL").setScore(.5f).build())
            .setBoundary(SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(32).setEnd(34).build())
            .build();
    private static final Segment S7 = Segment.newBuilder()
            .addValues(SegmentValue.newBuilder().setValue("mmmMMMM").setScore(1f).build())
            .addValues(SegmentValue.newBuilder().setValue("nnnNNNN").setScore(.5f).build())
            .setBoundary(SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(35).setEnd(50).build())
            .build();
    // @formatter:on

    public static final Key HIT_KEY = new Key("20260112_0", "test\u0000123.345.456");

    private AnnotationHitsTransformer transformer;

    @Mock
    private TermExtractor termExtractor;

    @Mock
    private Normalizer<String> normalizer;

    @Mock
    private AnnotationDataAccess annotationDao;

    @Mock
    private AllHitsFactory allHitsFactory;

    private String query;
    private int maxContextBoundary;
    private Set<String> validTypes;
    private String targetField;
    private Map<String,String> enrichmentFieldMap;
    private ShardQueryConfiguration shardQueryConfiguration;

    private Query settings;
    private MarkingFunctions<AccessExpressionMarkings> markingFunctions;

    private List<Annotation> annotations;
    private AnnotationSource annotationSource;
    private Optional<AnnotationSource> optionalSource;

    private ObjectMapper objectMapper = new ObjectMapper();
    private AllHits allHitsResult;

    @BeforeEach
    public void setup() {
        settings = new QueryImpl();
        markingFunctions = new MarkingFunctions.Default();
        annotations = new ArrayList<>();
        optionalSource = Optional.empty();
        enrichmentFieldMap = new HashMap<>();
        shardQueryConfiguration = new ShardQueryConfiguration();
        shardQueryConfiguration.setQuery(settings);

        allHitsResult = new AllHits();
        allHitsResult.setAnnotationId("my-annotation");
    }

    @Test
    public void disabledTest() {
        // default
        test(null, null);
        test(Map.entry(new Key(), new Document()), Map.entry(new Key(), new Document()));
        test(Map.entry(new Key("123"), new Document()), Map.entry(new Key("123"), new Document()));

        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "false");
        test(null, null);
        test(Map.entry(new Key(), new Document()), Map.entry(new Key(), new Document()));
        test(Map.entry(new Key("123"), new Document()), Map.entry(new Key("123"), new Document()));

        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "not-true-or-false");
        test(null, null);
        test(Map.entry(new Key(), new Document()), Map.entry(new Key(), new Document()));
        test(Map.entry(new Key("123"), new Document()), Map.entry(new Key("123"), new Document()));
    }

    @Test
    public void nullQueryTermExtractorTest() {
        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");
        termExtractor = null;

        assertThrows(IllegalStateException.class, () -> test(null, null));
    }

    @Test
    public void nullTermNormalizerExtractorTest() {
        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");
        normalizer = null;

        assertThrows(IllegalStateException.class, () -> test(null, null));
    }

    @Test
    public void emptySearchTermTest() {
        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");

        // no search terms, return self
        test(Map.entry(new Key("123"), new Document()), Map.entry(new Key("123"), new Document()));
    }

    @Test
    public void errorParseExceptionSearchTermsTest() throws ParseException, JavaRegexAnalyzer.JavaRegexParseException {
        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");
        query = "abc";
        when(termExtractor.extract(query, normalizer)).thenThrow(new ParseException("testing"));

        // no search terms, return self
        test(Map.entry(new Key("123"), new Document()), Map.entry(new Key("123"), new Document()));
    }

    @Test
    public void errorJavaRegexAnalyzerSearchTermsTest() throws ParseException, JavaRegexAnalyzer.JavaRegexParseException {
        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");
        query = "abc";
        when(termExtractor.extract(query, normalizer)).thenThrow(new JavaRegexAnalyzer.JavaRegexParseException("testing", 0));

        // no search terms, return self
        test(Map.entry(new Key("123"), new Document()), Map.entry(new Key("123"), new Document()));
    }

    @Test
    public void inputValidationTest() throws ParseException, JavaRegexAnalyzer.JavaRegexParseException {
        Set<String> queryTerms = Set.of("v1", "v2", "v3");
        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");
        query = "abc";
        when(termExtractor.extract(query, normalizer)).thenReturn(queryTerms);

        // null key
        test(new AbstractMap.SimpleEntry<>(null, new Document()), new AbstractMap.SimpleEntry<>(null, new Document()));
        // null document
        test(new AbstractMap.SimpleEntry<>(new Key(), null), new AbstractMap.SimpleEntry<>(new Key(), null));

        Document unkept = new Document();
        unkept.setToKeep(false);
        // not keeping document
        test(Map.entry(new Key(), unkept), Map.entry(new Key(), unkept));
    }

    @Test
    public void targetFieldAlreadySetTest() throws ParseException, JavaRegexAnalyzer.JavaRegexParseException {
        Set<String> queryTerms = Set.of("v1", "v2", "v3");
        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");
        query = "abc";
        targetField = "TARGET_FIELD";

        when(termExtractor.extract(query, normalizer)).thenReturn(queryTerms);

        Document doc = new Document();
        doc.put("TARGET_FIELD", new Content("abc", new Key(), true));
        test(Map.entry(new Key(), doc), Map.entry(new Key(), doc));
    }

    @Test
    public void invalidKeyStructureTest() throws ParseException, JavaRegexAnalyzer.JavaRegexParseException {
        Set<String> queryTerms = Set.of("v1", "v2", "v3");
        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");
        query = "abc";
        targetField = "TARGET_FIELD";

        when(termExtractor.extract(query, normalizer)).thenReturn(queryTerms);

        // empty
        test(Map.entry(new Key(), new Document()), Map.entry(new Key(), new Document()));
        // one part
        test(Map.entry(new Key("", "abc"), new Document()), Map.entry(new Key("", "abc"), new Document()));
        // three parts
        test(Map.entry(new Key("", "abc\u0000def\u0000ghi"), new Document()), Map.entry(new Key("", "abc\u0000def\u0000ghi"), new Document()));
    }

    @Test
    public void noAnnotationTest() throws ParseException, JavaRegexAnalyzer.JavaRegexParseException {
        Set<String> queryTerms = Set.of("v1", "v2", "v3");
        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");
        query = "abc";
        targetField = "TARGET_FIELD";

        when(termExtractor.extract(query, normalizer)).thenReturn(queryTerms);
        when(annotationDao.getAnnotations("20260112_0", "test", "123.345.456")).thenReturn(annotations);

        test(Map.entry(HIT_KEY, new Document()), Map.entry(HIT_KEY, new Document()));
    }

    @Test
    public void annotationTypeMissTest() throws ParseException, JavaRegexAnalyzer.JavaRegexParseException {
        Set<String> queryTerms = Set.of("v1", "v2", "v3");
        Segment s = Segment.newBuilder().addValues(SegmentValue.newBuilder().setValue("v1").setScore(.95f).build())
                        .setBoundary(SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(0).setEnd(25).build()).build();

        // ANNO3 not a valid type
        givenAnnotation(buildAnnotation("ANNO3", "20260112_0", "test", "123.345.456", "hash", s));

        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");
        query = "abc";
        targetField = "TARGET_FIELD";
        validTypes = Set.of("ANNO1", "ANNO2");

        when(termExtractor.extract(query, normalizer)).thenReturn(queryTerms);
        when(annotationDao.getAnnotations("20260112_0", "test", "123.345.456")).thenReturn(annotations);

        test(Map.entry(HIT_KEY, new Document()), Map.entry(HIT_KEY, new Document()));
    }

    @Test
    public void termMissTest() throws ParseException, JavaRegexAnalyzer.JavaRegexParseException, AllHitsException, JsonProcessingException {
        Set<String> queryTerms = Set.of("v1", "v2", "v3");

        givenAnnotation(buildAnnotation("ANNO1", "20260112_0", "test", "123.345.456", "hash", S7, S1, S6, S2, S5, S3, S4));

        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");
        query = "abc";
        targetField = "TARGET_FIELD";
        validTypes = Set.of("ANNO1", "ANNO2");

        when(termExtractor.extract(query, normalizer)).thenReturn(queryTerms);
        when(annotationDao.getAnnotations("20260112_0", "test", "123.345.456")).thenReturn(annotations);
        withNormalizers();

        Document expected = new Document();
        expected.put("TARGET_FIELD", new Content(allHitsToString(), HIT_KEY, true));

        test(Map.entry(HIT_KEY, new Document()), Map.entry(HIT_KEY, expected));
    }

    @Test
    public void singleHitTest() throws ParseException, JavaRegexAnalyzer.JavaRegexParseException, AllHitsException, JsonProcessingException {
        Set<String> queryTerms = Set.of("aaaaaaa", "v2", "v3");

        givenAnnotation(buildAnnotation("ANNO1", "20260112_0", "test", "123.345.456", "hash", S7, S1, S6, S2, S5, S3, S4));

        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");
        query = "abc";
        targetField = "TARGET_FIELD";
        validTypes = Set.of("ANNO1", "ANNO2");

        when(termExtractor.extract(query, normalizer)).thenReturn(queryTerms);
        when(annotationDao.getAnnotations("20260112_0", "test", "123.345.456")).thenReturn(annotations);
        withNormalizers();

        AnnotationHitsTransformer.SegmentHit hit = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 1);
        hit.setContextEnd(S1.getBoundary());
        withHits("my-annotation", List.of(hit));

        Document expected = new Document();
        expected.put("TARGET_FIELD", new Content(allHitsToString(allHitsResult), HIT_KEY, true));

        test(Map.entry(HIT_KEY, new Document()), Map.entry(HIT_KEY, expected));
    }

    @Test
    public void regexHitTest() throws ParseException, JavaRegexAnalyzer.JavaRegexParseException, AllHitsException, JsonProcessingException {
        Set<String> queryTerms = Set.of("a.*", "v2", "v3");

        givenAnnotation(buildAnnotation("ANNO1", "20260112_0", "test", "123.345.456", "hash", S7, S1, S6, S2, S5, S3, S4));

        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");
        query = "abc";
        targetField = "TARGET_FIELD";
        validTypes = Set.of("ANNO1", "ANNO2");

        when(termExtractor.extract(query, normalizer)).thenReturn(queryTerms);
        when(annotationDao.getAnnotations("20260112_0", "test", "123.345.456")).thenReturn(annotations);
        withNormalizers();

        AnnotationHitsTransformer.SegmentHit hit = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 1);
        hit.setContextEnd(S1.getBoundary());
        withHits("my-annotation", List.of(hit));

        Document expected = new Document();
        expected.put("TARGET_FIELD", new Content(allHitsToString(allHitsResult), HIT_KEY, true));

        test(Map.entry(HIT_KEY, new Document()), Map.entry(HIT_KEY, expected));
    }

    @Test
    public void multiHitTest() throws ParseException, JavaRegexAnalyzer.JavaRegexParseException, AllHitsException, JsonProcessingException {
        Set<String> queryTerms = Set.of("aaaaaaa", "bbbbbbb", "v3");

        givenAnnotation(buildAnnotation("ANNO1", "20260112_0", "test", "123.345.456", "hash", S7, S1, S6, S2, S5, S3, S4));

        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");
        query = "abc";
        targetField = "TARGET_FIELD";
        validTypes = Set.of("ANNO1", "ANNO2");

        when(termExtractor.extract(query, normalizer)).thenReturn(queryTerms);
        when(annotationDao.getAnnotations("20260112_0", "test", "123.345.456")).thenReturn(annotations);
        withNormalizers();

        AnnotationHitsTransformer.SegmentHit hit1 = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 0);
        hit1.setContextEnd(S1.getBoundary());
        AnnotationHitsTransformer.SegmentHit hit2 = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 1);
        hit2.setContextEnd(S1.getBoundary());
        withHits("my-annotation", List.of(hit1, hit2));

        Document expected = new Document();
        expected.put("TARGET_FIELD", new Content(allHitsToString(allHitsResult), HIT_KEY, true));

        test(Map.entry(HIT_KEY, new Document()), Map.entry(HIT_KEY, expected));
    }

    @Test
    public void multiHitRegexTest() throws ParseException, JavaRegexAnalyzer.JavaRegexParseException, AllHitsException, JsonProcessingException {
        Set<String> queryTerms = Set.of("aaa.*aa", "bbb.*bb", "v3");

        givenAnnotation(buildAnnotation("ANNO1", "20260112_0", "test", "123.345.456", "hash", S7, S1, S6, S2, S5, S3, S4));

        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");
        query = "abc";
        targetField = "TARGET_FIELD";
        validTypes = Set.of("ANNO1", "ANNO2");

        when(termExtractor.extract(query, normalizer)).thenReturn(queryTerms);
        when(annotationDao.getAnnotations("20260112_0", "test", "123.345.456")).thenReturn(annotations);
        withNormalizers();

        AnnotationHitsTransformer.SegmentHit hit1 = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 0);
        hit1.setContextEnd(S1.getBoundary());
        AnnotationHitsTransformer.SegmentHit hit2 = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 1);
        hit2.setContextEnd(S1.getBoundary());
        withHits("my-annotation", List.of(hit1, hit2));

        Document expected = new Document();
        expected.put("TARGET_FIELD", new Content(allHitsToString(allHitsResult), HIT_KEY, true));

        test(Map.entry(HIT_KEY, new Document()), Map.entry(HIT_KEY, expected));
    }

    @Test
    public void multiAnnotationHitTest() throws ParseException, JavaRegexAnalyzer.JavaRegexParseException, AllHitsException, JsonProcessingException {
        Set<String> queryTerms = Set.of("aaaaaaa", "bbbbbbb", "v3");

        givenAnnotation(buildAnnotation("ANNO1", "20260112_0", "test", "123.345.456", "hash", S7, S1, S6, S2, S5, S3, S4));
        givenAnnotation(buildAnnotation("ANNO1", "20260112_0", "test", "123.345.456", "hash", S7, S1, S6, S2, S5, S3, S4));

        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");
        query = "abc";
        targetField = "TARGET_FIELD";
        validTypes = Set.of("ANNO1", "ANNO2");

        when(termExtractor.extract(query, normalizer)).thenReturn(queryTerms);
        when(annotationDao.getAnnotations("20260112_0", "test", "123.345.456")).thenReturn(annotations);
        withNormalizers();

        AnnotationHitsTransformer.SegmentHit hit1 = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 0);
        hit1.setContextEnd(S1.getBoundary());
        AnnotationHitsTransformer.SegmentHit hit2 = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 1);
        hit2.setContextEnd(S1.getBoundary());
        withHits("my-annotation", List.of(hit1, hit2));

        AllHits allHits1 = new AllHits();
        allHits1.setAnnotationId("my-annotation");
        AllHits allHits2 = new AllHits();
        allHits2.setAnnotationId("my-annotation");

        Document expected = new Document();
        expected.put("TARGET_FIELD", new Content(allHitsToString(allHits1, allHits2), HIT_KEY, true));

        test(Map.entry(HIT_KEY, new Document()), Map.entry(HIT_KEY, expected));
    }

    @Test
    public void allHitsFactoryErrorTest() throws ParseException, JavaRegexAnalyzer.JavaRegexParseException, AllHitsException, JsonProcessingException {
        Set<String> queryTerms = Set.of("aaaaaaa", "bbbbbbb", "v3");

        givenAnnotation(buildAnnotation("id1", "ANNO1", "20260112_0", "test", "123.345.456", "hash", S7, S1, S6, S2, S5, S3, S4));
        givenAnnotation(buildAnnotation("id2", "ANNO1", "20260112_0", "test", "123.345.456", "hash", S7, S1, S6, S2, S5, S3, S4));

        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");
        query = "abc";
        targetField = "TARGET_FIELD";
        validTypes = Set.of("ANNO1", "ANNO2");

        when(termExtractor.extract(query, normalizer)).thenReturn(queryTerms);
        when(annotationDao.getAnnotations("20260112_0", "test", "123.345.456")).thenReturn(annotations);
        withNormalizers();

        when(allHitsFactory.create(any(), any(), any(), any())).thenThrow(new AllHitsException("testing"));

        AllHits hit1 = new AllHits();
        hit1.setAnnotationId("id1");
        hit1.addDynamicProperties("error", "testing");
        AllHits hit2 = new AllHits();
        hit2.setAnnotationId("id2");
        hit2.addDynamicProperties("error", "testing");

        Document expected = new Document();
        expected.put("TARGET_FIELD", new Content(allHitsToString(hit1, hit2), HIT_KEY, true));

        test(Map.entry(HIT_KEY, new Document()), Map.entry(HIT_KEY, expected));
    }

    @Test
    public void hitUnderMinScoreTest() throws ParseException, JavaRegexAnalyzer.JavaRegexParseException, JsonProcessingException {
        Set<String> queryTerms = Set.of("bbbbbbb", "v2", "v3");

        givenAnnotation(buildAnnotation("ANNO1", "20260112_0", "test", "123.345.456", "hash", S7, S1, S6, S2, S5, S3, S4));

        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");
        withParameter(AnnotationHitsTransformer.MIN_SCORE_PARAMETER, ".9");

        query = "abc";
        targetField = "TARGET_FIELD";
        validTypes = Set.of("ANNO1", "ANNO2");

        when(termExtractor.extract(query, normalizer)).thenReturn(queryTerms);
        when(annotationDao.getAnnotations("20260112_0", "test", "123.345.456")).thenReturn(annotations);
        withNormalizers();

        Document expected = new Document();
        expected.put("TARGET_FIELD", new Content(allHitsToString(), HIT_KEY, true));

        test(Map.entry(HIT_KEY, new Document()), Map.entry(HIT_KEY, expected));
    }

    @Test
    public void minScoreOverMaxTest() throws ParseException, JavaRegexAnalyzer.JavaRegexParseException, AllHitsException, JsonProcessingException {
        Set<String> queryTerms = Set.of("aaaaaaa", "v2", "v3");

        givenAnnotation(buildAnnotation("ANNO1", "20260112_0", "test", "123.345.456", "hash", S7, S1, S6, S2, S5, S3, S4));

        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");
        // aaaaaaa is 1.0
        withParameter(AnnotationHitsTransformer.MIN_SCORE_PARAMETER, "1.5");

        query = "abc";
        targetField = "TARGET_FIELD";
        validTypes = Set.of("ANNO1", "ANNO2");

        when(termExtractor.extract(query, normalizer)).thenReturn(queryTerms);
        when(annotationDao.getAnnotations("20260112_0", "test", "123.345.456")).thenReturn(annotations);
        withNormalizers();

        AnnotationHitsTransformer.SegmentHit hit1 = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 1);
        hit1.setContextEnd(S1.getBoundary());
        withHits("my-annotation", List.of(hit1));

        Document expected = new Document();
        expected.put("TARGET_FIELD", new Content(allHitsToString(allHitsResult), HIT_KEY, true));

        test(Map.entry(HIT_KEY, new Document()), Map.entry(HIT_KEY, expected));
    }

    @Test
    public void minScoreUnderZeroTest() throws ParseException, JavaRegexAnalyzer.JavaRegexParseException, AllHitsException, JsonProcessingException {
        Set<String> queryTerms = Set.of("bbbbbbb", "v2", "v3");

        givenAnnotation(buildAnnotation("ANNO1", "20260112_0", "test", "123.345.456", "hash", S7, S1, S6, S2, S5, S3, S4));

        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");
        // bbbbbbb is .5
        withParameter(AnnotationHitsTransformer.MIN_SCORE_PARAMETER, "-1.5");

        query = "abc";
        targetField = "TARGET_FIELD";
        validTypes = Set.of("ANNO1", "ANNO2");

        when(termExtractor.extract(query, normalizer)).thenReturn(queryTerms);
        when(annotationDao.getAnnotations("20260112_0", "test", "123.345.456")).thenReturn(annotations);
        withNormalizers();

        AnnotationHitsTransformer.SegmentHit hit1 = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 0);
        hit1.setContextEnd(S1.getBoundary());
        withHits("my-annotation", List.of(hit1));

        Document expected = new Document();
        expected.put("TARGET_FIELD", new Content(allHitsToString(allHitsResult), HIT_KEY, true));

        test(Map.entry(HIT_KEY, new Document()), Map.entry(HIT_KEY, expected));
    }

    @Test
    public void contextOneStartTest() throws ParseException, JavaRegexAnalyzer.JavaRegexParseException, AllHitsException, JsonProcessingException {
        Set<String> queryTerms = Set.of("bbbbbbb", "v2", "v3");

        givenAnnotation(buildAnnotation("ANNO1", "20260112_0", "test", "123.345.456", "hash", S7, S1, S6, S2, S5, S3, S4));

        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");

        query = "abc";
        targetField = "TARGET_FIELD";
        validTypes = Set.of("ANNO1", "ANNO2");
        maxContextBoundary = 1;

        when(termExtractor.extract(query, normalizer)).thenReturn(queryTerms);
        when(annotationDao.getAnnotations("20260112_0", "test", "123.345.456")).thenReturn(annotations);
        withNormalizers();

        AnnotationHitsTransformer.SegmentHit hit1 = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 0);
        hit1.setContextEnd(S2.getBoundary());
        withHits("my-annotation", List.of(hit1));

        Document expected = new Document();
        expected.put("TARGET_FIELD", new Content(allHitsToString(allHitsResult), HIT_KEY, true));

        test(Map.entry(HIT_KEY, new Document()), Map.entry(HIT_KEY, expected));
    }

    @Test
    public void contextOneEndTest() throws ParseException, JavaRegexAnalyzer.JavaRegexParseException, AllHitsException, JsonProcessingException {
        Set<String> queryTerms = Set.of("nnnnnnn", "v2", "v3");

        givenAnnotation(buildAnnotation("ANNO1", "20260112_0", "test", "123.345.456", "hash", S7, S1, S6, S2, S5, S3, S4));

        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");

        query = "abc";
        targetField = "TARGET_FIELD";
        validTypes = Set.of("ANNO1", "ANNO2");
        maxContextBoundary = 1;

        when(termExtractor.extract(query, normalizer)).thenReturn(queryTerms);
        when(annotationDao.getAnnotations("20260112_0", "test", "123.345.456")).thenReturn(annotations);
        withNormalizers();
        AnnotationHitsTransformer.SegmentHit hit1 = new AnnotationHitsTransformer.SegmentHit(S6.getBoundary(), S7.getBoundary(), 0);
        hit1.setContextEnd(S7.getBoundary());
        withHits("my-annotation", List.of(hit1));

        Document expected = new Document();
        expected.put("TARGET_FIELD", new Content(allHitsToString(allHitsResult), HIT_KEY, true));

        test(Map.entry(HIT_KEY, new Document()), Map.entry(HIT_KEY, expected));
    }

    @Test
    public void contextOneCenterTest() throws ParseException, JavaRegexAnalyzer.JavaRegexParseException, AllHitsException, JsonProcessingException {
        Set<String> queryTerms = Set.of("ggggggg", "v2", "v3");

        givenAnnotation(buildAnnotation("ANNO1", "20260112_0", "test", "123.345.456", "hash", S7, S1, S6, S2, S5, S3, S4));

        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");

        query = "abc";
        targetField = "TARGET_FIELD";
        validTypes = Set.of("ANNO1", "ANNO2");
        maxContextBoundary = 1;

        when(termExtractor.extract(query, normalizer)).thenReturn(queryTerms);
        when(annotationDao.getAnnotations("20260112_0", "test", "123.345.456")).thenReturn(annotations);
        withNormalizers();

        AnnotationHitsTransformer.SegmentHit hit1 = new AnnotationHitsTransformer.SegmentHit(S3.getBoundary(), S4.getBoundary(), 1);
        hit1.setContextEnd(S5.getBoundary());
        withHits("my-annotation", List.of(hit1));

        Document expected = new Document();
        expected.put("TARGET_FIELD", new Content(allHitsToString(allHitsResult), HIT_KEY, true));

        test(Map.entry(HIT_KEY, new Document()), Map.entry(HIT_KEY, expected));
    }

    @Test
    public void contextTruncatedTest() throws ParseException, JavaRegexAnalyzer.JavaRegexParseException, AllHitsException, JsonProcessingException {
        Set<String> queryTerms = Set.of("lllllll", "v2", "v3");

        givenAnnotation(buildAnnotation("ANNO1", "20260112_0", "test", "123.345.456", "hash", S7, S1, S6, S2, S5, S3, S4));

        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");

        query = "abc";
        targetField = "TARGET_FIELD";
        validTypes = Set.of("ANNO1", "ANNO2");
        maxContextBoundary = 3;

        when(termExtractor.extract(query, normalizer)).thenReturn(queryTerms);
        when(annotationDao.getAnnotations("20260112_0", "test", "123.345.456")).thenReturn(annotations);
        withNormalizers();

        AnnotationHitsTransformer.SegmentHit hit1 = new AnnotationHitsTransformer.SegmentHit(S3.getBoundary(), S6.getBoundary(), 0);
        hit1.setContextEnd(S7.getBoundary());
        withHits("my-annotation", List.of(hit1));

        Document expected = new Document();
        expected.put("TARGET_FIELD", new Content(allHitsToString(allHitsResult), HIT_KEY, true));

        test(Map.entry(HIT_KEY, new Document()), Map.entry(HIT_KEY, expected));
    }

    @Test
    public void contextBiggerThanTotalTest() throws ParseException, JavaRegexAnalyzer.JavaRegexParseException, AllHitsException, JsonProcessingException {
        Set<String> queryTerms = Set.of("nnnnnnn", "v2", "v3");

        givenAnnotation(buildAnnotation("ANNO1", "20260112_0", "test", "123.345.456", "hash", S7, S1, S6, S2, S5, S3, S4));

        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");

        query = "abc";
        targetField = "TARGET_FIELD";
        validTypes = Set.of("ANNO1", "ANNO2");
        maxContextBoundary = 25;

        when(termExtractor.extract(query, normalizer)).thenReturn(queryTerms);
        when(annotationDao.getAnnotations("20260112_0", "test", "123.345.456")).thenReturn(annotations);
        withNormalizers();

        AnnotationHitsTransformer.SegmentHit hit1 = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S7.getBoundary(), 0);
        hit1.setContextEnd(S7.getBoundary());
        withHits("my-annotation", List.of(hit1));

        Document expected = new Document();
        expected.put("TARGET_FIELD", new Content(allHitsToString(allHitsResult), HIT_KEY, true));

        test(Map.entry(HIT_KEY, new Document()), Map.entry(HIT_KEY, expected));
    }

    @Test
    public void underMaxBoundaryTest() throws ParseException, JavaRegexAnalyzer.JavaRegexParseException, AllHitsException, JsonProcessingException {
        Set<String> queryTerms = Set.of("lllllll", "v2", "v3");

        // ANNO3 not a valid type
        givenAnnotation(buildAnnotation("ANNO1", "20260112_0", "test", "123.345.456", "hash", S7, S1, S6, S2, S5, S3, S4));

        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");
        withParameter(AnnotationHitsTransformer.CONTEXT_SIZE_PARAMETER, "0");

        query = "abc";
        targetField = "TARGET_FIELD";
        validTypes = Set.of("ANNO1", "ANNO2");
        maxContextBoundary = 10;

        when(termExtractor.extract(query, normalizer)).thenReturn(queryTerms);
        when(annotationDao.getAnnotations("20260112_0", "test", "123.345.456")).thenReturn(annotations);
        withNormalizers();

        AnnotationHitsTransformer.SegmentHit hit1 = new AnnotationHitsTransformer.SegmentHit(S6.getBoundary(), S6.getBoundary(), 0);
        hit1.setContextEnd(S6.getBoundary());
        withHits("my-annotation", List.of(hit1));

        Document expected = new Document();
        expected.put("TARGET_FIELD", new Content(allHitsToString(allHitsResult), HIT_KEY, true));

        test(Map.entry(HIT_KEY, new Document()), Map.entry(HIT_KEY, expected));
    }

    @Test
    public void negativeContextBoundaryTest() throws ParseException, JavaRegexAnalyzer.JavaRegexParseException, AllHitsException, JsonProcessingException {
        Set<String> queryTerms = Set.of("lllllll", "v2", "v3");

        givenAnnotation(buildAnnotation("ANNO1", "20260112_0", "test", "123.345.456", "hash", S7, S1, S6, S2, S5, S3, S4));

        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");
        withParameter(AnnotationHitsTransformer.CONTEXT_SIZE_PARAMETER, "-5");

        query = "abc";
        targetField = "TARGET_FIELD";
        validTypes = Set.of("ANNO1", "ANNO2");
        maxContextBoundary = 10;

        when(termExtractor.extract(query, normalizer)).thenReturn(queryTerms);
        when(annotationDao.getAnnotations("20260112_0", "test", "123.345.456")).thenReturn(annotations);
        withNormalizers();

        AnnotationHitsTransformer.SegmentHit hit1 = new AnnotationHitsTransformer.SegmentHit(S6.getBoundary(), S6.getBoundary(), 0);
        hit1.setContextEnd(S6.getBoundary());
        withHits("my-annotation", List.of(hit1));

        Document expected = new Document();
        expected.put("TARGET_FIELD", new Content(allHitsToString(allHitsResult), HIT_KEY, true));

        test(Map.entry(HIT_KEY, new Document()), Map.entry(HIT_KEY, expected));
    }

    @Test
    public void plaintextKeywordParameterTest() throws AllHitsException, JsonProcessingException {
        givenAnnotation(buildAnnotation("ANNO1", "20260112_0", "test", "123.345.456", "hash", S7, S1, S6, S2, S5, S3, S4));

        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");
        withParameter(AnnotationHitsTransformer.KEYWORDS_PARAMETER, "aAa.*;bBb.*");

        query = "abc";
        targetField = "TARGET_FIELD";
        validTypes = Set.of("ANNO1", "ANNO2");

        when(annotationDao.getAnnotations("20260112_0", "test", "123.345.456")).thenReturn(annotations);
        lenient().when(normalizer.normalize("aAa.*")).thenReturn("aaa.*");
        lenient().when(normalizer.normalize("bBb.*")).thenReturn("bbb.*");
        withNormalizers();

        AnnotationHitsTransformer.SegmentHit hit1 = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 0);
        hit1.setContextEnd(S1.getBoundary());
        AnnotationHitsTransformer.SegmentHit hit2 = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 1);
        hit2.setContextEnd(S1.getBoundary());
        withHits("my-annotation", List.of(hit1, hit2));

        Document expected = new Document();
        expected.put("TARGET_FIELD", new Content(allHitsToString(allHitsResult), HIT_KEY, true));

        test(Map.entry(HIT_KEY, new Document()), Map.entry(HIT_KEY, expected));
    }

    @Test
    public void jsonKeywordParameterTest() throws AllHitsException, JsonProcessingException {
        givenAnnotation(buildAnnotation("ANNO1", "20260112_0", "test", "123.345.456", "hash", S7, S1, S6, S2, S5, S3, S4));

        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");
        withParameter(AnnotationHitsTransformer.KEYWORDS_PARAMETER, "[\"aAa.*\", \"bBb.*\"]");

        query = "abc";
        targetField = "TARGET_FIELD";
        validTypes = Set.of("ANNO1", "ANNO2");

        when(annotationDao.getAnnotations("20260112_0", "test", "123.345.456")).thenReturn(annotations);
        lenient().when(normalizer.normalize("aAa.*")).thenReturn("aaa.*");
        lenient().when(normalizer.normalize("bBb.*")).thenReturn("bbb.*");
        withNormalizers();

        AnnotationHitsTransformer.SegmentHit hit1 = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 0);
        hit1.setContextEnd(S1.getBoundary());
        AnnotationHitsTransformer.SegmentHit hit2 = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 1);
        hit2.setContextEnd(S1.getBoundary());
        withHits("my-annotation", List.of(hit1, hit2));

        Document expected = new Document();
        expected.put("TARGET_FIELD", new Content(allHitsToString(allHitsResult), HIT_KEY, true));

        test(Map.entry(HIT_KEY, new Document()), Map.entry(HIT_KEY, expected));
    }

    @Test
    public void urlEncodedJsonKeywordParameterTest() throws AllHitsException, JsonProcessingException {
        givenAnnotation(buildAnnotation("ANNO1", "20260112_0", "test", "123.345.456", "hash", S7, S1, S6, S2, S5, S3, S4));

        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");
        withParameter(AnnotationHitsTransformer.KEYWORDS_PARAMETER, URLEncoder.encode("[\"aAa.*\", \"bBb.*\"]", StandardCharsets.UTF_8));

        query = "abc";
        targetField = "TARGET_FIELD";
        validTypes = Set.of("ANNO1", "ANNO2");

        when(annotationDao.getAnnotations("20260112_0", "test", "123.345.456")).thenReturn(annotations);
        lenient().when(normalizer.normalize("aAa.*")).thenReturn("aaa.*");
        lenient().when(normalizer.normalize("bBb.*")).thenReturn("bbb.*");
        withNormalizers();

        AnnotationHitsTransformer.SegmentHit hit1 = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 0);
        hit1.setContextEnd(S1.getBoundary());
        AnnotationHitsTransformer.SegmentHit hit2 = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 1);
        hit2.setContextEnd(S1.getBoundary());
        withHits("my-annotation", List.of(hit1, hit2));

        Document expected = new Document();
        expected.put("TARGET_FIELD", new Content(allHitsToString(allHitsResult), HIT_KEY, true));

        test(Map.entry(HIT_KEY, new Document()), Map.entry(HIT_KEY, expected));
    }

    @Test
    public void enrichmentFieldMapShardQueryConfigurationDefaultGroupingNotationTest() {
        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");

        enrichmentFieldMap.put("EVENT_FIELD", "field");

        transformer = new AnnotationHitsTransformer(shardQueryConfiguration, query, termExtractor, normalizer, annotationDao, allHitsFactory,
                        maxContextBoundary, validTypes, targetField, enrichmentFieldMap);

        transformer.initialize(settings, markingFunctions);
        assertTrue(shardQueryConfiguration.getIncludeGroupingContext());
    }

    @Test
    public void enrichmentFieldMapShardQueryConfigurationGroupingNotationFalseTest() {
        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");
        withParameter(INCLUDE_GROUPING_CONTEXT, "false");

        enrichmentFieldMap.put("EVENT_FIELD", "field");

        transformer = new AnnotationHitsTransformer(shardQueryConfiguration, query, termExtractor, normalizer, annotationDao, allHitsFactory,
                        maxContextBoundary, validTypes, targetField, enrichmentFieldMap);

        transformer.initialize(settings, markingFunctions);
        assertTrue(shardQueryConfiguration.getIncludeGroupingContext());
    }

    @Test
    public void enrichmentFieldMapShardQueryConfigurationGroupingNotationTrueTest() {
        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");
        withParameter(INCLUDE_GROUPING_CONTEXT, "true");

        enrichmentFieldMap.put("EVENT_FIELD", "field");

        transformer = new AnnotationHitsTransformer(shardQueryConfiguration, query, termExtractor, normalizer, annotationDao, allHitsFactory,
                        maxContextBoundary, validTypes, targetField, enrichmentFieldMap);

        transformer.initialize(settings, markingFunctions);
        // not forcibly changed, because should be managed externally to AnnotationHitsTransformer
        assertFalse(shardQueryConfiguration.getIncludeGroupingContext());
    }

    @Test
    public void enrichmentFieldMapShardQueryConfigurationReturnFieldsMissingTest() {
        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");

        enrichmentFieldMap.put("EVENT_FIELD", "field");

        transformer = new AnnotationHitsTransformer(shardQueryConfiguration, query, termExtractor, normalizer, annotationDao, allHitsFactory,
                        maxContextBoundary, validTypes, targetField, enrichmentFieldMap);

        transformer.initialize(settings, markingFunctions);
        assertEquals(0, shardQueryConfiguration.getProjectFields().size());
    }

    @Test
    public void enrichmentFieldMapShardQueryConfigurationReturnFieldsMissTest() {
        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");
        withParameter(QueryParameters.RETURN_FIELDS, "field1");
        shardQueryConfiguration.setProjectFields(Set.of("field1"));

        enrichmentFieldMap.put("EVENT_FIELD", "field");

        transformer = new AnnotationHitsTransformer(shardQueryConfiguration, query, termExtractor, normalizer, annotationDao, allHitsFactory,
                        maxContextBoundary, validTypes, targetField, enrichmentFieldMap);

        transformer.initialize(settings, markingFunctions);
        assertEquals(2, shardQueryConfiguration.getProjectFields().size());
        assertTrue(shardQueryConfiguration.getProjectFields().contains("EVENT_FIELD"));
    }

    @Test
    public void enrichmentFieldMapShardQueryConfigurationReturnFieldsHitTest() {
        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");
        withParameter(QueryParameters.RETURN_FIELDS, "EVENT_FIELD");
        shardQueryConfiguration.setProjectFields(Set.of("EVENT_FIELD"));

        enrichmentFieldMap.put("EVENT_FIELD", "field");

        transformer = new AnnotationHitsTransformer(shardQueryConfiguration, query, termExtractor, normalizer, annotationDao, allHitsFactory,
                        maxContextBoundary, validTypes, targetField, enrichmentFieldMap);

        transformer.initialize(settings, markingFunctions);
        assertEquals(1, shardQueryConfiguration.getProjectFields().size());
        assertTrue(shardQueryConfiguration.getProjectFields().contains("EVENT_FIELD"));
    }

    @Test
    public void enrichmentFieldMap_sourceHashNotFoundTest()
                    throws ParseException, JavaRegexAnalyzer.JavaRegexParseException, AllHitsException, JsonProcessingException {
        Document expected = new Document();
        expected.put("TARGET_FIELD", new Content(allHitsToString(allHitsResult), HIT_KEY, true));

        enrichmentFieldMapTest(new Document(), expected);
    }

    @Test
    public void enrichmentFieldMap_fieldNotFoundTest()
                    throws ParseException, JavaRegexAnalyzer.JavaRegexParseException, AllHitsException, JsonProcessingException {
        givenAnnotationSource(AnnotationSource.newBuilder().setAnalyticHash("abc").setAnalyticSourceHash("hash").build());

        Document expected = new Document();
        expected.put("TARGET_FIELD", new Content(allHitsToString(allHitsResult), HIT_KEY, true));

        enrichmentFieldMapTest(new Document(), expected);
    }

    @Test
    public void enrichmentFieldMap_fieldWrongHashFormatTest()
                    throws ParseException, JavaRegexAnalyzer.JavaRegexParseException, AllHitsException, JsonProcessingException {
        givenAnnotationSource(AnnotationSource.newBuilder().setAnalyticHash("abc").setAnalyticSourceHash("hash").build());
        when(annotationDao.getAnnotationSource("hash")).thenReturn(optionalSource);

        Document expected = new Document();
        expected.put("TARGET_FIELD", new Content(allHitsToString(allHitsResult), HIT_KEY, true));
        expected.put("EVENT_FIELD.123.345.456", new Content("data", HIT_KEY, true));

        Document source = new Document();
        source.put("EVENT_FIELD.123.345.456", new Content("data", HIT_KEY, true), true);

        enrichmentFieldMapTest(source, expected);
    }

    @Test
    public void enrichmentFieldMap_fieldMatchTest()
                    throws ParseException, JavaRegexAnalyzer.JavaRegexParseException, AllHitsException, JsonProcessingException {
        givenAnnotationSource(AnnotationSource.newBuilder().setAnalyticHash("abc").setAnalyticSourceHash("hash").build());
        when(annotationDao.getAnnotationSource("hash")).thenReturn(optionalSource);

        allHitsResult.addDynamicProperties("field", "data");

        Document expected = new Document();
        expected.put("TARGET_FIELD", new Content(allHitsToString(allHitsResult), HIT_KEY, true));
        expected.put("EVENT_FIELD.abc.345.456", new Content("data", HIT_KEY, true));

        Document source = new Document();
        source.put("EVENT_FIELD.abc.345.456", new Content("data", HIT_KEY, true), true);

        enrichmentFieldMapTest(source, expected);
    }

    @Test
    public void enrichmentFieldMap_multiFieldMatchTest()
                    throws ParseException, JavaRegexAnalyzer.JavaRegexParseException, AllHitsException, JsonProcessingException {
        givenAnnotationSource(AnnotationSource.newBuilder().setAnalyticHash("abc").setAnalyticSourceHash("hash").build());
        when(annotationDao.getAnnotationSource("hash")).thenReturn(optionalSource);

        allHitsResult.addDynamicProperties("field", "data;data2");

        Document expected = new Document();
        expected.put("TARGET_FIELD", new Content(allHitsToString(allHitsResult), HIT_KEY, true));
        expected.put("EVENT_FIELD.abc.345.456", new Content("data", HIT_KEY, true));
        expected.put("EVENT_FIELD.abc.444.456", new Content("data2", HIT_KEY, true));

        Document source = new Document();
        source.put("EVENT_FIELD.abc.345.456", new Content("data", HIT_KEY, true), true);
        source.put("EVENT_FIELD.abc.444.456", new Content("data2", HIT_KEY, true), true);

        enrichmentFieldMapTest(source, expected);
    }

    @Test
    public void enrichmentFieldMap_fieldMatchNotInReturnFieldsTest()
                    throws ParseException, JavaRegexAnalyzer.JavaRegexParseException, AllHitsException, JsonProcessingException {
        withParameter(RETURN_FIELDS, "UUID");

        givenAnnotationSource(AnnotationSource.newBuilder().setAnalyticHash("abc").setAnalyticSourceHash("hash").build());
        when(annotationDao.getAnnotationSource("hash")).thenReturn(optionalSource);

        allHitsResult.addDynamicProperties("field", "data");

        Document expected = new Document();
        expected.put("TARGET_FIELD", new Content(allHitsToString(allHitsResult), HIT_KEY, true));

        Document source = new Document();
        source.put("EVENT_FIELD.abc.345.456", new Content("data", HIT_KEY, true), true);

        enrichmentFieldMapTest(source, expected);
    }

    @Test
    public void enrichmentFieldMap_fieldMatchAlreadyInReturnFieldsTest()
                    throws ParseException, JavaRegexAnalyzer.JavaRegexParseException, AllHitsException, JsonProcessingException {
        withParameter(RETURN_FIELDS, "A,EVENT_FIELD,B");

        givenAnnotationSource(AnnotationSource.newBuilder().setAnalyticHash("abc").setAnalyticSourceHash("hash").build());
        when(annotationDao.getAnnotationSource("hash")).thenReturn(optionalSource);

        allHitsResult.addDynamicProperties("field", "data");

        Document expected = new Document();
        expected.put("TARGET_FIELD", new Content(allHitsToString(allHitsResult), HIT_KEY, true));
        expected.put("EVENT_FIELD.abc.345.456", new Content("data", HIT_KEY, true));

        Document source = new Document();
        source.put("EVENT_FIELD.abc.345.456", new Content("data", HIT_KEY, true), true);

        enrichmentFieldMapTest(source, expected);
    }

    @Test
    public void enrichmentFieldMap_fieldMatchAlreadyInReturnFieldsWithGroupingTest()
                    throws ParseException, JavaRegexAnalyzer.JavaRegexParseException, AllHitsException, JsonProcessingException {
        withParameter(INCLUDE_GROUPING_CONTEXT, "true");
        withParameter(RETURN_FIELDS, "A,EVENT_FIELD,B");

        givenAnnotationSource(AnnotationSource.newBuilder().setAnalyticHash("abc").setAnalyticSourceHash("hash").build());
        when(annotationDao.getAnnotationSource("hash")).thenReturn(optionalSource);

        allHitsResult.addDynamicProperties("field", "data");

        Document expected = new Document();
        expected.put("TARGET_FIELD", new Content(allHitsToString(allHitsResult), HIT_KEY, true));
        expected.put("EVENT_FIELD.abc.345.456", new Content("data", HIT_KEY, true), true);

        Document source = new Document();
        source.put("EVENT_FIELD.abc.345.456", new Content("data", HIT_KEY, true), true);

        enrichmentFieldMapTest(source, expected);
    }

    @Test
    public void enrichmentFieldMap_fieldMatchGroupingNotationOnTest()
                    throws ParseException, JavaRegexAnalyzer.JavaRegexParseException, AllHitsException, JsonProcessingException {
        withParameter(INCLUDE_GROUPING_CONTEXT, "true");

        givenAnnotationSource(AnnotationSource.newBuilder().setAnalyticHash("abc").setAnalyticSourceHash("hash").build());
        when(annotationDao.getAnnotationSource("hash")).thenReturn(optionalSource);

        allHitsResult.addDynamicProperties("field", "data");

        Document expected = new Document();
        expected.put("TARGET_FIELD", new Content(allHitsToString(allHitsResult), HIT_KEY, true));
        expected.put("EVENT_FIELD.abc.345.456", new Content("data", HIT_KEY, true), true);

        Document source = new Document();
        source.put("EVENT_FIELD.abc.345.456", new Content("data", HIT_KEY, true), true);

        enrichmentFieldMapTest(source, expected);
    }

    @ParameterizedTest(name = "forceGrouping={0} forceStripFields={1}")
    @CsvSource({"true,false", "true,true"})
    public void forceGrouping_null_test(boolean forceGrouping, boolean stripFields) {
        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");

        if (forceGrouping) {
            // force population of the enrichment field map, this is what forces grouping notation
            enrichmentFieldMap.put("EVENT_FIELD", "all-hits-field");
        }
        if (stripFields) {
            withParameter(QueryParameters.RETURN_FIELDS, "field1");
        }

        test(null, null);
    }

    private void applyGroupingParameters(boolean forceGrouping, boolean stripFields) {
        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");
        if (forceGrouping) {
            // force population of the enrichment field map, this is what forces grouping notation
            enrichmentFieldMap.put("EVENT_FIELD", "all-hits-field");
        }
        if (stripFields) {
            withParameter(QueryParameters.RETURN_FIELDS, "SOME_FIELD");
        }
    }

    private Document getGroupingTestSourceDoc() {
        Document doc = new Document();
        doc.put("MY.GROUPED.FIELD", new Content("abc", new Key(), true), true);
        doc.put("EVENT_FIELD", new Content("remove me if stripping fields", new Key(), true));

        return doc;
    }

    private Document getGroupingTestExpectedDoc(boolean forceGrouping, boolean stripFields) {
        Document returnDoc = new Document();
        if (forceGrouping) {
            returnDoc.put("MY", new Content("abc", new Key(), true));
        } else {
            returnDoc.put("MY.GROUPED.FIELD", new Content("abc", new Key(), true), true);
        }
        if (!stripFields) {
            returnDoc.put("EVENT_FIELD", new Content("remove me if stripping fields", new Key(), true));
        }

        return returnDoc;
    }

    @ParameterizedTest(name = "forceGrouping={0} forceStripFields={1}")
    @CsvSource({"false,false", "true,false", "true,true"})
    public void forceGrouping_noSearchTerms_test(boolean forceGrouping, boolean stripFields) {
        applyGroupingParameters(forceGrouping, stripFields);
        Document doc = getGroupingTestSourceDoc();
        Document returnDoc = getGroupingTestExpectedDoc(forceGrouping, stripFields);

        test(Map.entry(new Key(), doc), Map.entry(new Key(), returnDoc));
    }

    @ParameterizedTest(name = "forceGrouping={0} forceStripFields={1}")
    @CsvSource({"false,false", "true,false", "true,true"})
    public void forceGroupingStrip_nullKey_test(boolean forceGrouping, boolean stripFields) throws ParseException, JavaRegexAnalyzer.JavaRegexParseException {
        applyGroupingParameters(forceGrouping, stripFields);
        Document doc = getGroupingTestSourceDoc();
        Document returnDoc = getGroupingTestExpectedDoc(forceGrouping, stripFields);

        // add some search terms otherwise we don't process far enough to see nullKey checked
        when(termExtractor.extract(query, normalizer)).thenReturn(Set.of("t1"));

        Map<Key,Document> input = new HashMap<>();
        input.put(null, doc);

        Map<Key,Document> expected = new HashMap<>();
        expected.put(null, returnDoc);

        test(input.entrySet().iterator().next(), expected.entrySet().iterator().next());
    }

    @ParameterizedTest(name = "forceGrouping={0} forceStripFields={1}")
    @CsvSource({"false,false", "true,false", "true,true"})
    public void forceGroupingStrip_nullDocument_test(boolean forceGrouping, boolean stripFields)
                    throws ParseException, JavaRegexAnalyzer.JavaRegexParseException {
        applyGroupingParameters(forceGrouping, stripFields);

        // add some search terms otherwise we don't process far enough to see nullKey checked
        when(termExtractor.extract(query, normalizer)).thenReturn(Set.of("t1"));

        Map<Key,Document> input = new HashMap<>();
        input.put(new Key("a"), null);

        Map<Key,Document> expected = new HashMap<>();
        expected.put(new Key("a"), null);

        test(input.entrySet().iterator().next(), expected.entrySet().iterator().next());
    }

    @ParameterizedTest(name = "forceGrouping={0} forceStripFields={1}")
    @CsvSource({"false,false", "true,false", "true,true"})
    public void forceGroupingStrip_notKeepDocument_test(boolean forceGrouping, boolean stripFields)
                    throws ParseException, JavaRegexAnalyzer.JavaRegexParseException {
        applyGroupingParameters(forceGrouping, stripFields);
        Document groupedDocument = getGroupingTestSourceDoc();
        groupedDocument.setToKeep(false);
        Document returnDoc = getGroupingTestExpectedDoc(forceGrouping, stripFields);
        returnDoc.setToKeep(false);

        // add some search terms otherwise we don't process far enough to see nullKey checked
        when(termExtractor.extract(query, normalizer)).thenReturn(Set.of("t1"));

        test(Map.entry(new Key(), groupedDocument), Map.entry(new Key(), returnDoc));
    }

    @ParameterizedTest(name = "forceGrouping={0} forceStripFields={1}")
    @CsvSource({"false,false", "true,false", "true,true"})
    public void forceGroupingStrip_targetFieldConflict_Test(boolean forceGrouping, boolean stripFields)
                    throws ParseException, JavaRegexAnalyzer.JavaRegexParseException {
        applyGroupingParameters(forceGrouping, stripFields);
        Document doc = getGroupingTestSourceDoc();
        doc.put("TARGET_FIELD", new Content("conflicting", new Key(), true));
        Document returnDoc = getGroupingTestExpectedDoc(forceGrouping, stripFields);
        returnDoc.put("TARGET_FIELD", new Content("conflicting", new Key(), true));

        // add some search terms otherwise we don't process far enough to see nullKey checked
        when(termExtractor.extract(query, normalizer)).thenReturn(Set.of("t1"));

        // set the target field to match the existing field
        targetField = "TARGET_FIELD";

        test(Map.entry(new Key(), doc), Map.entry(new Key(), returnDoc));
    }

    @ParameterizedTest(name = "forceGrouping={0} forceStripFields={1}")
    @CsvSource({"false,false", "true,false", "true,true"})
    public void forceGroupingStrip_unexpectedKey_test(boolean forceGrouping, boolean stripFields)
                    throws ParseException, JavaRegexAnalyzer.JavaRegexParseException {
        applyGroupingParameters(forceGrouping, stripFields);
        Document doc = getGroupingTestSourceDoc();
        Document returnDoc = getGroupingTestExpectedDoc(forceGrouping, stripFields);

        // add some search terms otherwise we don't process far enough to see nullKey checked
        when(termExtractor.extract(query, normalizer)).thenReturn(Set.of("t1"));

        // set the target field to match the existing field
        targetField = "TARGET_FIELD";

        test(Map.entry(new Key("unexpected"), doc), Map.entry(new Key("unexpected"), returnDoc));
    }

    @ParameterizedTest(name = "forceGrouping={0} forceStripFields={1}")
    @CsvSource({"false,false", "true,false", "true,true"})
    public void forceGroupingStrip_noAnnotations_Test(boolean forceGrouping, boolean stripFields)
                    throws ParseException, JavaRegexAnalyzer.JavaRegexParseException {
        applyGroupingParameters(forceGrouping, stripFields);
        Document doc = getGroupingTestSourceDoc();
        Document returnDoc = getGroupingTestExpectedDoc(forceGrouping, stripFields);

        // add some search terms otherwise we don't process far enough to see nullKey checked
        when(termExtractor.extract(query, normalizer)).thenReturn(Set.of("t1"));

        // set the target field to match the existing field
        targetField = "TARGET_FIELD";

        // no matching annotations
        when(annotationDao.getAnnotations("20260112_0", "test", "123.345.456")).thenReturn(annotations);

        test(Map.entry(HIT_KEY, doc), Map.entry(HIT_KEY, returnDoc));
    }

    @ParameterizedTest(name = "forceGrouping={0} forceStripFields={1}")
    @CsvSource({"false,false", "true,false", "true,true"})
    public void forceGroupingStrip_noMatchingTypes_Test(boolean forceGrouping, boolean stripFields)
                    throws ParseException, JavaRegexAnalyzer.JavaRegexParseException {
        applyGroupingParameters(forceGrouping, stripFields);
        Document doc = getGroupingTestSourceDoc();
        Document returnDoc = getGroupingTestExpectedDoc(forceGrouping, stripFields);

        // add some search terms otherwise we don't process far enough to see nullKey checked
        when(termExtractor.extract(query, normalizer)).thenReturn(Set.of("t1"));

        // set the target field to match the existing field
        targetField = "TARGET_FIELD";

        // get some annotations
        givenAnnotation(buildAnnotation("ANNO1", "20260112_0", "test", "123.345.456", "hash", S7, S1, S6, S2, S5, S3, S4));
        when(annotationDao.getAnnotations("20260112_0", "test", "123.345.456")).thenReturn(annotations);

        // set the types to not include the annotation found
        validTypes = Set.of("ANNO2");

        test(Map.entry(HIT_KEY, doc), Map.entry(HIT_KEY, returnDoc));
    }

    @ParameterizedTest(name = "forceGrouping={0} forceStripFields={1}")
    @CsvSource({"false,false", "true,false", "true,true"})
    public void forceGroupingStrip_unmatched_Test(boolean forceGrouping, boolean stripFields) throws ParseException, JavaRegexAnalyzer.JavaRegexParseException {
        applyGroupingParameters(forceGrouping, stripFields);
        Document doc = getGroupingTestSourceDoc();
        Document returnDoc = getGroupingTestExpectedDoc(forceGrouping, stripFields);
        returnDoc.put("TARGET_FIELD", new Content("[]", HIT_KEY, true));

        // add some search terms otherwise we don't process far enough to see nullKey checked
        when(termExtractor.extract(query, normalizer)).thenReturn(Set.of("t1"));

        // set the target field to match the existing field
        targetField = "TARGET_FIELD";

        // get some annotations
        givenAnnotation(buildAnnotation("ANNO1", "20260112_0", "test", "123.345.456", "hash", S7, S1, S6, S2, S5, S3, S4));
        when(annotationDao.getAnnotations("20260112_0", "test", "123.345.456")).thenReturn(annotations);

        // set the types to not include the annotation found
        validTypes = Set.of("ANNO1");

        withNormalizers();

        test(Map.entry(HIT_KEY, doc), Map.entry(HIT_KEY, returnDoc));
    }

    @ParameterizedTest(name = "forceStripFields={0}")
    @CsvSource({"false,false", "true,false", "true,true"})
    public void verifyForcedGroupingTest(boolean forceGrouping, boolean stripFields) {
        applyGroupingParameters(forceGrouping, stripFields);

        // the query config already forced grouping notation
        shardQueryConfiguration.setIncludeGroupingContext(true);

        transformer = new AnnotationHitsTransformer(shardQueryConfiguration, query, termExtractor, normalizer, annotationDao, allHitsFactory,
                        maxContextBoundary, validTypes, targetField, enrichmentFieldMap);
        transformer.initialize(settings, markingFunctions);

        Document doc = getGroupingTestSourceDoc();
        // this is forced false because we shouldn't actually be in a forced mode
        Document returnDoc = getGroupingTestExpectedDoc(false, stripFields);

        test(Map.entry(HIT_KEY, doc), Map.entry(HIT_KEY, returnDoc));
    }

    @Test
    public void forcedGroupingStrippedAcrossMultiplePageApplyCallsTest() {
        // ShardQueryLogic constructs a single AnnotationHitsTransformer instance per query (initialize() is only
        // called once) and reuses that same instance -- and its shardQueryConfiguration -- across every
        // page/next() call. Verify that grouping notation forced on the first page/apply() call is still
        // correctly stripped on subsequent pages/apply() calls using that same instance.
        applyGroupingParameters(true, false);

        Document expected = getGroupingTestExpectedDoc(true, false);

        transformer = new AnnotationHitsTransformer(shardQueryConfiguration, query, termExtractor, normalizer, annotationDao, allHitsFactory,
                        maxContextBoundary, validTypes, targetField, enrichmentFieldMap);
        transformer.initialize(settings, markingFunctions);
        assertTrue(shardQueryConfiguration.getIncludeGroupingContext(), "grouping context should be forced on for the life of the query");

        // page 1
        Entry<Key,Document> page1 = transformer.apply(Map.entry(new Key(), getGroupingTestSourceDoc()));
        assertEquals(expected, page1.getValue());

        // page 2: same transformer instance, same shardQueryConfiguration, reused as ShardQueryLogic now does
        Entry<Key,Document> page2 = transformer.apply(Map.entry(new Key(), getGroupingTestSourceDoc()));
        assertEquals(expected, page2.getValue());

        // page 3: verify it continues to work beyond just a second call
        Entry<Key,Document> page3 = transformer.apply(Map.entry(new Key(), getGroupingTestSourceDoc()));
        assertEquals(expected, page3.getValue());
    }

    @Test
    public void updateConfigPicksUpChangedParametersOnSubsequentPageTest()
                    throws ParseException, JavaRegexAnalyzer.JavaRegexParseException, AllHitsException, JsonProcessingException {
        // ShardQueryLogic now follows the initialize()/updateConfig() lifecycle contract used by the other
        // config-based transforms: construct once (calling initialize()), then call updateConfig() on later
        // pages so that any query parameters which legitimately change across pages (e.g. after a
        // checkpoint/resume) are picked up, without re-running the full constructor.
        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");
        withParameter(AnnotationHitsTransformer.MIN_SCORE_PARAMETER, ".9");
        query = "abc";
        targetField = "TARGET_FIELD";
        validTypes = Set.of("ANNO1", "ANNO2");

        transformer = new AnnotationHitsTransformer(shardQueryConfiguration, query, termExtractor, normalizer, annotationDao, allHitsFactory,
                        maxContextBoundary, validTypes, targetField, enrichmentFieldMap);
        transformer.initialize(settings, markingFunctions);

        // simulate settings changing on a later page/next() call and ShardQueryLogic invoking updateConfig()
        // (rather than reconstructing the transformer) on the existing instance
        withParameter(AnnotationHitsTransformer.MIN_SCORE_PARAMETER, "0");
        transformer.updateConfig(settings);

        Set<String> queryTerms = Set.of("bbbbbbb", "v2", "v3");
        givenAnnotation(buildAnnotation("ANNO1", "20260112_0", "test", "123.345.456", "hash", S7, S1, S6, S2, S5, S3, S4));
        when(termExtractor.extract(query, normalizer)).thenReturn(queryTerms);
        when(annotationDao.getAnnotations("20260112_0", "test", "123.345.456")).thenReturn(annotations);
        withNormalizers();

        AnnotationHitsTransformer.SegmentHit hit1 = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 0);
        hit1.setContextEnd(S1.getBoundary());
        withHits("my-annotation", List.of(hit1));

        Document expected = new Document();
        expected.put("TARGET_FIELD", new Content(allHitsToString(allHitsResult), HIT_KEY, true));

        // bbbbbbb scores .5, which would have been filtered out by the original min.score of .9, but should now
        // pass now that updateConfig() lowered min.score to 0
        Entry<Key,Document> result = transformer.apply(Map.entry(HIT_KEY, new Document()));
        assertEquals(expected, result.getValue());
    }

    @Test
    public void jexlQueryStringReadLiveFromConfigOnFirstApplyTest()
                    throws ParseException, JavaRegexAnalyzer.JavaRegexParseException, AllHitsException, JsonProcessingException {
        // AnnotationHitsTransformer may be constructed by ShardQueryLogic before the shared config's original
        // jexl query string has been populated (see ShardQueryLogic#loadQueryParameters() calling
        // getTransformer() before setOriginalJexlQuery() is called). Verify that if the config's jexl query is
        // set (live) between construction and the first apply() call, the live value is used rather than the
        // (null) value captured at construction time.
        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");

        Set<String> queryTerms = Set.of("aaaaaaa", "v2", "v3");
        givenAnnotation(buildAnnotation("ANNO1", "20260112_0", "test", "123.345.456", "hash", S7, S1, S6, S2, S5, S3, S4));
        targetField = "TARGET_FIELD";
        validTypes = Set.of("ANNO1", "ANNO2");
        when(annotationDao.getAnnotations("20260112_0", "test", "123.345.456")).thenReturn(annotations);
        withNormalizers();

        // construct with a null jexlQueryString, simulating the premature getTransformer() call
        transformer = new AnnotationHitsTransformer(shardQueryConfiguration, null, termExtractor, normalizer, annotationDao, allHitsFactory, maxContextBoundary,
                        validTypes, targetField, enrichmentFieldMap);
        transformer.initialize(settings, markingFunctions);

        // the config's jexl query is populated afterward, as it would be later in ShardQueryLogic#initialize()
        String liveJexlQuery = "abc";
        shardQueryConfiguration.setOriginalJexlQuery(liveJexlQuery);
        when(termExtractor.extract(liveJexlQuery, normalizer)).thenReturn(queryTerms);

        AnnotationHitsTransformer.SegmentHit hit1 = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 1);
        hit1.setContextEnd(S1.getBoundary());
        withHits("my-annotation", List.of(hit1));

        Document expected = new Document();
        expected.put("TARGET_FIELD", new Content(allHitsToString(allHitsResult), HIT_KEY, true));

        Entry<Key,Document> result = transformer.apply(Map.entry(HIT_KEY, new Document()));
        assertEquals(expected, result.getValue());
    }

    @Test
    public void finalDocumentKeyTest() {
        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");
        withParameter(AnnotationHitsTransformer.KEYWORDS_PARAMETER, "keyword");

        targetField = "TARGET_FIELD";
        when(normalizer.normalize("keyword")).thenReturn("keyword");

        QuerySpanCollector collector = mock(QuerySpanCollector.class);
        QuerySpan querySpan = mock(QuerySpan.class);
        Range range = new Range("begin", "end");
        YieldCallback yieldCallback = mock(YieldCallback.class);

        // force a final document to return
        when(querySpan.hasEntries()).thenReturn(true);
        // give something to return
        when(collector.getCombinedQuerySpan(querySpan, true)).thenReturn(querySpan);

        List<Entry<Key,Document>> entryList = List.of();
        FinalDocumentTrackingIterator fdti = new FinalDocumentTrackingIterator(collector, querySpan, range, entryList.iterator(), yieldCallback);
        // will the stats be returned?
        assertTrue(fdti.hasNext());

        Entry<Key,Document> finalDoc = fdti.next();
        transformer = new AnnotationHitsTransformer(shardQueryConfiguration, query, termExtractor, normalizer, annotationDao, allHitsFactory,
                        maxContextBoundary, validTypes, targetField, enrichmentFieldMap);
        transformer.initialize(settings, markingFunctions);

        assertEquals(finalDoc, transformer.apply(finalDoc));

        verifyNoInteractions(termExtractor);
        verifyNoInteractions(annotationDao);
        verifyNoInteractions(allHitsFactory);
    }

    /**
     * Simulates what happens after the last document is returned
     */
    @Test
    public void finalDocumentWithDocumentRangeTest() {
        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");
        query = "abc";
        targetField = "TARGET_FIELD";
        validTypes = Set.of("ANNO1", "ANNO2");

        QuerySpanCollector collector = mock(QuerySpanCollector.class);
        QuerySpan querySpan = mock(QuerySpan.class);
        Range range = new Range(HIT_KEY, true, null, true);
        YieldCallback yieldCallback = mock(YieldCallback.class);

        // force a final document to return
        when(querySpan.hasEntries()).thenReturn(true);
        // give something to return
        when(collector.getCombinedQuerySpan(querySpan, true)).thenReturn(querySpan);

        List<Entry<Key,Document>> entryList = List.of();
        FinalDocumentTrackingIterator fdti = new FinalDocumentTrackingIterator(collector, querySpan, range, entryList.iterator(), yieldCallback);
        // will the stats be returned?
        assertTrue(fdti.hasNext());
        Entry<Key,Document> finalDocumentEntry = fdti.next();
        Document originalDoc = finalDocumentEntry.getValue().copy();
        test(finalDocumentEntry, Map.entry(finalDocumentEntry.getKey(), originalDoc));

        verifyNoInteractions(termExtractor);
        verifyNoInteractions(annotationDao);
        verifyNoInteractions(normalizer);
        verifyNoInteractions(allHitsFactory);
    }

    private void enrichmentFieldMapTest(Document input, Document output) throws ParseException, JavaRegexAnalyzer.JavaRegexParseException, AllHitsException {
        withParameter(AnnotationHitsTransformer.ENABLED_PARAMETER, "true");
        enrichmentFieldMap.put("EVENT_FIELD", "field");

        Set<String> queryTerms = Set.of("aaaaaaa", "v2", "v3");

        givenAnnotation(buildAnnotation("ANNO1", "20260112_0", "test", "123.345.456", "hash", S7, S1, S6, S2, S5, S3, S4));

        query = "abc";
        targetField = "TARGET_FIELD";
        validTypes = Set.of("ANNO1");

        when(termExtractor.extract(query, normalizer)).thenReturn(queryTerms);
        when(annotationDao.getAnnotations("20260112_0", "test", "123.345.456")).thenReturn(annotations);
        withNormalizers();

        AnnotationHitsTransformer.SegmentHit hit = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 1);
        hit.setContextEnd(S1.getBoundary());
        withHits("my-annotation", List.of(hit));

        test(Map.entry(HIT_KEY, input), Map.entry(HIT_KEY, output));
    }

    private String allHitsToString(AllHits... allHits) throws JsonProcessingException {
        return objectMapper.writeValueAsString(allHits);
    }

    private void withHits(String id, List<AnnotationHitsTransformer.SegmentHit> expectedHits) throws AllHitsException {
        when(allHitsFactory.create(any(), any(), any(), any())).thenAnswer(invocation -> {
            List<AnnotationHitsTransformer.SegmentHit> hits = invocation.getArgument(1);
            assertEquals(expectedHits.size(), hits.size());

            for (AnnotationHitsTransformer.SegmentHit hit : hits) {
                boolean found = false;
                for (AnnotationHitsTransformer.SegmentHit expectedHit : expectedHits) {
                    if (expectedHit.equals(hit)) {
                        found = true;
                        break;
                    }
                }
                assertTrue(found);
            }

            AllHits allHits = new AllHits();
            allHits.setAnnotationId(id);

            return allHits;
        });
    }

    private void withNormalizers() {
        lenient().when(normalizer.normalize("aaaAAAA")).thenReturn("aaaaaaa");
        lenient().when(normalizer.normalize("bbbBBBB")).thenReturn("bbbbbbb");
        lenient().when(normalizer.normalize("cccCCCC")).thenReturn("ccccccc");
        lenient().when(normalizer.normalize("dddDDDD")).thenReturn("ddddddd");
        lenient().when(normalizer.normalize("eeeEEEE")).thenReturn("eeeeeee");
        lenient().when(normalizer.normalize("fffFFFF")).thenReturn("fffffff");
        lenient().when(normalizer.normalize("gggGGGG")).thenReturn("ggggggg");
        lenient().when(normalizer.normalize("hhhHHHH")).thenReturn("hhhhhhh");
        lenient().when(normalizer.normalize("iiiIIII")).thenReturn("iiiiiii");
        lenient().when(normalizer.normalize("jjjJJJJ")).thenReturn("jjjjjjj");
        lenient().when(normalizer.normalize("kkkKKKK")).thenReturn("kkkkkkk");
        lenient().when(normalizer.normalize("lllLLLL")).thenReturn("lllllll");
        lenient().when(normalizer.normalize("mmmMMMM")).thenReturn("mmmmmmm");
        lenient().when(normalizer.normalize("nnnNNNN")).thenReturn("nnnnnnn");
    }

    private void withParameter(String key, String value) {
        settings.addParameter(key, value);
    }

    @SuppressWarnings("SameParameterValue")
    private Annotation buildAnnotation(String annotationType, String shard, String dataType, String documentId, String sourceHash, Segment... segments) {
        return buildAnnotation("id", annotationType, shard, dataType, documentId, sourceHash, segments);
    }

    @SuppressWarnings("SameParameterValue")
    private Annotation buildAnnotation(String id, String annotationType, String shard, String dataType, String documentId, String sourceHash,
                    Segment... segments) {
        // @formatter:off
        return Annotation.newBuilder()
                .setShard(shard)
                .setDataType(dataType)
                .setUid(documentId)
                .setAnnotationType(annotationType)
                .setAnalyticSourceHash(sourceHash)
                .putAllMetadata(Map.of("visibility", "ALL", "created_date", "2026-01-12T00:00:00Z"))
                .addAllSegments(List.of(segments))
                .setAnnotationId(id)
                .build();
        // @formatter:on
    }

    private Annotation buildAnnotation(Segment... segments) {
        return buildAnnotation("ANNO1", "20260112_0", "test", "123.345.456", "abc", segments);
    }

    private void givenAnnotation(Annotation annotation) {
        annotations.add(annotation);
    }

    private void givenAnnotationSource(AnnotationSource annotationSource) {
        this.annotationSource = annotationSource;
        this.optionalSource = Optional.of(annotationSource);
    }

    private void test(Entry<Key,Document> entry, Entry<Key,Document> expected) {
        transformer = new AnnotationHitsTransformer(shardQueryConfiguration, query, termExtractor, normalizer, annotationDao, allHitsFactory,
                        maxContextBoundary, validTypes, targetField, enrichmentFieldMap);
        transformer.initialize(settings, markingFunctions);
        Entry<Key,Document> transformed = transformer.apply(entry);

        if (expected == null) {
            assertNull(transformed);
        } else {
            assertNotNull(transformed);
            assertEquals(expected.getKey(), transformed.getKey());
            assertEquals(expected.getValue(), transformed.getValue());
        }
    }

}
