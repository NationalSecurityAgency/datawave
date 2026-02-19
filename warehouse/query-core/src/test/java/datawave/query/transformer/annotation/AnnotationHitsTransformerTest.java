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
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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
import datawave.marking.MarkingFunctions;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.QueryParameters;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.config.ShardQueryConfiguration;
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
    private MarkingFunctions markingFunctions;

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
