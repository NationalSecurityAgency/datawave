package datawave.query.tables;

import static datawave.query.transformer.annotation.AnnotationHitsTransformer.CONTEXT_SIZE_PARAMETER;
import static datawave.query.transformer.annotation.AnnotationHitsTransformer.ENABLED_PARAMETER;
import static datawave.query.transformer.annotation.AnnotationHitsTransformer.KEYWORDS_PARAMETER;
import static datawave.query.transformer.annotation.AnnotationHitsTransformer.MIN_SCORE_PARAMETER;
import static datawave.query.transformer.annotation.AnnotationHitsTransformer.TIMEUNIT_PARAMETER;
import static datawave.query.util.WiseGuysIngest.caponeUID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;

import datawave.annotation.data.AnnotationSerializer;
import datawave.annotation.data.transform.DefaultTimestampTransformer;
import datawave.annotation.data.transform.DefaultVisibilityTransformer;
import datawave.annotation.data.v1.AccumuloAnnotationSerializer;
import datawave.annotation.data.v1.AccumuloAnnotationSourceSerializer;
import datawave.annotation.data.v1.AnnotationDataAccess;
import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.annotation.protobuf.v1.BoundaryType;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.protobuf.v1.SegmentBoundary;
import datawave.annotation.protobuf.v1.SegmentValue;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.iterator.DatawaveTransformIterator;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.data.normalizer.LcNoDiacriticsNormalizer;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.microservice.query.QueryImpl;
import datawave.query.QueryParameters;
import datawave.query.QueryTestTableHelper;
import datawave.query.RebuildingScannerTestHelper;
import datawave.query.config.annotation.AllHitsQueryConfig;
import datawave.query.config.annotation.AnnotationConfig;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.planner.TimedVisitorManager;
import datawave.query.transformer.DocumentTransform;
import datawave.query.transformer.DocumentTransformer;
import datawave.query.transformer.annotation.AllHitsException;
import datawave.query.transformer.annotation.AllHitsFactory;
import datawave.query.transformer.annotation.AllHitsFactoryErrorOnly;
import datawave.query.transformer.annotation.AnnotationHitsTransformer;
import datawave.query.transformer.annotation.BoundaryComparator;
import datawave.query.transformer.annotation.SegmentValueByScoreComparator;
import datawave.query.transformer.annotation.TermExtractor;
import datawave.query.transformer.annotation.model.AllHits;
import datawave.query.util.AbstractQueryTest;
import datawave.query.util.WiseGuysIngest;
import datawave.table.constants.TableName;
import datawave.webservice.query.result.event.DefaultField;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.DefaultEventQueryResponse;

@ExtendWith(SpringExtension.class)
@ComponentScan(basePackages = "datawave.query")
// @formatter:off
@ContextConfiguration(locations = {
        "classpath:datawave/query/QueryLogicFactory.xml",
        "classpath:beanRefContext.xml",
        "classpath:MarkingFunctionsContext.xml",
        "classpath:MetadataHelperContext.xml",
        "classpath:CacheContext.xml"})
// @formatter:on
public class ShardQueryLogicTest extends AbstractQueryTest {

    private static final Logger log = Logger.getLogger(ShardQueryLogicTest.class);

    private static final Authorizations auths = new Authorizations("ALL");
    private static final Set<Authorizations> authSet = Collections.singleton(auths);

    // @formatter:off
    private static final Segment S1 = Segment.newBuilder()
            .addValues(SegmentValue.newBuilder().setValue("capone").setScore(0.3f).build())
            .addValues(SegmentValue.newBuilder().setValue("carl").setScore(1.0f).build())
            .setBoundary(SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(20).setEnd(30).build())
            .build();

    private static final Segment S2 = Segment.newBuilder()
            .addValues(SegmentValue.newBuilder().setValue("a1").setScore(0.6f).build())
            .addValues(SegmentValue.newBuilder().setValue("a2").setScore(0.9f).build())
            .setBoundary(SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(40).setEnd(50).build())
            .build();
    private static final Segment S3 = Segment.newBuilder()
            .addValues(SegmentValue.newBuilder().setValue("b1").setScore(0.5f).build())
            .addValues(SegmentValue.newBuilder().setValue("b2").setScore(0.1f).build())
            .setBoundary(SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(80).setEnd(100).build())
            .build();
    private static final Segment S4 = Segment.newBuilder()
            .addValues(SegmentValue.newBuilder().setValue("c1").setScore(0.6f).build())
            .addValues(SegmentValue.newBuilder().setValue("c2").setScore(0.7f).build())
            .setBoundary(SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(100).setEnd(110).build())
            .build();
    private static final Segment S5 = Segment.newBuilder()
            .addValues(SegmentValue.newBuilder().setValue("d1").setScore(0.6f).build())
            .setBoundary(SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(120).setEnd(250).build())
            .build();

    private static final Segment S6 = Segment.newBuilder()
            .addValues(SegmentValue.newBuilder().setValue("w1").setScore(0.9f).build())
            .addValues(SegmentValue.newBuilder().setValue("w2").setScore(1.0f).build())
            .setBoundary(SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(2).setEnd(3).build())
            .build();
    private static final Segment S7 = Segment.newBuilder()
            .addValues(SegmentValue.newBuilder().setValue("x1").setScore(0.6f).build())
            .addValues(SegmentValue.newBuilder().setValue("x2").setScore(0.9f).build())
            .setBoundary(SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(4).setEnd(5).build())
            .build();
    private static final Segment S8 = Segment.newBuilder()
            .addValues(SegmentValue.newBuilder().setValue("y1").setScore(0.5f).build())
            .addValues(SegmentValue.newBuilder().setValue("y2").setScore(0.1f).build())
            .setBoundary(SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(8).setEnd(10).build())
            .build();
    private static final Segment S9 = Segment.newBuilder()
            .addValues(SegmentValue.newBuilder().setValue("z1").setScore(0.6f).build())
            .addValues(SegmentValue.newBuilder().setValue("z2").setScore(0.7f).build())
            .setBoundary(SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(10).setEnd(11).build())
            .build();
    // @formatter:on

    @Autowired
    @Qualifier("EventQuery")
    protected ShardQueryLogic logic;

    private static AccumuloClient sharedClient;

    private AccumuloClient client;

    private final List<Annotation> annotations = new ArrayList<>();
    private final List<AnnotationSource> annotationSources = new ArrayList<>();
    private final Map<String,Map<String,String>> expectedFields = new HashMap<>();
    private final Map<String,List<String>> expectNoField = new HashMap<>();
    private final Map<String,List<Entry<Key,Value>>> extraData = new HashMap<>();

    private Set<Set<String>> expectedGroups = new HashSet<>();
    private List<EventBase> events = new ArrayList<>();

    @Override
    public ShardQueryLogic getLogic() {
        return logic;
    }

    @Override
    public Authorizations getAuths() {
        return auths;
    }

    @Override
    protected void extraConfigurations() {
        disableQueryPlanAssertion();
        setupAnnotationsTables(client);
        addExtraEventData(client);
    }

    @Override
    protected void executeQuery(ShardQueryLogic logic) throws Exception {
        try {
            DocumentTransformer transformer = (DocumentTransformer) (logic.getTransformer(logic.getConfig().getQuery()));
            TransformIterator iter = new DatawaveTransformIterator(logic.iterator(), transformer);
            List<Object> eventList = new ArrayList<>();
            while (iter.hasNext()) {
                Object o = iter.next();
                if (o != null) {
                    eventList.add(o);
                }
            }

            BaseQueryResponse response = transformer.createResponse(eventList);
            assertTrue(response instanceof DefaultEventQueryResponse);
            this.events = ((DefaultEventQueryResponse) response).getEvents();
        } finally {
            logic.close();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void extraAssertions() {
        if (expectedGroups.isEmpty()) {
            assertTrue(events == null || events.isEmpty());
            return;
        }

        // planAndExecuteQuery() invokes extraAssertions() once per index table variant, so match against a
        // deep local copy rather than destructively consuming the shared expectedGroups sets.
        Set<Set<String>> remaining = new HashSet<>();
        for (Set<String> group : expectedGroups) {
            remaining.add(new HashSet<>(group));
        }

        for (Iterator<Set<String>> it = remaining.iterator(); it.hasNext();) {
            Set<String> expectedSet = it.next();
            boolean found = false;

            for (EventBase event : events) {
                if (expectedSet.contains("UID:" + event.getMetadata().getInternalId())) {
                    expectedSet.remove("UID:" + event.getMetadata().getInternalId());
                    ((List<DefaultField>) event.getFields()).forEach((f) -> expectedSet.remove(f.getName() + ":" + f.getValueString()));
                    if (expectedSet.isEmpty()) {
                        found = true;
                        it.remove();
                    }

                    // check for any expected fields
                    Map<String,String> expectedFieldsForDoc = expectedFields.computeIfAbsent(event.getMetadata().getInternalId(), x -> new HashMap<>());
                    List<String> expectedNoFieldsForDoc = expectNoField.computeIfAbsent(event.getMetadata().getInternalId(), x -> new ArrayList<>());

                    int foundCount = 0;
                    for (DefaultField field : (List<DefaultField>) event.getFields()) {
                        for (Entry<String,String> fieldValue : expectedFieldsForDoc.entrySet()) {
                            if (field.getName().equals(fieldValue.getKey())) {
                                assertEquals(fieldValue.getValue(), field.getValueString());
                                foundCount++;
                            }
                        }
                        for (String noField : expectedNoFieldsForDoc) {
                            if (field.getName().equals(noField)) {
                                fail("Encountered field which should not have been present in doc: " + event.getMetadata().getInternalId() + " field: "
                                                + noField);
                            }
                        }
                    }

                    assertEquals(expectedFieldsForDoc.size(), foundCount);
                    break;
                }
            }
            assertTrue(found, "field not found " + expectedSet);
        }
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));

        sharedClient = new QueryTestTableHelper(ShardQueryLogicTest.class.toString(), log, RebuildingScannerTestHelper.TEARDOWN.EVERY_OTHER_SANS_CONSISTENCY,
                        RebuildingScannerTestHelper.INTERRUPT.EVERY_OTHER).client;
        WiseGuysIngest.writeItAll(sharedClient, WiseGuysIngest.WhatKindaRange.DOCUMENT);
        PrintUtility.printTable(sharedClient, auths, TableName.SHARD);
        PrintUtility.printTable(sharedClient, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(sharedClient, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
    }

    @AfterAll
    public static void afterAll() {
        TypeRegistry.reset();
    }

    @BeforeEach
    public void setup() {
        this.client = sharedClient;
        setClientForTest(this.client);
        this.logic.setFullTableScanEnabled(true);
        this.logic.setAllHitsQueryConfig(new AllHitsQueryConfig());

        this.annotations.clear();
        this.annotationSources.clear();
        this.expectedFields.clear();
        this.expectNoField.clear();
        this.extraData.clear();
        this.expectedGroups = new HashSet<>();
        this.events = new ArrayList<>();
    }

    private void runTestQuery(Set<Set<String>> expected) throws Exception {
        this.expectedGroups = expected;
        givenDate("20091231", "20150101");
        planAndExecuteQuery();
    }

    @Test
    public void testFieldMappingTransformViaProfile() throws Exception {
        givenQuery("UUID =~ '^[CS].*'");
        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "false");
        givenParameter(QueryParameters.QUERY_PROFILE, "copyFieldEventQuery");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.sopranoUID, "MAGIC_COPY:18"));
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.corleoneUID, "MAGIC_COPY:18"));
        expected.add(Sets.newHashSet("UID:" + caponeUID, "MAGIC_COPY:18"));
        runTestQuery(expected);
    }

    @Test
    public void testRegex() throws Exception {
        givenQuery("UUID=='CAPONE' AND QUOTE=~'.*kind'");
        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");

        Set<Set<String>> expected = new HashSet<>();
        // todo: make this work someday
        // expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));

        runTestQuery(expected);
    }

    @Test
    public void testFwdRegex() throws Exception {
        givenQuery("UUID=='CAPONE' AND QUOTE=~'kin.*'");
        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");

        Set<Set<String>> expected = new HashSet<>();
        // todo: make this work someday
        // expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));

        runTestQuery(expected);
    }

    @Test
    public void testEvalRegex() throws Exception {
        givenQuery("UUID=='CAPONE' AND ((_Eval_ = true) && QUOTE=~'.*alone')");
        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));

        runTestQuery(expected);
    }

    @Test
    public void testNegativeEvalRegex() throws Exception {
        givenQuery("UUID=='CAPONE' AND ((_Eval_ = true) && QUOTE!~'.*alone')");
        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");

        Set<Set<String>> expected = new HashSet<>();
        runTestQuery(expected);
    }

    @Test
    public void testNegativeEvalRegexV2() throws Exception {
        givenQuery("UUID=='CAPONE' AND ((_Eval_ = true) && !(QUOTE=~'.*alone'))");
        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");

        Set<Set<String>> expected = new HashSet<>();
        runTestQuery(expected);
    }

    @Test
    public void testDoubeWildcard() throws Exception {
        givenQuery("UUID=='CAPONE' AND QUOTE=~'.*ind.*'");
        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));

        runTestQuery(expected);
    }

    @Test
    public void testNegativeRegex() throws Exception {
        givenQuery("UUID=='CAPONE' AND QUOTE!~'.*ind'");
        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));

        runTestQuery(expected);
    }

    @Test
    public void testNegativeRegexV2() throws Exception {
        givenQuery("UUID=='CAPONE' AND !(QUOTE=~'.*ind')");
        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));

        runTestQuery(expected);
    }

    @Test
    public void testFilterRegex() throws Exception {
        givenQuery("UUID=='CAPONE' AND filter:includeRegex(QUOTE,'.*kind word alone.*')");
        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));

        runTestQuery(expected);
    }

    @Test
    public void testNegativeFilterRegex() throws Exception {
        givenQuery("UUID=='CAPONE' AND !filter:includeRegex(QUOTE,'.*kind word alone.*')");
        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");

        Set<Set<String>> expected = new HashSet<>();

        runTestQuery(expected);
    }

    @Test
    public void testNegativeFilterRegexV2() throws Exception {
        givenQuery("UUID=='CAPONE' AND !(filter:includeRegex(QUOTE,'.*kind word alone.*'))");
        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");
        Set<Set<String>> expected = new HashSet<>();

        runTestQuery(expected);
    }

    @Test
    public void testExcludeDataTypesBangDataType() throws Exception {
        givenQuery("UUID=='TATTAGLIA'");
        givenParameter(QueryParameters.DATATYPE_FILTER_SET, "!test2");

        Set<Set<String>> expected = new HashSet<>();
        // No results expected
        runTestQuery(expected);
    }

    @Test
    public void testExcludeDataTypesNegateDataType() throws Exception {
        givenQuery("UUID=='TATTAGLIA'");
        givenParameter(QueryParameters.DATATYPE_FILTER_SET, "test2,!test2");

        Set<Set<String>> expected = new HashSet<>();
        // Expect one result, since the negated data type results in empty set, which is treated by Datawave as all data types
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.tattagliaUID));

        runTestQuery(expected);
    }

    @Test
    public void testExcludeDataTypesIncludeOneTypeExcludeOneType() throws Exception {
        givenQuery("UUID=='TATTAGLIA' || UUID=='CAPONE'");
        givenParameter(QueryParameters.DATATYPE_FILTER_SET, "test2,!test");
        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.tattagliaUID));

        runTestQuery(expected);
    }

    @Test
    public void annotationHitsNoAnnotationsTest() throws Exception {
        withAnnotationHits();

        givenQuery("UUID=='CAPONE'");

        expectNoField(caponeUID, "ALL_HITS_RESULTS");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsTransformerReusedAndUpdatedAcrossGetTransformerCallsTest() throws Exception {
        // ShardQueryLogic follows an initialize()/updateConfig() lifecycle contract for its config-based
        // transforms (UniqueTransform, GroupingTransform, FieldRenameTransform, AnnotationHitsTransformer):
        // the first getTransformer() call constructs the transform (calling initialize()), and any subsequent
        // getTransformer() call (e.g. on a later page/next()) must reuse that same instance rather than
        // reconstructing it, updating its configuration in place via updateConfig() instead. Verify that
        // contract holds for AnnotationHitsTransformer specifically, since it was only recently migrated to
        // this pattern (previously it was unconditionally rebuilt on every call).
        withAnnotationHits();
        // min score high enough to filter out the S1 hit below on the first call
        givenParameter(MIN_SCORE_PARAMETER, "0.99");

        givenAnnotation(buildAnnotation(S1));
        givenQuery("UUID=='CAPONE'");
        givenDate("20091231", "20150101");

        setClientForTest(this.client);
        this.logic.setFullTableScanEnabled(true);
        QueryImpl settings = getSettings();
        logic.setMaxEvaluationPipelines(1);
        logic.setHitList(true);

        GenericQueryConfiguration config = logic.initialize(client, settings, Collections.singleton(getAuths()));
        logic.setupQuery(config);

        QueryLogicTransformer firstTransformer = logic.getTransformer(settings);
        DocumentTransform firstAnnotationHitsTransform = ((DocumentTransformer) firstTransformer).containsTransform(AnnotationHitsTransformer.class);
        assertNotNull(firstAnnotationHitsTransform, "AnnotationHitsTransformer should have been constructed on the first getTransformer() call");

        // simulate a later page/next() call lowering the min score, as could legitimately happen across pages
        settings.addParameter(MIN_SCORE_PARAMETER, "0");

        QueryLogicTransformer secondTransformer = logic.getTransformer(settings);
        DocumentTransform secondAnnotationHitsTransform = ((DocumentTransformer) secondTransformer).containsTransform(AnnotationHitsTransformer.class);

        assertSame(firstTransformer, secondTransformer, "the same DocumentTransformer instance should be reused across getTransformer() calls");
        assertSame(firstAnnotationHitsTransform, secondAnnotationHitsTransform,
                        "the same AnnotationHitsTransformer instance should be reused (via updateConfig()) rather than reconstructed");

        logic.close();
    }

    @Test
    public void annotationHitsSingleHitNoContextWindowTest() throws Exception {
        withAnnotationHits();

        givenAnnotation(buildAnnotation(S1));

        givenQuery("UUID=='CAPONE'");

        AnnotationHitsTransformer.SegmentHit hit = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 0);
        hit.setContextEnd(S1.getBoundary());
        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S1, S2, S3, S4, S5, S6, S7, S8, S9);
        AllHits hits = getExpectedAnnotationHits("03AE6355", List.of(hit), context);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits);

        expectField(caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsNotEnabledByQueryParamTest() throws Exception {
        withAnnotationHits();
        // disable the transformer via query param
        givenParameter(ENABLED_PARAMETER, "");

        givenAnnotation(buildAnnotation(S1));

        givenQuery("UUID=='CAPONE'");

        expectNoField(caponeUID, "ALL_HITS_RESULTS");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsNullTermExtractorTest() throws Exception {
        withAnnotationHits();
        logic.getAllHitsQueryConfig().setQueryTermExtractor(null);

        givenAnnotation(buildAnnotation(S1));

        givenQuery("UUID=='CAPONE'");

        expectNoField(caponeUID, "ALL_HITS_RESULTS");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));
        this.expectedGroups = expected;
        givenDate("20091231", "20150101");
        assertThrows(IllegalStateException.class, this::planAndExecuteQuery);
    }

    @Test
    public void annotationHitsNullNormalizerTest() throws Exception {
        withAnnotationHits();
        logic.getAllHitsQueryConfig().setTermNormalizer(null);

        givenAnnotation(buildAnnotation(S1));

        givenQuery("UUID=='CAPONE'");

        expectNoField(caponeUID, "ALL_HITS_RESULTS");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));
        this.expectedGroups = expected;
        givenDate("20091231", "20150101");
        assertThrows(IllegalStateException.class, this::planAndExecuteQuery);
    }

    @Test
    public void annotationHitsSingleHit1ContextWindowTest() throws Exception {
        withAnnotationHits();

        givenAnnotation(buildAnnotation(S1, S2, S6));

        givenQuery("UUID=='CAPONE'");

        AnnotationHitsTransformer.SegmentHit hit = new AnnotationHitsTransformer.SegmentHit(S6.getBoundary(), S1.getBoundary(), 0);
        hit.setContextEnd(S2.getBoundary());
        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S1, S2, S6);
        AllHits hits = getExpectedAnnotationHits("5485ED5D", List.of(hit), context);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits);

        expectField(caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);
        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsSingleHit2ContextWindowTest() throws Exception {
        withAnnotationHits();

        givenAnnotation(buildAnnotation(S1, S2, S3, S6, S7));

        givenQuery("UUID=='CAPONE'");

        AnnotationHitsTransformer.SegmentHit hit = new AnnotationHitsTransformer.SegmentHit(S6.getBoundary(), S1.getBoundary(), 0);
        hit.setContextEnd(S3.getBoundary());
        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S1, S2, S3, S6, S7);
        AllHits hits = getExpectedAnnotationHits("B1E42D02", List.of(hit), context);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits);

        expectField(caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsSingleHit3ContextWindowTest() throws Exception {
        withAnnotationHits();

        givenAnnotation(buildAnnotation(S1, S2, S3, S4, S6, S7, S8));

        givenQuery("UUID=='CAPONE'");

        AnnotationHitsTransformer.SegmentHit hit = new AnnotationHitsTransformer.SegmentHit(S6.getBoundary(), S1.getBoundary(), 0);
        hit.setContextEnd(S4.getBoundary());
        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S1, S2, S3, S4, S6, S7, S8);
        AllHits hits = getExpectedAnnotationHits("2CFF3C2F", List.of(hit), context);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits);

        expectField(caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsSingleHitTest() throws Exception {
        withAnnotationHits();

        givenAnnotation(buildAnnotation(S5, S2, S3, S4, S1));

        givenQuery("UUID=='CAPONE'");

        AnnotationHitsTransformer.SegmentHit hit = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 0);
        hit.setContextEnd(S4.getBoundary());
        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S1, S2, S3, S4, S5, S6, S7, S8, S9);
        AllHits hits = getExpectedAnnotationHits("E9EA0949", List.of(hit), context);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits);

        // omit segment 5 because it is beyond the window
        expectField(caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsSingleHitFullWindowTest() throws Exception {
        withAnnotationHits();
        givenAnnotation(buildAnnotation(S2, S3, S4, S1, S5, S8, S6, S7, S9));

        givenQuery("UUID=='CAPONE'");

        AnnotationHitsTransformer.SegmentHit hit = new AnnotationHitsTransformer.SegmentHit(S7.getBoundary(), S1.getBoundary(), 0);
        hit.setContextEnd(S4.getBoundary());
        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S1, S2, S3, S4, S5, S6, S7, S8, S9);
        AllHits hits = getExpectedAnnotationHits("04798A0E", List.of(hit), context);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits);

        // omit edge segments beyond the window
        expectField(caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsMultiHitSameBoundaryTest() throws Exception {
        withAnnotationHits();
        givenAnnotation(buildAnnotation(S2, S3, S4, S1, S5, S8, S6, S7, S9));

        givenQuery("UUID=='CAPONE' || UUID=='CARL'");

        AnnotationHitsTransformer.SegmentHit hit1 = new AnnotationHitsTransformer.SegmentHit(S7.getBoundary(), S1.getBoundary(), 0);
        hit1.setContextEnd(S4.getBoundary());
        AnnotationHitsTransformer.SegmentHit hit2 = new AnnotationHitsTransformer.SegmentHit(S7.getBoundary(), S1.getBoundary(), 1);
        hit2.setContextEnd(S4.getBoundary());
        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S2, S3, S4, S1, S5, S8, S6, S7, S9);
        AllHits hits = getExpectedAnnotationHits("04798A0E", List.of(hit1, hit2), context);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits);

        expectField(caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsMultiHitTest() throws Exception {
        withAnnotationHits();
        givenAnnotation(buildAnnotation(S2, S3, S4, S1, S5, S8, S6, S7, S9));

        givenQuery("UUID=='CAPONE' || UUID=='w1' || UUID=='d1'");

        AnnotationHitsTransformer.SegmentHit hit1 = new AnnotationHitsTransformer.SegmentHit(S7.getBoundary(), S1.getBoundary(), 0);
        hit1.setContextEnd(S4.getBoundary());
        AnnotationHitsTransformer.SegmentHit hit2 = new AnnotationHitsTransformer.SegmentHit(S6.getBoundary(), S6.getBoundary(), 0);
        hit2.setContextEnd(S9.getBoundary());
        AnnotationHitsTransformer.SegmentHit hit3 = new AnnotationHitsTransformer.SegmentHit(S2.getBoundary(), S5.getBoundary(), 0);
        hit3.setContextEnd(S5.getBoundary());
        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S2, S3, S4, S1, S5, S8, S6, S7, S9);
        AllHits hits = getExpectedAnnotationHits("04798A0E", List.of(hit2, hit1, hit3), context);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits);

        expectField(caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsFromSimpleKeywordParamTest() throws Exception {
        withAnnotationHits();
        givenParameter(KEYWORDS_PARAMETER, "w1;d1");
        givenAnnotation(buildAnnotation(S2, S3, S4, S1, S5, S8, S6, S7, S9));

        givenQuery("UUID=='CAPONE'");

        AnnotationHitsTransformer.SegmentHit hit2 = new AnnotationHitsTransformer.SegmentHit(S6.getBoundary(), S6.getBoundary(), 0);
        hit2.setContextEnd(S9.getBoundary());
        AnnotationHitsTransformer.SegmentHit hit3 = new AnnotationHitsTransformer.SegmentHit(S2.getBoundary(), S5.getBoundary(), 0);
        hit3.setContextEnd(S5.getBoundary());
        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S2, S3, S4, S1, S5, S8, S6, S7, S9);
        AllHits hits = getExpectedAnnotationHits("04798A0E", List.of(hit2, hit3), context);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits);

        expectField(caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsFromJsonKeywordParamTest() throws Exception {
        withAnnotationHits();
        givenParameter(KEYWORDS_PARAMETER, "[\"w1\", \"d1\"]");
        givenAnnotation(buildAnnotation(S2, S3, S4, S1, S5, S8, S6, S7, S9));

        givenQuery("UUID=='CAPONE'");

        AnnotationHitsTransformer.SegmentHit hit2 = new AnnotationHitsTransformer.SegmentHit(S6.getBoundary(), S6.getBoundary(), 0);
        hit2.setContextEnd(S9.getBoundary());
        AnnotationHitsTransformer.SegmentHit hit3 = new AnnotationHitsTransformer.SegmentHit(S2.getBoundary(), S5.getBoundary(), 0);
        hit3.setContextEnd(S5.getBoundary());
        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S2, S3, S4, S1, S5, S8, S6, S7, S9);
        AllHits hits = getExpectedAnnotationHits("04798A0E", List.of(hit2, hit3), context);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits);

        expectField(caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsFromEncodedJsonKeywordParamTest() throws Exception {
        withAnnotationHits();

        givenParameter(KEYWORDS_PARAMETER, URLEncoder.encode("[\"w1\", \"d1\"]", StandardCharsets.UTF_8));
        givenAnnotation(buildAnnotation(S2, S3, S4, S1, S5, S8, S6, S7, S9));

        givenQuery("UUID=='CAPONE'");

        AnnotationHitsTransformer.SegmentHit hit2 = new AnnotationHitsTransformer.SegmentHit(S6.getBoundary(), S6.getBoundary(), 0);
        hit2.setContextEnd(S9.getBoundary());
        AnnotationHitsTransformer.SegmentHit hit3 = new AnnotationHitsTransformer.SegmentHit(S2.getBoundary(), S5.getBoundary(), 0);
        hit3.setContextEnd(S5.getBoundary());
        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S2, S3, S4, S1, S5, S8, S6, S7, S9);
        AllHits hits = getExpectedAnnotationHits("04798A0E", List.of(hit2, hit3), context);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits);

        expectField(caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsMultiHitRestrictedByMinScoreTest() throws Exception {
        withAnnotationHits();
        givenAnnotation(buildAnnotation(S2, S3, S4, S1, S5, S8, S6, S7, S9));

        givenQuery("UUID=='CAPONE' || UUID=='w1' || UUID=='d1'");
        givenParameter(MIN_SCORE_PARAMETER, ".6");

        // eliminates hit 1 because it is lower than .6
        // .9 > .6
        AnnotationHitsTransformer.SegmentHit hit2 = new AnnotationHitsTransformer.SegmentHit(S6.getBoundary(), S6.getBoundary(), 0);
        hit2.setContextEnd(S9.getBoundary());
        // .6 == .6
        AnnotationHitsTransformer.SegmentHit hit3 = new AnnotationHitsTransformer.SegmentHit(S2.getBoundary(), S5.getBoundary(), 0);
        hit3.setContextEnd(S5.getBoundary());
        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S2, S3, S4, S1, S5, S8, S6, S7, S9);
        AllHits hits = getExpectedAnnotationHits("04798A0E", List.of(hit2, hit3), context);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits);

        expectField(caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsMultiHitRestrictedByMinScoreBoundaryTest() throws Exception {
        withAnnotationHits();
        givenAnnotation(buildAnnotation(S2, S3, S4, S1, S5, S8, S6, S7, S9));

        givenQuery("UUID=='CAPONE' || UUID=='w1' || UUID=='d1'");
        givenParameter(MIN_SCORE_PARAMETER, ".61");

        // eliminates hit 1 because it is lower than .6
        // eliminates hit 3 because it is lower than .61
        AnnotationHitsTransformer.SegmentHit hit2 = new AnnotationHitsTransformer.SegmentHit(S6.getBoundary(), S6.getBoundary(), 0);
        hit2.setContextEnd(S9.getBoundary());
        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S2, S3, S4, S1, S5, S8, S6, S7, S9);
        AllHits hits = getExpectedAnnotationHits("04798A0E", List.of(hit2), context);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits);

        expectField(caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationsHitsWithReducedContext() throws Exception {
        withAnnotationHits();

        givenAnnotation(buildAnnotation(S1, S2, S3, S4, S5, S6, S7, S8, S9));

        givenQuery("UUID=='CAPONE'");
        givenParameter(CONTEXT_SIZE_PARAMETER, "1");

        AnnotationHitsTransformer.SegmentHit hit = new AnnotationHitsTransformer.SegmentHit(S9.getBoundary(), S1.getBoundary(), 0);
        hit.setContextEnd(S2.getBoundary());
        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S1, S2, S3, S4, S5, S6, S7, S8, S9);
        AllHits hits = getExpectedAnnotationHits("04798A0E", List.of(hit), context);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits);

        expectField(caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationsHitsWithNegativeContext() throws Exception {
        withAnnotationHits();

        givenAnnotation(buildAnnotation(S1, S2, S3, S4, S5, S6, S7, S8, S9));

        givenQuery("UUID=='CAPONE'");
        givenParameter(CONTEXT_SIZE_PARAMETER, "-1");

        AnnotationHitsTransformer.SegmentHit hit = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 0);
        hit.setContextEnd(S1.getBoundary());
        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S1, S2, S3, S4, S5, S6, S7, S8, S9);
        AllHits hits = getExpectedAnnotationHits("04798A0E", List.of(hit), context);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits);

        expectField(caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationsHitsAboveMaxContext() throws Exception {
        withAnnotationHits();

        givenAnnotation(buildAnnotation(S1, S2, S3, S4, S5, S6, S7, S8, S9));

        givenQuery("UUID=='CAPONE'");
        givenParameter(CONTEXT_SIZE_PARAMETER, "4");

        AnnotationHitsTransformer.SegmentHit hit = new AnnotationHitsTransformer.SegmentHit(S7.getBoundary(), S1.getBoundary(), 0);
        hit.setContextEnd(S4.getBoundary());
        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S1, S2, S3, S4, S5, S6, S7, S8, S9);
        AllHits hits = getExpectedAnnotationHits("04798A0E", List.of(hit), context);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits);

        expectField(caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationsHitsFactoryErrorTest() throws Exception {
        withAnnotationHits();
        logic.getAllHitsQueryConfig().setAllHitsFactoryClass(AllHitsFactoryErrorOnly.class.getCanonicalName());

        givenAnnotation(buildAnnotation(S1, S2, S3, S4, S5, S6, S7, S8, S9));

        givenQuery("UUID=='CAPONE'");

        expectField(caponeUID, "ALL_HITS_RESULTS",
                        "[{\"annotationId\":\"04798A0E\",\"maxTermHitConfidence\":0.0,\"keywordResultList\":[],\"error\":\"test failure\"}]");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsWrongTypeTest() throws Exception {
        withAnnotationHits();
        givenAnnotation(buildAnnotation("ANNO2", "capone", "abc", S1));

        givenQuery("UUID=='CAPONE'");

        expectNoField(caponeUID, "ALL_HITS_RESULTS");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsMultipleAnnotationsTest() throws Exception {
        withAnnotationHits();
        givenAnnotation(buildAnnotation("ANNO1", "capone", "abc", S1));
        givenAnnotation(buildAnnotation("ANNO1", "capone2", "abcd", S1, S2));

        givenQuery("UUID=='CAPONE'");

        AnnotationHitsTransformer.SegmentHit hit1 = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 0);
        hit1.setContextEnd(S1.getBoundary());
        AnnotationHitsTransformer.SegmentHit hit2 = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 0);
        hit2.setContextEnd(S2.getBoundary());

        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S1);
        AllHits hits1 = getExpectedAnnotationHits("03AE6355", List.of(hit1), context);
        context = buildSortedContext(S1, S2);
        AllHits hits2 = getExpectedAnnotationHits("BCC16AC0", List.of(hit2), context);

        String expectedAnnotationHits = getExpectedALlHitsRollup(hits1, hits2);

        expectField(caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsFromWildcardKeywordTest() throws Exception {
        withAnnotationHits();
        givenAnnotation(buildAnnotation("ANNO1", "capone", "abc", S1));

        // capo.* is expanded in query planning to capone
        givenQuery("UUID =~ 'CAPO.*'");

        AnnotationHitsTransformer.SegmentHit hit1 = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 0);
        hit1.setContextEnd(S1.getBoundary());

        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S1);
        AllHits hits1 = getExpectedAnnotationHits("03AE6355", List.of(hit1), context);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits1);

        expectField(caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsFromNonIndexedWildcardKeywordTest() throws Exception {
        withAnnotationHits();

        // create a new segment which would match the original wildcard if it weren't expanded in planning
        Segment wildcard = Segment.newBuilder().addValues(SegmentValue.newBuilder().setValue("cap").setScore(0.4f).build())
                        .addValues(SegmentValue.newBuilder().setValue("ca").setScore(1.0f).build())
                        .setBoundary(SegmentBoundary.newBuilder().setBoundaryType(BoundaryType.TIME_MILLI).setStart(30).setEnd(40).build()).build();
        givenAnnotation(buildAnnotation("ANNO1", "capone", "abc", S1, wildcard));

        givenQuery("UUID =~ 'CAP.*'");

        AnnotationHitsTransformer.SegmentHit hit1 = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 0);
        hit1.setContextEnd(wildcard.getBoundary());

        AnnotationHitsTransformer.SegmentHit hit2 = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), wildcard.getBoundary(), 0);
        hit2.setContextEnd(wildcard.getBoundary());

        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S1, wildcard);
        AllHits hits1 = getExpectedAnnotationHits("62292BD8", List.of(hit1, hit2), context);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits1);

        expectField(caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsTimeRangeInSecondsTest() throws Exception {
        withAnnotationHits();

        givenAnnotation(buildAnnotation("ANNO1", "capone", "abc", S1));

        givenParameter(TIMEUNIT_PARAMETER, TimeUnit.SECONDS.toString());

        givenQuery("UUID=='CAPONE'");

        AnnotationHitsTransformer.SegmentHit hit1 = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 0);
        hit1.setContextEnd(S1.getBoundary());

        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S1);

        AllHitsFactory factory = new AllHitsFactory();
        AllHits hits1 = factory.create("03AE6355", List.of(hit1), context, TimeUnit.SECONDS);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits1);

        expectField(caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsTimeRangeInMicrosTest() throws Exception {
        withAnnotationHits();

        givenAnnotation(buildAnnotation("ANNO1", "capone", "abc", S1));

        givenParameter(TIMEUNIT_PARAMETER, TimeUnit.MICROSECONDS.toString());

        givenQuery("UUID=='CAPONE'");

        AnnotationHitsTransformer.SegmentHit hit1 = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 0);
        hit1.setContextEnd(S1.getBoundary());

        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S1);

        AllHitsFactory factory = new AllHitsFactory();
        AllHits hits1 = factory.create("03AE6355", List.of(hit1), context, TimeUnit.MICROSECONDS);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits1);

        expectField(caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsLuceneUnfieldedTest() throws Exception {
        withAnnotationHits();
        logic.getAllHitsQueryConfig().setQueryTermExtractor(new TermExtractor(Set.of("_ANYFIELD_")));
        disableQueryTreeValidation();

        givenAnnotation(buildAnnotation("ANNO1", "capone", "abc", S1));

        givenParameter(TIMEUNIT_PARAMETER, TimeUnit.MICROSECONDS.toString());
        givenParameter(QueryParameters.QUERY_SYNTAX, "LUCENE");
        givenQuery("CAPONE");

        AnnotationHitsTransformer.SegmentHit hit1 = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 0);
        hit1.setContextEnd(S1.getBoundary());

        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S1);

        AllHitsFactory factory = new AllHitsFactory();
        AllHits hits1 = factory.create("03AE6355", List.of(hit1), context, TimeUnit.MICROSECONDS);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits1);

        expectField(caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsLuceneFieldedTest() throws Exception {
        withAnnotationHits();
        logic.getAllHitsQueryConfig().setQueryTermExtractor(new TermExtractor(Set.of("_ANYFIELD_", "UUID")));
        disableQueryTreeValidation();

        givenAnnotation(buildAnnotation("ANNO1", "capone", "abc", S1));

        givenParameter(TIMEUNIT_PARAMETER, TimeUnit.MICROSECONDS.toString());
        givenParameter(QueryParameters.QUERY_SYNTAX, "LUCENE");
        givenQuery("UUID:CAPONE");

        AnnotationHitsTransformer.SegmentHit hit1 = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 0);
        hit1.setContextEnd(S1.getBoundary());

        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S1);

        AllHitsFactory factory = new AllHitsFactory();
        AllHits hits1 = factory.create("03AE6355", List.of(hit1), context, TimeUnit.MICROSECONDS);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits1);

        expectField(caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsEnrichmentFieldTest() throws Exception {
        withAnnotationHits();
        logic.getAllHitsQueryConfig().getAnnotationEnrichmentFieldMap().put("FAVORITE_FOOD", "favoriteFoods");
        logic.getAllHitsQueryConfig().getAnnotationEnrichmentFieldMap().put("FAVORITE_COLOR", "favoriteColors");

        // make sure the source and annotation match sourceHashes. This is a chicken and egg problem for the test because it isn't pre-stored so have to hard
        // code
        givenAnnotation(buildAnnotation("ANNO1", "CAPONE", "084B04750A629E7F058CC976ECCE2235", S5, S2, S3, S4, S1));
        givenAnnotationSource(AnnotationSource.newBuilder().setEngine("engine").setModel("model").putMetadata("visibility", "ALL")
                        .putMetadata("created_date", "2026-02-04T00:00:00Z").build());

        // add a little data into the event to match on the enrichment field
        givenExtraEventData(new Key("20130101_0", "test" + '\u0000' + caponeUID, "FAVORITE_FOOD.8180F09F.12333.213" + '\u0000' + "ziti",
                        new ColumnVisibility("ALL"), 1356998400000L), new Value());
        givenExtraEventData(new Key("20130101_0", "test" + '\u0000' + caponeUID, "FAVORITE_FOOD.8180F09F.12333.213" + '\u0000' + "meatballs",
                        new ColumnVisibility("ALL"), 1356998400000L), new Value());
        givenExtraEventData(new Key("20130101_0", "test" + '\u0000' + caponeUID, "FAVORITE_FOOD.333.12333.213" + '\u0000' + "turkey",
                        new ColumnVisibility("ALL"), 1356998400000L), new Value());
        givenExtraEventData(
                        new Key("20130101_0", "test" + '\u0000' + caponeUID, "FAVORITE_FOOD" + '\u0000' + "taco", new ColumnVisibility("ALL"), 1356998400000L),
                        new Value());

        givenExtraEventData(new Key("20130101_0", "test" + '\u0000' + caponeUID, "FAVORITE_COLOR.8180F09F.12333.213" + '\u0000' + "purple",
                        new ColumnVisibility("ALL"), 1356998400000L), new Value());
        givenExtraEventData(new Key("20130101_0", "test" + '\u0000' + caponeUID, "FAVORITE_COLOR.222.12333.213" + '\u0000' + "orange",
                        new ColumnVisibility("ALL"), 1356998400000L), new Value());
        givenExtraEventData(new Key("20130101_0", "test" + '\u0000' + caponeUID, "FAVORITE_COLOR" + '\u0000' + "yellow", new ColumnVisibility("ALL"),
                        1356998400000L), new Value());

        givenQuery("UUID=='CAPONE'");

        AnnotationHitsTransformer.SegmentHit hit = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 0);
        hit.setContextEnd(S4.getBoundary());
        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S1, S2, S3, S4, S5, S6, S7, S8, S9);
        AllHits hits = getExpectedAnnotationHits("E9EA0949", List.of(hit), context);
        hits.addDynamicProperties("favoriteFoods", "meatballs;ziti");
        hits.addDynamicProperties("favoriteColors", "purple");
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits);

        // omit segment 5 because it is beyond the window
        expectField(caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));
        runTestQuery(expected);
    }

    private void disableQueryTreeValidation() {
        TimedVisitorManager visitorManager = ((DefaultQueryPlanner) logic.getQueryPlanner()).getVisitorManager();
        visitorManager.setValidateAst(false);
    }

    private void addExtraEventData(AccumuloClient client) {
        try {
            BatchWriter bw = client.createBatchWriter("shard");

            for (String row : extraData.keySet()) {
                Mutation m = new Mutation(row);
                for (Entry<Key,Value> kv : extraData.get(row)) {
                    Key key = kv.getKey();
                    m.put(key.getColumnFamily(), key.getColumnQualifier(), key.getColumnVisibilityParsed(), key.getTimestamp(), kv.getValue());
                }
                bw.addMutation(m);
            }
            bw.flush();
            bw.close();
        } catch (TableNotFoundException | MutationsRejectedException e) {
            throw new RuntimeException(e);
        }
    }

    private void setupAnnotationsTables(AccumuloClient client) {
        try {
            // drop existing tables if they exist
            client.tableOperations().delete("annotation");
        } catch (AccumuloException | TableNotFoundException | AccumuloSecurityException e) {
            // no-op
        }

        try {
            client.tableOperations().delete("annotationSource");
        } catch (AccumuloException | TableNotFoundException | AccumuloSecurityException e) {
            // no-op
        }

        try {
            // create annotations tables
            client.tableOperations().create("annotation");
            client.tableOperations().create("annotationSource");

            AnnotationSerializer<Iterator<Entry<Key,Value>>,Annotation> serializer = new AccumuloAnnotationSerializer();
            AccumuloAnnotationSourceSerializer sourceSerializer = new AccumuloAnnotationSourceSerializer();
            AnnotationDataAccess dataAccess = new AnnotationDataAccess(client, authSet, "annotation", "annotationSource", serializer, sourceSerializer);
            for (Annotation annotation : annotations) {
                dataAccess.addAnnotation(annotation);
            }
            for (AnnotationSource annotationSource : annotationSources) {
                dataAccess.addAnnotationSource(annotationSource);
            }
        } catch (AccumuloException | AccumuloSecurityException | TableExistsException e) {
            throw new RuntimeException(e);
        }
    }

    private void withAnnotationHits() {
        logic.setAllHitsQueryConfig(new AllHitsQueryConfig());
        logic.getAllHitsQueryConfig().setEnabled(true);
        logic.getAllHitsQueryConfig().setMaxContextLength(3);
        logic.getAllHitsQueryConfig().setTargetField("ALL_HITS_RESULTS");
        logic.getAllHitsQueryConfig().setValidAnnotationTypes(Set.of("ANNO1"));
        logic.getAllHitsQueryConfig().setQueryTermExtractor(new TermExtractor(Set.of("FOO", "BAR", "UUID")));
        logic.getAllHitsQueryConfig().setTermNormalizer(new LcNoDiacriticsNormalizer());
        logic.getAllHitsQueryConfig().setAnnotationConfig(new AnnotationConfig());
        logic.getAllHitsQueryConfig().getAnnotationConfig().setAnnotationTableName("annotation");
        logic.getAllHitsQueryConfig().getAnnotationConfig().setAnnotationSourceTableName("annotationSource");
        logic.getAllHitsQueryConfig().getAnnotationConfig().setTimestampTransformer(new DefaultTimestampTransformer());
        logic.getAllHitsQueryConfig().getAnnotationConfig().setVisibilityTransformer(new DefaultVisibilityTransformer());
        givenParameter(ENABLED_PARAMETER, "true");
    }

    private AllHits getExpectedAnnotationHits(String annotationId, List<AnnotationHitsTransformer.SegmentHit> sortedHits,
                    TreeMap<SegmentBoundary,List<SegmentValue>> context) throws AllHitsException {
        AllHitsFactory factory = new AllHitsFactory();
        return factory.create(annotationId, sortedHits, context, TimeUnit.MILLISECONDS);
    }

    private String getExpectedALlHitsRollup(AllHits... hits) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(hits);
    }

    private TreeMap<SegmentBoundary,List<SegmentValue>> buildSortedContext(Segment... segments) {
        TreeMap<SegmentBoundary,List<SegmentValue>> sortedContext = new TreeMap<>(new BoundaryComparator());
        for (Segment segment : segments) {
            SegmentBoundary boundary = segment.getBoundary();
            List<SegmentValue> values = segment.getValuesList();
            List<SegmentValue> sortedValues = new ArrayList<>(values);
            Collections.sort(sortedValues, new SegmentValueByScoreComparator());
            sortedContext.put(boundary, sortedValues);
        }

        return sortedContext;
    }

    private Annotation buildAnnotation(String annotationType, String documentId, String sourceHash, Segment... segments) {
        // @formatter:off
        return Annotation.newBuilder()
                .setShard("20130101_0")
                .setDataType("test")
                .setUid(caponeUID)
                .setAnnotationType(annotationType)
                .setDocumentId(documentId)
                .setAnalyticSourceHash(sourceHash)
                .putAllMetadata(Map.of("visibility", "ALL", "created_date", "2025-12-29T00:00:00Z"))
                .addAllSegments(List.of(segments))
                .build();
        // @formatter:on
    }

    private Annotation buildAnnotation(Segment... segments) {
        return buildAnnotation("ANNO1", "CAPONE", "abc", segments);
    }

    private void givenAnnotation(Annotation annotation) {
        annotations.add(annotation);
    }

    private void givenAnnotationSource(AnnotationSource annotationSource) {
        annotationSources.add(annotationSource);
    }

    private void givenExtraEventData(Key key, Value value) {
        List<Entry<Key,Value>> existing = extraData.computeIfAbsent(key.getRow().toString(), x -> new ArrayList<>());
        existing.add(Map.entry(key, value));
    }

    private void expectNoField(String id, String field) {
        List<String> noFields = expectNoField.computeIfAbsent(id, x -> new ArrayList<>());
        noFields.add(field);
    }

    private void expectField(String id, String field, String value) {
        Map<String,String> fieldMap = expectedFields.computeIfAbsent(id, x -> new HashMap<>());
        fieldMap.put(field, value);
    }
}
