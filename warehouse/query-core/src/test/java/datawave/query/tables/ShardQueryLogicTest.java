package datawave.query.tables;

import static datawave.query.transformer.annotation.AnnotationHitsTransformer.CONTEXT_SIZE_PARAMETER;
import static datawave.query.transformer.annotation.AnnotationHitsTransformer.ENABLED_PARAMETER;
import static datawave.query.transformer.annotation.AnnotationHitsTransformer.KEYWORDS_PARAMETER;
import static datawave.query.transformer.annotation.AnnotationHitsTransformer.MIN_SCORE_PARAMETER;
import static datawave.query.transformer.annotation.AnnotationHitsTransformer.TIMEUNIT_PARAMETER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.log4j.Logger;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

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
import datawave.annotation.protobuf.v1.BoundaryType;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.protobuf.v1.SegmentBoundary;
import datawave.annotation.protobuf.v1.SegmentValue;
import datawave.configuration.spring.SpringBean;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.iterator.DatawaveTransformIterator;
import datawave.data.normalizer.LcNoDiacriticsNormalizer;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.QueryParameters;
import datawave.query.QueryTestTableHelper;
import datawave.query.RebuildingScannerTestHelper;
import datawave.query.config.annotation.AllHitsQueryConfig;
import datawave.query.config.annotation.AnnotationConfig;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.planner.TimedVisitorManager;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.transformer.DocumentTransformer;
import datawave.query.transformer.annotation.AllHitsException;
import datawave.query.transformer.annotation.AllHitsFactory;
import datawave.query.transformer.annotation.AllHitsFactoryErrorOnly;
import datawave.query.transformer.annotation.AnnotationHitsTransformer;
import datawave.query.transformer.annotation.BoundaryComparator;
import datawave.query.transformer.annotation.SegmentValueByScoreComparator;
import datawave.query.transformer.annotation.TermExtractor;
import datawave.query.transformer.annotation.model.AllHits;
import datawave.query.util.WiseGuysIngest;
import datawave.util.TableName;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;
import datawave.webservice.query.result.event.DefaultField;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.DefaultEventQueryResponse;

public abstract class ShardQueryLogicTest {

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

    @Inject
    @SpringBean(name = "EventQuery")
    protected ShardQueryLogic logic;
    protected KryoDocumentDeserializer deserializer;

    private final DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
    private final Map<String,String> queryParameters = new HashMap<>();

    private String query;
    private Date startDate;
    private Date endDate;

    private List<Annotation> annotations = new ArrayList<>();
    private Map<String,Map<String,String>> expectedFields = new HashMap<>();
    private Map<String,List<String>> expectNoField = new HashMap<>();

    protected abstract String getRange();

    @RunWith(Arquillian.class)
    public static class ShardRange extends ShardQueryLogicTest {

        @Override
        protected String getRange() {
            return WiseGuysIngest.WhatKindaRange.SHARD.name();
        }

        @Before
        public void setup() {
            super.setup();
            logic.setCollapseUids(true);
        }
    }

    @RunWith(Arquillian.class)
    public static class DocumentRange extends ShardQueryLogicTest {

        @Override
        protected String getRange() {
            return WiseGuysIngest.WhatKindaRange.DOCUMENT.name();
        }

        @Before
        public void setup() {
            super.setup();
            logic.setCollapseUids(false);
        }
    }

    @Deployment
    public static JavaArchive createDeployment() throws Exception {
        return ShrinkWrap.create(JavaArchive.class)
                        .addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi", "datawave.query", "org.jboss.logging",
                                        "datawave.webservice.query.result.event")
                        .deleteClass(DefaultEdgeEventQueryLogic.class).deleteClass(RemoteEdgeDictionary.class)
                        .deleteClass(datawave.query.metrics.QueryMetricQueryLogic.class)
                        .addAsManifestResource(new StringAsset(
                                        "<alternatives>" + "<stereotype>datawave.query.tables.edge.MockAlternative</stereotype>" + "</alternatives>"),
                                        "beans.xml");
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
    }

    @AfterClass
    public static void afterClass() throws Exception {
        TypeRegistry.reset();
    }

    @Before
    public void setup() {
        this.logic.setFullTableScanEnabled(true);
        this.deserializer = new KryoDocumentDeserializer();
        this.expectedFields = new HashMap<>();
        this.expectNoField = new HashMap<>();
        this.annotations = new ArrayList<>();
    }

    @After
    public void tearDown() throws Exception {
        this.logic = null;
        this.query = null;
        this.queryParameters.clear();
        this.startDate = null;
        this.endDate = null;
    }

    private AccumuloClient createClient() throws Exception {
        AccumuloClient client = new QueryTestTableHelper(ShardRange.class.toString(), log, RebuildingScannerTestHelper.TEARDOWN.EVERY_OTHER_SANS_CONSISTENCY,
                        RebuildingScannerTestHelper.INTERRUPT.EVERY_OTHER).client;
        WiseGuysIngest.writeItAll(client, WiseGuysIngest.WhatKindaRange.valueOf(getRange()));
        PrintUtility.printTable(client, auths, TableName.SHARD);
        PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        return client;
    }

    private Query createSettings() {
        QueryImpl settings = new QueryImpl();
        settings.setBeginDate(this.startDate);
        settings.setEndDate(this.endDate);
        settings.setPagesize(Integer.MAX_VALUE);
        settings.setQueryAuthorizations(auths.serialize());
        settings.setQuery(this.query);
        settings.setParameters(this.queryParameters);
        settings.setId(UUID.randomUUID());
        return settings;
    }

    protected void runTestQuery(Set<Set<String>> expected) throws Exception {
        log.debug("runTestQuery");

        Query settings = createSettings();
        log.debug("query: " + settings.getQuery());
        log.debug("logic: " + settings.getQueryLogicName());

        AccumuloClient client = createClient();
        setupAnnotationsTables(client);
        GenericQueryConfiguration config = logic.initialize(client, settings, authSet);
        logic.setupQuery(config);

        DocumentTransformer transformer = (DocumentTransformer) (logic.getTransformer(settings));
        TransformIterator iter = new DatawaveTransformIterator(logic.iterator(), transformer);
        List<Object> eventList = new ArrayList<>();
        while (iter.hasNext()) {
            eventList.add(iter.next());
        }

        BaseQueryResponse response = transformer.createResponse(eventList);

        // un-comment to look at the json output
        // ObjectMapper mapper = new ObjectMapper();
        // mapper.enable(MapperFeature.USE_WRAPPER_NAME_AS_PROPERTY_NAME);
        // mapper.writeValue(new File("/tmp/grouped2.json"), response);

        assertTrue(response instanceof DefaultEventQueryResponse);
        DefaultEventQueryResponse eventQueryResponse = (DefaultEventQueryResponse) response;

        if (expected.isEmpty()) {
            assertTrue(eventQueryResponse.getEvents() == null || eventQueryResponse.getEvents().isEmpty());
        } else {
            for (Iterator<Set<String>> it = expected.iterator(); it.hasNext();) {
                Set<String> expectedSet = it.next();
                boolean found = false;

                for (EventBase event : eventQueryResponse.getEvents()) {
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
                            for (Map.Entry<String,String> fieldValue : expectedFieldsForDoc.entrySet()) {
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
                assertTrue("field not found " + expectedSet, found);
            }
        }
    }

    @Test
    public void testFieldMappingTransformViaProfile() throws Exception {
        givenQuery("UUID =~ '^[CS].*'");
        givenQueryParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "false");
        givenQueryParameter(QueryParameters.QUERY_PROFILE, "copyFieldEventQuery");
        givenStartDate("20091231");
        givenEndDate("20150101");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.sopranoUID, "MAGIC_COPY:18"));
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.corleoneUID, "MAGIC_COPY:18"));
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID, "MAGIC_COPY:18"));
        runTestQuery(expected);
    }

    @Test
    public void testRegex() throws Exception {
        givenQuery("UUID=='CAPONE' AND QUOTE=~'.*kind'");
        givenQueryParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");
        givenStartDate("20091231");
        givenEndDate("20150101");

        Set<Set<String>> expected = new HashSet<>();
        // todo: make this work someday
        // expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));

        runTestQuery(expected);
    }

    @Test
    public void testFwdRegex() throws Exception {
        givenQuery("UUID=='CAPONE' AND QUOTE=~'kin.*'");
        givenQueryParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");
        givenStartDate("20091231");
        givenEndDate("20150101");

        Set<Set<String>> expected = new HashSet<>();
        // todo: make this work someday
        // expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));

        runTestQuery(expected);
    }

    @Test
    public void testEvalRegex() throws Exception {
        givenQuery("UUID=='CAPONE' AND ((_Eval_ = true) && QUOTE=~'.*alone')");
        givenQueryParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");
        givenStartDate("20091231");
        givenEndDate("20150101");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));

        runTestQuery(expected);
    }

    @Test
    public void testNegativeEvalRegex() throws Exception {
        givenQuery("UUID=='CAPONE' AND ((_Eval_ = true) && QUOTE!~'.*alone')");
        givenQueryParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");
        givenStartDate("20091231");
        givenEndDate("20150101");

        Set<Set<String>> expected = new HashSet<>();
        runTestQuery(expected);
    }

    @Test
    public void testNegativeEvalRegexV2() throws Exception {
        givenQuery("UUID=='CAPONE' AND ((_Eval_ = true) && !(QUOTE=~'.*alone'))");
        givenQueryParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");
        givenStartDate("20091231");
        givenEndDate("20150101");

        Set<Set<String>> expected = new HashSet<>();
        runTestQuery(expected);
    }

    @Test
    public void testDoubeWildcard() throws Exception {
        givenQuery("UUID=='CAPONE' AND QUOTE=~'.*ind.*'");
        givenQueryParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");
        givenStartDate("20091231");
        givenEndDate("20150101");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));

        runTestQuery(expected);
    }

    @Test
    public void testNegativeRegex() throws Exception {
        givenQuery("UUID=='CAPONE' AND QUOTE!~'.*ind'");
        givenQueryParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");
        givenStartDate("20091231");
        givenEndDate("20150101");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));

        runTestQuery(expected);
    }

    @Test
    public void testNegativeRegexV2() throws Exception {
        givenQuery("UUID=='CAPONE' AND !(QUOTE=~'.*ind')");
        givenQueryParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");
        givenStartDate("20091231");
        givenEndDate("20150101");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));

        runTestQuery(expected);
    }

    @Test
    public void testFilterRegex() throws Exception {
        givenQuery("UUID=='CAPONE' AND filter:includeRegex(QUOTE,'.*kind word alone.*')");
        givenQueryParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");
        givenStartDate("20091231");
        givenEndDate("20150101");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));

        runTestQuery(expected);
    }

    @Test
    public void testNegativeFilterRegex() throws Exception {
        givenQuery("UUID=='CAPONE' AND !filter:includeRegex(QUOTE,'.*kind word alone.*')");
        givenQueryParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");
        givenStartDate("20091231");
        givenEndDate("20150101");

        String queryString = "UUID=='CAPONE' AND !filter:includeRegex(QUOTE,'.*kind word alone.*')";
        Set<Set<String>> expected = new HashSet<>();

        runTestQuery(expected);
    }

    @Test
    public void testNegativeFilterRegexV2() throws Exception {
        givenQuery("UUID=='CAPONE' AND !(filter:includeRegex(QUOTE,'.*kind word alone.*'))");
        givenQueryParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");
        givenStartDate("20091231");
        givenEndDate("20150101");
        Set<Set<String>> expected = new HashSet<>();

        runTestQuery(expected);
    }

    @Test
    public void testExcludeDataTypesBangDataType() throws Exception {
        givenQuery("UUID=='TATTAGLIA'");
        givenQueryParameter(QueryParameters.DATATYPE_FILTER_SET, "!test2");
        givenStartDate("20091231");
        givenEndDate("20150101");

        Set<Set<String>> expected = new HashSet<>();
        // No results expected
        runTestQuery(expected);
    }

    @Test
    public void testExcludeDataTypesNegateDataType() throws Exception {
        givenQuery("UUID=='TATTAGLIA'");
        givenQueryParameter(QueryParameters.DATATYPE_FILTER_SET, "test2,!test2");
        givenStartDate("20091231");
        givenEndDate("20150101");

        Set<Set<String>> expected = new HashSet<>();
        // Expect one result, since the negated data type results in empty set, which is treated by Datawave as all data types
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.tattagliaUID));

        runTestQuery(expected);
    }

    @Test
    public void testExcludeDataTypesIncludeOneTypeExcludeOneType() throws Exception {
        givenQuery("UUID=='TATTAGLIA' || UUID=='CAPONE'");
        givenQueryParameter(QueryParameters.DATATYPE_FILTER_SET, "test2,!test");
        givenStartDate("20091231");
        givenEndDate("20150101");
        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.tattagliaUID));

        runTestQuery(expected);
    }

    @Test
    public void annotationHitsNoAnnotationsTest() throws Exception {
        withAnnotationHits();

        givenQuery("UUID=='CAPONE'");
        givenStartDate("20091231");
        givenEndDate("20150101");

        expectNoField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsSingleHitNoContextWindowTest() throws Exception {
        withAnnotationHits();

        givenAnnotation(buildAnnotation(S1));

        givenQuery("UUID=='CAPONE'");
        givenStartDate("20091231");
        givenEndDate("20150101");

        AnnotationHitsTransformer.SegmentHit hit = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 0);
        hit.setContextEnd(S1.getBoundary());
        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S1, S2, S3, S4, S5, S6, S7, S8, S9);
        AllHits hits = getExpectedAnnotationHits("03AE6355", List.of(hit), context);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits);

        expectField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsNotEnabledByQueryParamTest() throws Exception {
        withAnnotationHits();
        // disable the transformer via query param
        givenQueryParameter(ENABLED_PARAMETER, "");

        givenAnnotation(buildAnnotation(S1));

        givenQuery("UUID=='CAPONE'");
        givenStartDate("20091231");
        givenEndDate("20150101");

        expectNoField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    @Test(expected = IllegalStateException.class)
    public void annotationHitsNullTermExtractorTest() throws Exception {
        withAnnotationHits();
        logic.getAllHitsQueryConfig().setQueryTermExtractor(null);

        givenAnnotation(buildAnnotation(S1));

        givenQuery("UUID=='CAPONE'");
        givenStartDate("20091231");
        givenEndDate("20150101");

        expectNoField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    @Test(expected = IllegalStateException.class)
    public void annotationHitsNullNormalizerTest() throws Exception {
        withAnnotationHits();
        logic.getAllHitsQueryConfig().setTermNormalizer(null);

        givenAnnotation(buildAnnotation(S1));

        givenQuery("UUID=='CAPONE'");
        givenStartDate("20091231");
        givenEndDate("20150101");

        expectNoField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsSingleHit1ContextWindowTest() throws Exception {
        withAnnotationHits();

        givenAnnotation(buildAnnotation(S1, S2, S6));

        givenQuery("UUID=='CAPONE'");
        givenStartDate("20091231");
        givenEndDate("20150101");

        AnnotationHitsTransformer.SegmentHit hit = new AnnotationHitsTransformer.SegmentHit(S6.getBoundary(), S1.getBoundary(), 0);
        hit.setContextEnd(S2.getBoundary());
        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S1, S2, S6);
        AllHits hits = getExpectedAnnotationHits("565A3AED", List.of(hit), context);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits);

        expectField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);
        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsSingleHit2ContextWindowTest() throws Exception {
        withAnnotationHits();

        givenAnnotation(buildAnnotation(S1, S2, S3, S6, S7));

        givenQuery("UUID=='CAPONE'");
        givenStartDate("20091231");
        givenEndDate("20150101");

        AnnotationHitsTransformer.SegmentHit hit = new AnnotationHitsTransformer.SegmentHit(S6.getBoundary(), S1.getBoundary(), 0);
        hit.setContextEnd(S3.getBoundary());
        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S1, S2, S3, S6, S7);
        AllHits hits = getExpectedAnnotationHits("1D6AAA19", List.of(hit), context);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits);

        expectField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsSingleHit3ContextWindowTest() throws Exception {
        withAnnotationHits();

        givenAnnotation(buildAnnotation(S1, S2, S3, S4, S6, S7, S8));

        givenQuery("UUID=='CAPONE'");
        givenStartDate("20091231");
        givenEndDate("20150101");

        AnnotationHitsTransformer.SegmentHit hit = new AnnotationHitsTransformer.SegmentHit(S6.getBoundary(), S1.getBoundary(), 0);
        hit.setContextEnd(S4.getBoundary());
        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S1, S2, S3, S4, S6, S7, S8);
        AllHits hits = getExpectedAnnotationHits("816EDD11", List.of(hit), context);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits);

        expectField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsSingleHitTest() throws Exception {
        withAnnotationHits();

        givenAnnotation(buildAnnotation(S5, S2, S3, S4, S1));

        givenQuery("UUID=='CAPONE'");
        givenStartDate("20091231");
        givenEndDate("20150101");

        AnnotationHitsTransformer.SegmentHit hit = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 0);
        hit.setContextEnd(S4.getBoundary());
        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S1, S2, S3, S4, S5, S6, S7, S8, S9);
        AllHits hits = getExpectedAnnotationHits("8382B062", List.of(hit), context);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits);

        // omit segment 5 because it is beyond the window
        expectField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsSingleHitFullWindowTest() throws Exception {
        withAnnotationHits();
        givenAnnotation(buildAnnotation(S2, S3, S4, S1, S5, S8, S6, S7, S9));

        givenQuery("UUID=='CAPONE'");
        givenStartDate("20091231");
        givenEndDate("20150101");

        AnnotationHitsTransformer.SegmentHit hit = new AnnotationHitsTransformer.SegmentHit(S7.getBoundary(), S1.getBoundary(), 0);
        hit.setContextEnd(S4.getBoundary());
        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S1, S2, S3, S4, S5, S6, S7, S8, S9);
        AllHits hits = getExpectedAnnotationHits("40AD77A3", List.of(hit), context);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits);

        // omit edge segments beyond the window
        expectField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsMultiHitSameBoundaryTest() throws Exception {
        withAnnotationHits();
        givenAnnotation(buildAnnotation(S2, S3, S4, S1, S5, S8, S6, S7, S9));

        givenQuery("UUID=='CAPONE' || UUID=='CARL'");
        givenStartDate("20091231");
        givenEndDate("20150101");

        AnnotationHitsTransformer.SegmentHit hit1 = new AnnotationHitsTransformer.SegmentHit(S7.getBoundary(), S1.getBoundary(), 0);
        hit1.setContextEnd(S4.getBoundary());
        AnnotationHitsTransformer.SegmentHit hit2 = new AnnotationHitsTransformer.SegmentHit(S7.getBoundary(), S1.getBoundary(), 1);
        hit2.setContextEnd(S4.getBoundary());
        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S2, S3, S4, S1, S5, S8, S6, S7, S9);
        AllHits hits = getExpectedAnnotationHits("40AD77A3", List.of(hit1, hit2), context);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits);

        expectField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsMultiHitTest() throws Exception {
        withAnnotationHits();
        givenAnnotation(buildAnnotation(S2, S3, S4, S1, S5, S8, S6, S7, S9));

        givenQuery("UUID=='CAPONE' || UUID=='w1' || UUID=='d1'");
        givenStartDate("20091231");
        givenEndDate("20150101");

        AnnotationHitsTransformer.SegmentHit hit1 = new AnnotationHitsTransformer.SegmentHit(S7.getBoundary(), S1.getBoundary(), 0);
        hit1.setContextEnd(S4.getBoundary());
        AnnotationHitsTransformer.SegmentHit hit2 = new AnnotationHitsTransformer.SegmentHit(S6.getBoundary(), S6.getBoundary(), 0);
        hit2.setContextEnd(S9.getBoundary());
        AnnotationHitsTransformer.SegmentHit hit3 = new AnnotationHitsTransformer.SegmentHit(S2.getBoundary(), S5.getBoundary(), 0);
        hit3.setContextEnd(S5.getBoundary());
        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S2, S3, S4, S1, S5, S8, S6, S7, S9);
        AllHits hits = getExpectedAnnotationHits("40AD77A3", List.of(hit2, hit1, hit3), context);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits);

        expectField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsFromSimpleKeywordParamTest() throws Exception {
        withAnnotationHits();
        givenQueryParameter(KEYWORDS_PARAMETER, "w1;d1");
        givenAnnotation(buildAnnotation(S2, S3, S4, S1, S5, S8, S6, S7, S9));

        givenQuery("UUID=='CAPONE'");
        givenStartDate("20091231");
        givenEndDate("20150101");

        AnnotationHitsTransformer.SegmentHit hit2 = new AnnotationHitsTransformer.SegmentHit(S6.getBoundary(), S6.getBoundary(), 0);
        hit2.setContextEnd(S9.getBoundary());
        AnnotationHitsTransformer.SegmentHit hit3 = new AnnotationHitsTransformer.SegmentHit(S2.getBoundary(), S5.getBoundary(), 0);
        hit3.setContextEnd(S5.getBoundary());
        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S2, S3, S4, S1, S5, S8, S6, S7, S9);
        AllHits hits = getExpectedAnnotationHits("40AD77A3", List.of(hit2, hit3), context);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits);

        expectField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsFromJsonKeywordParamTest() throws Exception {
        withAnnotationHits();
        givenQueryParameter(KEYWORDS_PARAMETER, "[\"w1\", \"d1\"]");
        givenAnnotation(buildAnnotation(S2, S3, S4, S1, S5, S8, S6, S7, S9));

        givenQuery("UUID=='CAPONE'");
        givenStartDate("20091231");
        givenEndDate("20150101");

        AnnotationHitsTransformer.SegmentHit hit2 = new AnnotationHitsTransformer.SegmentHit(S6.getBoundary(), S6.getBoundary(), 0);
        hit2.setContextEnd(S9.getBoundary());
        AnnotationHitsTransformer.SegmentHit hit3 = new AnnotationHitsTransformer.SegmentHit(S2.getBoundary(), S5.getBoundary(), 0);
        hit3.setContextEnd(S5.getBoundary());
        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S2, S3, S4, S1, S5, S8, S6, S7, S9);
        AllHits hits = getExpectedAnnotationHits("40AD77A3", List.of(hit2, hit3), context);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits);

        expectField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsFromEncodedJsonKeywordParamTest() throws Exception {
        withAnnotationHits();

        givenQueryParameter(KEYWORDS_PARAMETER, URLEncoder.encode("[\"w1\", \"d1\"]", StandardCharsets.UTF_8));
        givenAnnotation(buildAnnotation(S2, S3, S4, S1, S5, S8, S6, S7, S9));

        givenQuery("UUID=='CAPONE'");
        givenStartDate("20091231");
        givenEndDate("20150101");

        AnnotationHitsTransformer.SegmentHit hit2 = new AnnotationHitsTransformer.SegmentHit(S6.getBoundary(), S6.getBoundary(), 0);
        hit2.setContextEnd(S9.getBoundary());
        AnnotationHitsTransformer.SegmentHit hit3 = new AnnotationHitsTransformer.SegmentHit(S2.getBoundary(), S5.getBoundary(), 0);
        hit3.setContextEnd(S5.getBoundary());
        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S2, S3, S4, S1, S5, S8, S6, S7, S9);
        AllHits hits = getExpectedAnnotationHits("40AD77A3", List.of(hit2, hit3), context);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits);

        expectField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsMultiHitRestrictedByMinScoreTest() throws Exception {
        withAnnotationHits();
        givenAnnotation(buildAnnotation(S2, S3, S4, S1, S5, S8, S6, S7, S9));

        givenQuery("UUID=='CAPONE' || UUID=='w1' || UUID=='d1'");
        givenStartDate("20091231");
        givenEndDate("20150101");
        givenQueryParameter(MIN_SCORE_PARAMETER, ".6");

        // eliminates hit 1 because it is lower than .6
        // .9 > .6
        AnnotationHitsTransformer.SegmentHit hit2 = new AnnotationHitsTransformer.SegmentHit(S6.getBoundary(), S6.getBoundary(), 0);
        hit2.setContextEnd(S9.getBoundary());
        // .6 == .6
        AnnotationHitsTransformer.SegmentHit hit3 = new AnnotationHitsTransformer.SegmentHit(S2.getBoundary(), S5.getBoundary(), 0);
        hit3.setContextEnd(S5.getBoundary());
        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S2, S3, S4, S1, S5, S8, S6, S7, S9);
        AllHits hits = getExpectedAnnotationHits("40AD77A3", List.of(hit2, hit3), context);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits);

        expectField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsMultiHitRestrictedByMinScoreBoundaryTest() throws Exception {
        withAnnotationHits();
        givenAnnotation(buildAnnotation(S2, S3, S4, S1, S5, S8, S6, S7, S9));

        givenQuery("UUID=='CAPONE' || UUID=='w1' || UUID=='d1'");
        givenStartDate("20091231");
        givenEndDate("20150101");
        givenQueryParameter(MIN_SCORE_PARAMETER, ".61");

        // eliminates hit 1 because it is lower than .6
        // eliminates hit 3 because it is lower than .61
        AnnotationHitsTransformer.SegmentHit hit2 = new AnnotationHitsTransformer.SegmentHit(S6.getBoundary(), S6.getBoundary(), 0);
        hit2.setContextEnd(S9.getBoundary());
        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S2, S3, S4, S1, S5, S8, S6, S7, S9);
        AllHits hits = getExpectedAnnotationHits("40AD77A3", List.of(hit2), context);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits);

        expectField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationsHitsWithReducedContext() throws Exception {
        withAnnotationHits();

        givenAnnotation(buildAnnotation(S1, S2, S3, S4, S5, S6, S7, S8, S9));

        givenQuery("UUID=='CAPONE'");
        givenStartDate("20091231");
        givenEndDate("20150101");
        givenQueryParameter(CONTEXT_SIZE_PARAMETER, "1");

        AnnotationHitsTransformer.SegmentHit hit = new AnnotationHitsTransformer.SegmentHit(S9.getBoundary(), S1.getBoundary(), 0);
        hit.setContextEnd(S2.getBoundary());
        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S1, S2, S3, S4, S5, S6, S7, S8, S9);
        AllHits hits = getExpectedAnnotationHits("71D4C8BE", List.of(hit), context);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits);

        expectField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationsHitsWithNegativeContext() throws Exception {
        withAnnotationHits();

        givenAnnotation(buildAnnotation(S1, S2, S3, S4, S5, S6, S7, S8, S9));

        givenQuery("UUID=='CAPONE'");
        givenStartDate("20091231");
        givenEndDate("20150101");
        givenQueryParameter(CONTEXT_SIZE_PARAMETER, "-1");

        AnnotationHitsTransformer.SegmentHit hit = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 0);
        hit.setContextEnd(S1.getBoundary());
        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S1, S2, S3, S4, S5, S6, S7, S8, S9);
        AllHits hits = getExpectedAnnotationHits("71D4C8BE", List.of(hit), context);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits);

        expectField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationsHitsAboveMaxContext() throws Exception {
        withAnnotationHits();

        givenAnnotation(buildAnnotation(S1, S2, S3, S4, S5, S6, S7, S8, S9));

        givenQuery("UUID=='CAPONE'");
        givenStartDate("20091231");
        givenEndDate("20150101");
        givenQueryParameter(CONTEXT_SIZE_PARAMETER, "4");

        AnnotationHitsTransformer.SegmentHit hit = new AnnotationHitsTransformer.SegmentHit(S7.getBoundary(), S1.getBoundary(), 0);
        hit.setContextEnd(S4.getBoundary());
        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S1, S2, S3, S4, S5, S6, S7, S8, S9);
        AllHits hits = getExpectedAnnotationHits("71D4C8BE", List.of(hit), context);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits);

        expectField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationsHitsFactoryErrorTest() throws Exception {
        withAnnotationHits();
        logic.getAllHitsQueryConfig().setAllHitsFactoryClass(AllHitsFactoryErrorOnly.class.getCanonicalName());

        givenAnnotation(buildAnnotation(S1, S2, S3, S4, S5, S6, S7, S8, S9));

        givenQuery("UUID=='CAPONE'");
        givenStartDate("20091231");
        givenEndDate("20150101");

        expectField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS",
                        "[{\"annotationId\":\"71D4C8BE\",\"maxTermHitConfidence\":0.0,\"keywordResultList\":[],\"errorMessage\":\"test failure\"}]");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsWrongTypeTest() throws Exception {
        withAnnotationHits();
        givenAnnotation(buildAnnotation("ANNO2", "capone", "abc", S1));

        givenQuery("UUID=='CAPONE'");
        givenStartDate("20091231");
        givenEndDate("20150101");

        expectNoField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsMultipleAnnotationsTest() throws Exception {
        withAnnotationHits();
        givenAnnotation(buildAnnotation("ANNO1", "capone", "abc", S1));
        givenAnnotation(buildAnnotation("ANNO1", "capone2", "abcd", S1, S2));

        givenQuery("UUID=='CAPONE'");
        givenStartDate("20091231");
        givenEndDate("20150101");

        AnnotationHitsTransformer.SegmentHit hit1 = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 0);
        hit1.setContextEnd(S1.getBoundary());
        AnnotationHitsTransformer.SegmentHit hit2 = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 0);
        hit2.setContextEnd(S2.getBoundary());

        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S1);
        AllHits hits1 = getExpectedAnnotationHits("03AE6355", List.of(hit1), context);
        context = buildSortedContext(S1, S2);
        AllHits hits2 = getExpectedAnnotationHits("DCC5F4AB", List.of(hit2), context);

        String expectedAnnotationHits = getExpectedALlHitsRollup(hits1, hits2);

        expectField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsFromWildcardKeywordTest() throws Exception {
        withAnnotationHits();
        givenAnnotation(buildAnnotation("ANNO1", "capone", "abc", S1));

        // capo.* is expanded in query planning to capone
        givenQuery("UUID =~ 'CAPO.*'");
        givenStartDate("20091231");
        givenEndDate("20150101");

        AnnotationHitsTransformer.SegmentHit hit1 = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 0);
        hit1.setContextEnd(S1.getBoundary());

        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S1);
        AllHits hits1 = getExpectedAnnotationHits("03AE6355", List.of(hit1), context);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits1);

        expectField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
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
        givenStartDate("20091231");
        givenEndDate("20150101");

        AnnotationHitsTransformer.SegmentHit hit1 = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 0);
        hit1.setContextEnd(wildcard.getBoundary());

        AnnotationHitsTransformer.SegmentHit hit2 = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), wildcard.getBoundary(), 0);
        hit2.setContextEnd(wildcard.getBoundary());

        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S1, wildcard);
        AllHits hits1 = getExpectedAnnotationHits("5F8B7BC3", List.of(hit1, hit2), context);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits1);

        expectField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsTimeRangeInSecondsTest() throws Exception {
        withAnnotationHits();

        givenAnnotation(buildAnnotation("ANNO1", "capone", "abc", S1));

        givenQueryParameter(TIMEUNIT_PARAMETER, TimeUnit.SECONDS.toString());

        givenQuery("UUID=='CAPONE'");
        givenStartDate("20091231");
        givenEndDate("20150101");

        AnnotationHitsTransformer.SegmentHit hit1 = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 0);
        hit1.setContextEnd(S1.getBoundary());

        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S1);

        AllHitsFactory factory = new AllHitsFactory();
        AllHits hits1 = factory.create("03AE6355", List.of(hit1), context, TimeUnit.SECONDS);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits1);

        expectField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void annotationHitsTimeRangeInMicrosTest() throws Exception {
        withAnnotationHits();

        givenAnnotation(buildAnnotation("ANNO1", "capone", "abc", S1));

        givenQueryParameter(TIMEUNIT_PARAMETER, TimeUnit.MICROSECONDS.toString());

        givenQuery("UUID=='CAPONE'");
        givenStartDate("20091231");
        givenEndDate("20150101");

        AnnotationHitsTransformer.SegmentHit hit1 = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 0);
        hit1.setContextEnd(S1.getBoundary());

        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S1);

        AllHitsFactory factory = new AllHitsFactory();
        AllHits hits1 = factory.create("03AE6355", List.of(hit1), context, TimeUnit.MICROSECONDS);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits1);

        expectField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void luceneUnfieldedTest() throws Exception {
        withAnnotationHits();
        logic.getAllHitsQueryConfig().setQueryTermExtractor(new TermExtractor(Set.of("_ANYFIELD_")));
        disableQueryTreeValidation();

        givenAnnotation(buildAnnotation("ANNO1", "capone", "abc", S1));

        givenQueryParameter(TIMEUNIT_PARAMETER, TimeUnit.MICROSECONDS.toString());
        givenQueryParameter(QueryParameters.QUERY_SYNTAX, "LUCENE");
        givenQuery("CAPONE");
        givenStartDate("20091231");
        givenEndDate("20150101");

        AnnotationHitsTransformer.SegmentHit hit1 = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 0);
        hit1.setContextEnd(S1.getBoundary());

        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S1);

        AllHitsFactory factory = new AllHitsFactory();
        AllHits hits1 = factory.create("03AE6355", List.of(hit1), context, TimeUnit.MICROSECONDS);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits1);

        expectField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void luceneFieldedTest() throws Exception {
        withAnnotationHits();
        logic.getAllHitsQueryConfig().setQueryTermExtractor(new TermExtractor(Set.of("_ANYFIELD_", "UUID")));
        disableQueryTreeValidation();

        givenAnnotation(buildAnnotation("ANNO1", "capone", "abc", S1));

        givenQueryParameter(TIMEUNIT_PARAMETER, TimeUnit.MICROSECONDS.toString());
        givenQueryParameter(QueryParameters.QUERY_SYNTAX, "LUCENE");
        givenQuery("UUID:CAPONE");
        givenStartDate("20091231");
        givenEndDate("20150101");

        AnnotationHitsTransformer.SegmentHit hit1 = new AnnotationHitsTransformer.SegmentHit(S1.getBoundary(), S1.getBoundary(), 0);
        hit1.setContextEnd(S1.getBoundary());

        TreeMap<SegmentBoundary,List<SegmentValue>> context = buildSortedContext(S1);

        AllHitsFactory factory = new AllHitsFactory();
        AllHits hits1 = factory.create("03AE6355", List.of(hit1), context, TimeUnit.MICROSECONDS);
        String expectedAnnotationHits = getExpectedALlHitsRollup(hits1);

        expectField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS", expectedAnnotationHits);

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    private void disableQueryTreeValidation() {
        TimedVisitorManager visitorManager = ((DefaultQueryPlanner) logic.getQueryPlanner()).getVisitorManager();
        visitorManager.setValidateAst(false);
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

            BatchWriter writer = client.createBatchWriter("annotation");
            AnnotationSerializer<Iterator<Map.Entry<Key,Value>>,Annotation> serializer = new AccumuloAnnotationSerializer();
            AccumuloAnnotationSourceSerializer sourceSerializer = new AccumuloAnnotationSourceSerializer();
            AnnotationDataAccess dataAccess = new AnnotationDataAccess(client, authSet, "annotation", "annotationSource", serializer, sourceSerializer);
            for (Annotation annotation : annotations) {
                // if we really want to validate the annotation id matches pull it here
                dataAccess.addAnnotation(annotation);
            }
            writer.flush();
            writer.close();
        } catch (AccumuloException | AccumuloSecurityException | TableExistsException | TableNotFoundException e) {
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
        givenQueryParameter(ENABLED_PARAMETER, "true");
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
                .setUid(WiseGuysIngest.caponeUID)
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

    private void expectNoField(String id, String field) {
        List<String> noFields = expectNoField.computeIfAbsent(id, x -> new ArrayList<>());
        noFields.add(field);
    }

    private void expectField(String id, String field, String value) {
        Map<String,String> fieldMap = expectedFields.computeIfAbsent(id, x -> new HashMap<>());
        fieldMap.put(field, value);
    }

    private void givenQuery(String query) {
        this.query = query;
    }

    private void givenQueryParameter(String parameter, String value) {
        this.queryParameters.put(parameter, value);
    }

    private void givenStartDate(String date) throws ParseException {
        this.startDate = dateFormat.parse(date);
    }

    private void givenEndDate(String date) throws ParseException {
        this.endDate = dateFormat.parse(date);
    }
}
