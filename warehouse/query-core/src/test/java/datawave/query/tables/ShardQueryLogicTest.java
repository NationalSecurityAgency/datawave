package datawave.query.tables;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
import java.util.UUID;

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

import com.google.common.collect.Sets;

import datawave.annotation.data.AnnotationSerializer;
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
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.QueryParameters;
import datawave.query.QueryTestTableHelper;
import datawave.query.RebuildingScannerTestHelper;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.transformer.DocumentTransformer;
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
    public void allHitsNoAnnotationsTest() throws Exception {
        withAllHits();

        givenQuery("UUID=='CAPONE'");
        givenStartDate("20091231");
        givenEndDate("20150101");

        expectNoField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void allHitsSingleHitNoContextWindowTest() throws Exception {
        withAllHits();

        givenAnnotation(buildAnnotation(S1));

        givenQuery("UUID=='CAPONE'");
        givenStartDate("20091231");
        givenEndDate("20150101");

        expectField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS",
                        "[{\"confidence\":0.3,\"oneBestContext\":[{\"label\":\"carl\",\"confidence\":1.0,\"timeRange\":{\"startTime\":20.0,\"endTime\":30.0}}],\"termHits\":[{\"termLabel\":\"capone\",\"confidence\":0.3,\"timeRange\":{\"startTime\":20.0,\"endTime\":30.0}}]}]");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void allHitsSingleHit1ContextWindowTest() throws Exception {
        withAllHits();

        givenAnnotation(buildAnnotation(S1, S2, S6));

        givenQuery("UUID=='CAPONE'");
        givenStartDate("20091231");
        givenEndDate("20150101");

        expectField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS",
                        "[{\"confidence\":0.3,\"oneBestContext\":[{\"label\":\"w2\",\"confidence\":1.0,\"timeRange\":{\"startTime\":2.0,\"endTime\":3.0}},{\"label\":\"carl\",\"confidence\":1.0,\"timeRange\":{\"startTime\":20.0,\"endTime\":30.0}},{\"label\":\"a2\",\"confidence\":0.9,\"timeRange\":{\"startTime\":40.0,\"endTime\":50.0}}],\"termHits\":[{\"termLabel\":\"capone\",\"confidence\":0.3,\"timeRange\":{\"startTime\":20.0,\"endTime\":30.0}}]}]");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void allHitsSingleHit2ContextWindowTest() throws Exception {
        withAllHits();

        givenAnnotation(buildAnnotation(S1, S2, S3, S6, S7));

        givenQuery("UUID=='CAPONE'");
        givenStartDate("20091231");
        givenEndDate("20150101");

        expectField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS",
                        "[{\"confidence\":0.3,\"oneBestContext\":[{\"label\":\"w2\",\"confidence\":1.0,\"timeRange\":{\"startTime\":2.0,\"endTime\":3.0}},{\"label\":\"x2\",\"confidence\":0.9,\"timeRange\":{\"startTime\":4.0,\"endTime\":5.0}},{\"label\":\"carl\",\"confidence\":1.0,\"timeRange\":{\"startTime\":20.0,\"endTime\":30.0}},{\"label\":\"a2\",\"confidence\":0.9,\"timeRange\":{\"startTime\":40.0,\"endTime\":50.0}},{\"label\":\"b1\",\"confidence\":0.5,\"timeRange\":{\"startTime\":80.0,\"endTime\":100.0}}],\"termHits\":[{\"termLabel\":\"capone\",\"confidence\":0.3,\"timeRange\":{\"startTime\":20.0,\"endTime\":30.0}}]}]");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void allHitsSingleHit3ContextWindowTest() throws Exception {
        withAllHits();

        givenAnnotation(buildAnnotation(S1, S2, S3, S4, S6, S7, S8));

        givenQuery("UUID=='CAPONE'");
        givenStartDate("20091231");
        givenEndDate("20150101");

        expectField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS",
                        "[{\"confidence\":0.3,\"oneBestContext\":[{\"label\":\"w2\",\"confidence\":1.0,\"timeRange\":{\"startTime\":2.0,\"endTime\":3.0}},{\"label\":\"x2\",\"confidence\":0.9,\"timeRange\":{\"startTime\":4.0,\"endTime\":5.0}},{\"label\":\"y1\",\"confidence\":0.5,\"timeRange\":{\"startTime\":8.0,\"endTime\":10.0}},{\"label\":\"carl\",\"confidence\":1.0,\"timeRange\":{\"startTime\":20.0,\"endTime\":30.0}},{\"label\":\"a2\",\"confidence\":0.9,\"timeRange\":{\"startTime\":40.0,\"endTime\":50.0}},{\"label\":\"b1\",\"confidence\":0.5,\"timeRange\":{\"startTime\":80.0,\"endTime\":100.0}},{\"label\":\"c2\",\"confidence\":0.7,\"timeRange\":{\"startTime\":100.0,\"endTime\":110.0}}],\"termHits\":[{\"termLabel\":\"capone\",\"confidence\":0.3,\"timeRange\":{\"startTime\":20.0,\"endTime\":30.0}}]}]");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void allHitsSingleHitTest() throws Exception {
        withAllHits();

        givenAnnotation(buildAnnotation(S5, S2, S3, S4, S1));

        givenQuery("UUID=='CAPONE'");
        givenStartDate("20091231");
        givenEndDate("20150101");

        // omit segment 5 because it is beyond the window
        expectField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS",
                        "[{\"confidence\":0.3,\"oneBestContext\":[{\"label\":\"carl\",\"confidence\":1.0,\"timeRange\":{\"startTime\":20.0,\"endTime\":30.0}},{\"label\":\"a2\",\"confidence\":0.9,\"timeRange\":{\"startTime\":40.0,\"endTime\":50.0}},{\"label\":\"b1\",\"confidence\":0.5,\"timeRange\":{\"startTime\":80.0,\"endTime\":100.0}},{\"label\":\"c2\",\"confidence\":0.7,\"timeRange\":{\"startTime\":100.0,\"endTime\":110.0}}],\"termHits\":[{\"termLabel\":\"capone\",\"confidence\":0.3,\"timeRange\":{\"startTime\":20.0,\"endTime\":30.0}}]}]");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void allHitsSingleHitFullWindowTest() throws Exception {
        withAllHits();
        givenAnnotation(buildAnnotation(S2, S3, S4, S1, S5, S8, S6, S7, S9));

        givenQuery("UUID=='CAPONE'");
        givenStartDate("20091231");
        givenEndDate("20150101");

        // omit edge segments beyond the window
        expectField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS",
                        "[{\"confidence\":0.3,\"oneBestContext\":[{\"label\":\"x2\",\"confidence\":0.9,\"timeRange\":{\"startTime\":4.0,\"endTime\":5.0}},{\"label\":\"y1\",\"confidence\":0.5,\"timeRange\":{\"startTime\":8.0,\"endTime\":10.0}},{\"label\":\"z2\",\"confidence\":0.7,\"timeRange\":{\"startTime\":10.0,\"endTime\":11.0}},{\"label\":\"carl\",\"confidence\":1.0,\"timeRange\":{\"startTime\":20.0,\"endTime\":30.0}},{\"label\":\"a2\",\"confidence\":0.9,\"timeRange\":{\"startTime\":40.0,\"endTime\":50.0}},{\"label\":\"b1\",\"confidence\":0.5,\"timeRange\":{\"startTime\":80.0,\"endTime\":100.0}},{\"label\":\"c2\",\"confidence\":0.7,\"timeRange\":{\"startTime\":100.0,\"endTime\":110.0}}],\"termHits\":[{\"termLabel\":\"capone\",\"confidence\":0.3,\"timeRange\":{\"startTime\":20.0,\"endTime\":30.0}}]}]");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void allHitsMultiHitSameBoundaryTest() throws Exception {
        withAllHits();
        givenAnnotation(buildAnnotation(S2, S3, S4, S1, S5, S8, S6, S7, S9));

        givenQuery("UUID=='CAPONE' || UUID=='CARL'");
        givenStartDate("20091231");
        givenEndDate("20150101");

        // omit edge segments beyond the window
        expectField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS",
                        "[{\"confidence\":1.0,\"oneBestContext\":[{\"label\":\"x2\",\"confidence\":0.9,\"timeRange\":{\"startTime\":4.0,\"endTime\":5.0}},{\"label\":\"y1\",\"confidence\":0.5,\"timeRange\":{\"startTime\":8.0,\"endTime\":10.0}},{\"label\":\"z2\",\"confidence\":0.7,\"timeRange\":{\"startTime\":10.0,\"endTime\":11.0}},{\"label\":\"carl\",\"confidence\":1.0,\"timeRange\":{\"startTime\":20.0,\"endTime\":30.0}},{\"label\":\"a2\",\"confidence\":0.9,\"timeRange\":{\"startTime\":40.0,\"endTime\":50.0}},{\"label\":\"b1\",\"confidence\":0.5,\"timeRange\":{\"startTime\":80.0,\"endTime\":100.0}},{\"label\":\"c2\",\"confidence\":0.7,\"timeRange\":{\"startTime\":100.0,\"endTime\":110.0}}],\"termHits\":[{\"termLabel\":\"capone\",\"confidence\":0.3,\"timeRange\":{\"startTime\":20.0,\"endTime\":30.0}},{\"termLabel\":\"carl\",\"confidence\":1.0,\"timeRange\":{\"startTime\":20.0,\"endTime\":30.0}}]}]");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    @Test
    public void allHitsMultiHitTest() throws Exception {
        withAllHits();
        givenAnnotation(buildAnnotation(S2, S3, S4, S1, S5, S8, S6, S7, S9));

        givenQuery("UUID=='CAPONE' || UUID=='w1' || UUID=='d1'");
        givenStartDate("20091231");
        givenEndDate("20150101");

        // omit edge segments beyond the window
        expectField(WiseGuysIngest.caponeUID, "ALL_HITS_RESULTS",
                        "[{\"confidence\":0.9,\"oneBestContext\":[{\"label\":\"w2\",\"confidence\":1.0,\"timeRange\":{\"startTime\":2.0,\"endTime\":3.0}},{\"label\":\"x2\",\"confidence\":0.9,\"timeRange\":{\"startTime\":4.0,\"endTime\":5.0}},{\"label\":\"y1\",\"confidence\":0.5,\"timeRange\":{\"startTime\":8.0,\"endTime\":10.0}},{\"label\":\"z2\",\"confidence\":0.7,\"timeRange\":{\"startTime\":10.0,\"endTime\":11.0}}],\"termHits\":[{\"termLabel\":\"w1\",\"confidence\":0.9,\"timeRange\":{\"startTime\":2.0,\"endTime\":3.0}}]},{\"confidence\":0.3,\"oneBestContext\":[{\"label\":\"x2\",\"confidence\":0.9,\"timeRange\":{\"startTime\":4.0,\"endTime\":5.0}},{\"label\":\"y1\",\"confidence\":0.5,\"timeRange\":{\"startTime\":8.0,\"endTime\":10.0}},{\"label\":\"z2\",\"confidence\":0.7,\"timeRange\":{\"startTime\":10.0,\"endTime\":11.0}},{\"label\":\"carl\",\"confidence\":1.0,\"timeRange\":{\"startTime\":20.0,\"endTime\":30.0}},{\"label\":\"a2\",\"confidence\":0.9,\"timeRange\":{\"startTime\":40.0,\"endTime\":50.0}},{\"label\":\"b1\",\"confidence\":0.5,\"timeRange\":{\"startTime\":80.0,\"endTime\":100.0}},{\"label\":\"c2\",\"confidence\":0.7,\"timeRange\":{\"startTime\":100.0,\"endTime\":110.0}}],\"termHits\":[{\"termLabel\":\"capone\",\"confidence\":0.3,\"timeRange\":{\"startTime\":20.0,\"endTime\":30.0}}]},{\"confidence\":0.6,\"oneBestContext\":[{\"label\":\"a2\",\"confidence\":0.9,\"timeRange\":{\"startTime\":40.0,\"endTime\":50.0}},{\"label\":\"b1\",\"confidence\":0.5,\"timeRange\":{\"startTime\":80.0,\"endTime\":100.0}},{\"label\":\"c2\",\"confidence\":0.7,\"timeRange\":{\"startTime\":100.0,\"endTime\":110.0}},{\"label\":\"d1\",\"confidence\":0.6,\"timeRange\":{\"startTime\":120.0,\"endTime\":250.0}}],\"termHits\":[{\"termLabel\":\"d1\",\"confidence\":0.6,\"timeRange\":{\"startTime\":120.0,\"endTime\":250.0}}]}]");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));
        runTestQuery(expected);
    }

    private void setupAnnotationsTables(AccumuloClient client) {
        try {
            // drop existing tables if they exist
            client.tableOperations().delete("annotations");
        } catch (AccumuloException | TableNotFoundException | AccumuloSecurityException e) {
            // no-op
        }

        try {
            client.tableOperations().delete("annotationsSource");
        } catch (AccumuloException | TableNotFoundException | AccumuloSecurityException e) {
            // no-op
        }

        try {
            // create annotations tables
            client.tableOperations().create("annotations");
            client.tableOperations().create("annotationsSource");

            BatchWriter writer = client.createBatchWriter("annotations");
            AnnotationSerializer<Iterator<Map.Entry<Key,Value>>,Annotation> serializer = new AccumuloAnnotationSerializer();
            AccumuloAnnotationSourceSerializer sourceSerializer = new AccumuloAnnotationSourceSerializer();
            AnnotationDataAccess dataAccess = new AnnotationDataAccess(client, authSet, "annotations", "annotationsSource", serializer, sourceSerializer);
            for (Annotation annotation : annotations) {
                dataAccess.addAnnotation(annotation);
            }
            writer.flush();
            writer.close();
        } catch (AccumuloException | AccumuloSecurityException | TableExistsException | TableNotFoundException e) {
            throw new RuntimeException(e);
        }

    }

    private void withAllHits() {
        logic.setAllHitsEnabled(true);
        logic.setAllHitsContextLength(3);
        logic.setAllHitsValidQueryFields(Set.of("FOO", "BAR", "UUID"));
        logic.setAllHitsTargetField("ALL_HITS_RESULTS");
        logic.setAllHitsValidTypes(Set.of("ANNO1"));
        logic.setAnnotationTableName("annotations");
        logic.setAnnotationSourceTableName("annotationsSource");
    }

    private String getExpectedAllHits() {
        // TODO
        return null;
    }

    private Annotation buildAnnotation(Segment... segments) {
        // @formatter:off
        return Annotation.newBuilder()
                .setShard("20130101_0")
                .setDataType("test")
                .setUid(WiseGuysIngest.caponeUID)
                .setAnnotationType("ANNO1")
                .setDocumentId("CAPONE")
                .setAnalyticSourceHash("abc")
                .putAllMetadata(Map.of("visibility", "ALL", "created_date", "2025-12-29T00:00:00Z"))
                .addAllSegments(List.of(segments))
                .build();
        // @formatter:on
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
