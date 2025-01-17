package datawave.query.tables;

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
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.Sets;

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

    @Inject
    @SpringBean(name = "EventQuery")
    protected ShardQueryLogic logic;
    protected KryoDocumentDeserializer deserializer;

    private final DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
    private final Map<String,String> queryParameters = new HashMap<>();

    private String query;
    private Date startDate;
    private Date endDate;

    protected abstract String getRange();

    @RunWith(Arquillian.class)
    public static class ShardRange extends ShardQueryLogicTest {

        @Override
        protected String getRange() {
            return WiseGuysIngest.WhatKindaRange.SHARD.name();
        }
    }

    @RunWith(Arquillian.class)
    public static class DocumentRange extends ShardQueryLogicTest {

        @Override
        protected String getRange() {
            return WiseGuysIngest.WhatKindaRange.DOCUMENT.name();
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

        Assert.assertTrue(response instanceof DefaultEventQueryResponse);
        DefaultEventQueryResponse eventQueryResponse = (DefaultEventQueryResponse) response;

        if (expected.isEmpty()) {
            Assert.assertTrue(eventQueryResponse.getEvents() == null || eventQueryResponse.getEvents().isEmpty());
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
                        break;
                    }
                }
                Assert.assertTrue("field not found " + expectedSet, found);
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
