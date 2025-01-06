package datawave.query.tables;

import java.text.DateFormat;
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
import datawave.microservice.query.QueryImpl;
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

/**
 * generic ShardQueryLogic tests -testPrimaryToSecondaryFieldMapForQueryProfile
 *
 */
public abstract class ShardQueryLogicTest {

    private static final Logger log = Logger.getLogger(ShardQueryLogicTest.class);

    @RunWith(Arquillian.class)
    public static class ShardRange extends ShardQueryLogicTest {
        protected static AccumuloClient connector = null;
        private static Authorizations auths = new Authorizations("ALL");

        @BeforeClass
        public static void setUp() throws Exception {

            // testing tear downs but without consistency, because when we tear it down then we loose the ongoing bloom filter and subsequently the rebuild will
            // start returning
            // different keys.
            QueryTestTableHelper qtth = new QueryTestTableHelper(ShardRange.class.toString(), log,
                            RebuildingScannerTestHelper.TEARDOWN.EVERY_OTHER_SANS_CONSISTENCY, RebuildingScannerTestHelper.INTERRUPT.EVERY_OTHER);
            connector = qtth.client;

            WiseGuysIngest.writeItAll(connector, WiseGuysIngest.WhatKindaRange.SHARD);
            PrintUtility.printTable(connector, auths, TableName.SHARD);
            PrintUtility.printTable(connector, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(connector, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        }

        @Override
        protected void runTestQuery(Set<Set<String>> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms) throws Exception {
            super.runTestQuery(expected, querystr, startDate, endDate, extraParms, connector);
        }
    }

    @RunWith(Arquillian.class)
    public static class DocumentRange extends ShardQueryLogicTest {
        protected static AccumuloClient connector = null;
        private static Authorizations auths = new Authorizations("ALL");

        @BeforeClass
        public static void setUp() throws Exception {

            // testing tear downs but without consistency, because when we tear it down then we loose the ongoing bloom filter and subsequently the rebuild will
            // start returning
            // different keys.
            QueryTestTableHelper qtth = new QueryTestTableHelper(DocumentRange.class.toString(), log,
                            RebuildingScannerTestHelper.TEARDOWN.EVERY_OTHER_SANS_CONSISTENCY, RebuildingScannerTestHelper.INTERRUPT.EVERY_OTHER);
            connector = qtth.client;

            WiseGuysIngest.writeItAll(connector, WiseGuysIngest.WhatKindaRange.DOCUMENT);
            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(connector, auths, TableName.SHARD);
            PrintUtility.printTable(connector, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(connector, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        }

        @Override
        protected void runTestQuery(Set<Set<String>> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms) throws Exception {
            super.runTestQuery(expected, querystr, startDate, endDate, extraParms, connector);
        }
    }

    protected Authorizations auths = new Authorizations("ALL");

    protected Set<Authorizations> authSet = Collections.singleton(auths);

    @Inject
    @SpringBean(name = "EventQuery")
    protected ShardQueryLogic logic;

    protected KryoDocumentDeserializer deserializer;

    private final DateFormat format = new SimpleDateFormat("yyyyMMdd");

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

    @AfterClass
    public static void teardown() {
        TypeRegistry.reset();
    }

    @Before
    public void setup() {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));

        logic.setFullTableScanEnabled(true);
        deserializer = new KryoDocumentDeserializer();
    }

    protected abstract void runTestQuery(Set<Set<String>> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms)
                    throws Exception;

    protected void runTestQuery(Set<Set<String>> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms,
                    AccumuloClient connector) throws Exception {
        log.debug("runTestQuery");

        QueryImpl settings = new QueryImpl();
        settings.setBeginDate(startDate);
        settings.setEndDate(endDate);
        settings.setPagesize(Integer.MAX_VALUE);
        settings.setQueryAuthorizations(auths.serialize());
        settings.setQuery(querystr);
        settings.setParameters(extraParms);
        settings.setId(UUID.randomUUID());

        log.debug("query: " + settings.getQuery());
        log.debug("logic: " + settings.getQueryLogicName());

        GenericQueryConfiguration config = logic.initialize(connector, settings, authSet);
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
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "false");
        extraParameters.put("query.profile", "copyFieldEventQuery");

        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");

        String queryString = "UUID =~ '^[CS].*'";

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.sopranoUID, "MAGIC_COPY:18"));
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.corleoneUID, "MAGIC_COPY:18"));
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID, "MAGIC_COPY:18"));
        runTestQuery(expected, queryString, startDate, endDate, extraParameters);
    }

    @Test
    public void testRegex() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");

        String queryString = "UUID=='CAPONE' AND QUOTE=~'.*kind'";
        Set<Set<String>> expected = new HashSet<>();
        // todo: make this work someday
        // expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));

        runTestQuery(expected, queryString, format.parse("20091231"), format.parse("20150101"), extraParameters);

    }

    @Test
    public void testFwdRegex() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");

        String queryString = "UUID=='CAPONE' AND QUOTE=~'kin.*'";
        Set<Set<String>> expected = new HashSet<>();
        // todo: make this work someday
        // expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));

        runTestQuery(expected, queryString, format.parse("20091231"), format.parse("20150101"), extraParameters);

    }

    @Test
    public void testEvalRegex() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");

        String queryString = "UUID=='CAPONE' AND ((_Eval_ = true) && QUOTE=~'.*alone')";
        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));

        runTestQuery(expected, queryString, format.parse("20091231"), format.parse("20150101"), extraParameters);
    }

    @Test
    public void testNegativeEvalRegex() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");

        String queryString = "UUID=='CAPONE' AND ((_Eval_ = true) && QUOTE!~'.*alone')";
        Set<Set<String>> expected = new HashSet<>();
        runTestQuery(expected, queryString, format.parse("20091231"), format.parse("20150101"), extraParameters);

    }

    @Test
    public void testNegativeEvalRegexV2() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");

        String queryString = "UUID=='CAPONE' AND ((_Eval_ = true) && !(QUOTE=~'.*alone'))";
        Set<Set<String>> expected = new HashSet<>();
        runTestQuery(expected, queryString, format.parse("20091231"), format.parse("20150101"), extraParameters);

    }

    @Test
    public void testDoubeWildcard() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");

        String queryString = "UUID=='CAPONE' AND QUOTE=~'.*ind.*'";
        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));

        runTestQuery(expected, queryString, format.parse("20091231"), format.parse("20150101"), extraParameters);
    }

    @Test
    public void testNegativeRegex() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");

        String queryString = "UUID=='CAPONE' AND QUOTE!~'.*ind'";
        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));

        runTestQuery(expected, queryString, format.parse("20091231"), format.parse("20150101"), extraParameters);
    }

    @Test
    public void testNegativeRegexV2() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");

        String queryString = "UUID=='CAPONE' AND !(QUOTE=~'.*ind')";
        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));

        runTestQuery(expected, queryString, format.parse("20091231"), format.parse("20150101"), extraParameters);
    }

    @Test
    public void testFilterRegex() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");

        String queryString = "UUID=='CAPONE' AND filter:includeRegex(QUOTE,'.*kind word alone.*')";
        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));

        runTestQuery(expected, queryString, format.parse("20091231"), format.parse("20150101"), extraParameters);
    }

    @Test
    public void testNegativeFilterRegex() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");

        String queryString = "UUID=='CAPONE' AND !filter:includeRegex(QUOTE,'.*kind word alone.*')";
        Set<Set<String>> expected = new HashSet<>();

        runTestQuery(expected, queryString, format.parse("20091231"), format.parse("20150101"), extraParameters);
    }

    @Test
    public void testNegativeFilterRegexV2() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");

        String queryString = "UUID=='CAPONE' AND !(filter:includeRegex(QUOTE,'.*kind word alone.*'))";
        Set<Set<String>> expected = new HashSet<>();

        runTestQuery(expected, queryString, format.parse("20091231"), format.parse("20150101"), extraParameters);
    }

    @Test
    public void testExcludeDataTypesBangDataType() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("datatype.filter.set", "!test2");

        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");

        String queryString = "UUID=='TATTAGLIA'";
        Set<Set<String>> expected = new HashSet<>();
        // No results expected

        runTestQuery(expected, queryString, startDate, endDate, extraParameters);
    }

    @Test
    public void testExcludeDataTypesNegateDataType() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("datatype.filter.set", "test2,!test2");

        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");

        String queryString = "UUID=='TATTAGLIA'";
        Set<Set<String>> expected = new HashSet<>();
        // Expect one result, since the negated data type results in empty set, which is treated by Datawave as all data types
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.tattagliaUID));

        runTestQuery(expected, queryString, startDate, endDate, extraParameters);
    }

    @Test
    public void testExcludeDataTypesIncludeOneTypeExcludeOneType() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("datatype.filter.set", "test2,!test");

        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");

        String queryString = "UUID=='TATTAGLIA' || UUID=='CAPONE'";
        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.tattagliaUID));

        runTestQuery(expected, queryString, startDate, endDate, extraParameters);
    }
}
