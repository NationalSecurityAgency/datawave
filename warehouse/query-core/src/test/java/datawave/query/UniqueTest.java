package datawave.query;

import java.net.URL;
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
import datawave.query.exceptions.InvalidQueryException;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.transformer.DocumentTransformer;
import datawave.query.util.WiseGuysIngest;
import datawave.util.TableName;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.DefaultEventQueryResponse;

/**
 * Applies uniqueness to queries
 *
 */
public abstract class UniqueTest {

    private static final Logger log = Logger.getLogger(UniqueTest.class);

    @RunWith(Arquillian.class)
    public static class ShardRange extends UniqueTest {
        protected static AccumuloClient client = null;
        private static Authorizations auths = new Authorizations("ALL");

        @BeforeClass
        public static void setUp() throws Exception {

            // testing tear downs but without consistency, because when we tear it down then we loose the ongoing bloom filter and subsequently the rebuild will
            // start returning
            // different keys.
            QueryTestTableHelper qtth = new QueryTestTableHelper(ShardRange.class.toString(), log,
                            RebuildingScannerTestHelper.TEARDOWN.EVERY_OTHER_SANS_CONSISTENCY, RebuildingScannerTestHelper.INTERRUPT.EVERY_OTHER);
            client = qtth.client;

            WiseGuysIngest.writeItAll(client, WiseGuysIngest.WhatKindaRange.SHARD);
            PrintUtility.printTable(client, auths, TableName.SHARD);
            PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        }

        @Override
        protected void runTestQueryWithUniqueness(Set<Set<String>> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms)
                        throws Exception {
            super.runTestQueryWithUniqueness(expected, querystr, startDate, endDate, extraParms, client);
        }
    }

    @RunWith(Arquillian.class)
    public static class DocumentRange extends UniqueTest {
        protected static AccumuloClient client = null;
        private static Authorizations auths = new Authorizations("ALL");

        @BeforeClass
        public static void setUp() throws Exception {

            // testing tear downs but without consistency, because when we tear it down then we loose the ongoing bloom filter and subsequently the rebuild will
            // start returning
            // different keys.
            QueryTestTableHelper qtth = new QueryTestTableHelper(DocumentRange.class.toString(), log,
                            RebuildingScannerTestHelper.TEARDOWN.EVERY_OTHER_SANS_CONSISTENCY, RebuildingScannerTestHelper.INTERRUPT.EVERY_OTHER);
            client = qtth.client;

            WiseGuysIngest.writeItAll(client, WiseGuysIngest.WhatKindaRange.DOCUMENT);
            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(client, auths, TableName.SHARD);
            PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        }

        @Override
        protected void runTestQueryWithUniqueness(Set<Set<String>> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms)
                        throws Exception {
            super.runTestQueryWithUniqueness(expected, querystr, startDate, endDate, extraParms, client);
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
                                        "datawave.webservice.query.result.event", "datawave.core.query.result.event")
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
        // setup the hadoop configuration
        URL hadoopConfig = this.getClass().getResource("/testhadoop.config");
        logic.setHdfsSiteConfigURLs(hadoopConfig.toExternalForm());

        logic.setQueryExecutionForPageTimeout(300000000000000L);
        deserializer = new KryoDocumentDeserializer();
    }

    protected abstract void runTestQueryWithUniqueness(Set<Set<String>> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms)
                    throws Exception;

    protected void runTestQueryWithUniqueness(Set<Set<String>> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms,
                    AccumuloClient client) throws Exception {
        log.debug("runTestQueryWithUniqueness");

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

        GenericQueryConfiguration config = logic.initialize(client, settings, authSet);
        logic.setupQuery(config);

        DocumentTransformer transformer = (DocumentTransformer) (logic.getTransformer(settings));
        TransformIterator iter = new DatawaveTransformIterator(logic.iterator(), transformer);
        List<Object> eventList = new ArrayList<>();
        while (iter.hasNext()) {
            Object o = iter.next();
            if (o != null) {
                eventList.add(o);
            }
        }

        BaseQueryResponse response = transformer.createResponse(eventList);

        // un-comment to look at the json output
        // ObjectMapper mapper = new ObjectMapper();
        // mapper.enable(MapperFeature.USE_WRAPPER_NAME_AS_PROPERTY_NAME);
        // mapper.writeValue(new File("/tmp/grouped2.json"), response);

        Assert.assertTrue(response instanceof DefaultEventQueryResponse);
        DefaultEventQueryResponse eventQueryResponse = (DefaultEventQueryResponse) response;

        // copy expected set to avoid modifying parameter passed in
        expected = new HashSet<>(expected);

        for (EventBase event : eventQueryResponse.getEvents()) {
            boolean found = false;
            for (Iterator<Set<String>> it = expected.iterator(); it.hasNext();) {
                Set<String> expectedSet = it.next();

                if (expectedSet.contains(event.getMetadata().getInternalId())) {
                    it.remove();
                    found = true;
                    break;
                }
            }
            Assert.assertTrue("Failed to find " + event.getMetadata().getInternalId() + " in expected results", found);
        }
        Assert.assertTrue("Failed to find all expected results.  Missing " + expected, expected.isEmpty());
    }

    @Test
    public void testUniqueness() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");

        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");

        String queryString = "UUID =~ '^[CS].*'";

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet(WiseGuysIngest.sopranoUID, WiseGuysIngest.corleoneUID, WiseGuysIngest.caponeUID));
        extraParameters.put("unique.fields", "DEATH_DATE,$MAGIC");
        runTestQueryWithUniqueness(expected, queryString, startDate, endDate, extraParameters);

        expected.clear();
        expected.add(Sets.newHashSet(WiseGuysIngest.sopranoUID));
        expected.add(Sets.newHashSet(WiseGuysIngest.corleoneUID));
        expected.add(Sets.newHashSet(WiseGuysIngest.caponeUID));
        extraParameters.put("unique.fields", "$DEATH_DATE,BIRTH_DATE");
        runTestQueryWithUniqueness(expected, queryString, startDate, endDate, extraParameters);

        expected.clear();
        expected.add(Sets.newHashSet(WiseGuysIngest.sopranoUID));
        expected.add(Sets.newHashSet(WiseGuysIngest.corleoneUID));
        expected.add(Sets.newHashSet(WiseGuysIngest.caponeUID));
        extraParameters.put("unique.fields", "death_date,birth_date");
        runTestQueryWithUniqueness(expected, queryString, startDate, endDate, extraParameters);
    }

    @Test
    public void testUniquenessWithMissingField() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");

        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");

        String queryString = "UUID =~ '^[CS].*'";

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet(WiseGuysIngest.sopranoUID, WiseGuysIngest.corleoneUID));
        expected.add(Sets.newHashSet(WiseGuysIngest.caponeUID));
        extraParameters.put("unique.fields", "NUMBER");
        runTestQueryWithUniqueness(expected, queryString, startDate, endDate, extraParameters);

    }

    @Test
    public void testUniquenessUsingFunction() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");

        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");

        String queryString = "UUID =~ '^[CS].*' && f:unique($DEATH_DATE,MAGIC)";
        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet(WiseGuysIngest.sopranoUID, WiseGuysIngest.corleoneUID, WiseGuysIngest.caponeUID));
        runTestQueryWithUniqueness(expected, queryString, startDate, endDate, extraParameters);

        queryString = "UUID =~ '^[CS].*' && f:unique('DEATH_DATE','$BIRTH_DATE')";
        expected.clear();
        expected.add(Sets.newHashSet(WiseGuysIngest.sopranoUID));
        expected.add(Sets.newHashSet(WiseGuysIngest.corleoneUID));
        expected.add(Sets.newHashSet(WiseGuysIngest.caponeUID));
        runTestQueryWithUniqueness(expected, queryString, startDate, endDate, extraParameters);

        queryString = "UUID =~ '^[CS].*' && f:unique('death_date','$birth_date')";
        expected.clear();
        expected.add(Sets.newHashSet(WiseGuysIngest.sopranoUID));
        expected.add(Sets.newHashSet(WiseGuysIngest.corleoneUID));
        expected.add(Sets.newHashSet(WiseGuysIngest.caponeUID));
        runTestQueryWithUniqueness(expected, queryString, startDate, endDate, extraParameters);
    }

    @Test
    public void testUniquenessUsingLuceneFunction() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("query.syntax", "LUCENE");

        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");

        String queryString = "UUID:/^[CS].*/ AND #UNIQUE(DEATH_DATE,$MAGIC)";
        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet(WiseGuysIngest.sopranoUID, WiseGuysIngest.corleoneUID, WiseGuysIngest.caponeUID));
        runTestQueryWithUniqueness(expected, queryString, startDate, endDate, extraParameters);

        queryString = "UUID:/^[CS].*/ AND #UNIQUE(DEATH_DATE,$BIRTH_DATE)";
        expected.clear();
        expected.add(Sets.newHashSet(WiseGuysIngest.sopranoUID));
        expected.add(Sets.newHashSet(WiseGuysIngest.corleoneUID));
        expected.add(Sets.newHashSet(WiseGuysIngest.caponeUID));
        runTestQueryWithUniqueness(expected, queryString, startDate, endDate, extraParameters);

        queryString = "UUID:/^[CS].*/ AND #UNIQUE(death_date,birth_date)";
        expected.clear();
        expected.add(Sets.newHashSet(WiseGuysIngest.sopranoUID));
        expected.add(Sets.newHashSet(WiseGuysIngest.corleoneUID));
        expected.add(Sets.newHashSet(WiseGuysIngest.caponeUID));
        runTestQueryWithUniqueness(expected, queryString, startDate, endDate, extraParameters);
    }

    @Test(expected = InvalidQueryException.class)
    public void testUniquenessWithBadField() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("query.syntax", "LUCENE");

        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");

        String queryString = "UUID:/^[CS].*/ AND #UNIQUE(FOO_BAR,$MAGIC)";
        runTestQueryWithUniqueness(new HashSet(), queryString, startDate, endDate, extraParameters);

        queryString = "UUID:/^[CS].*/ AND #UNIQUE(foo_bar,$magic)";
        runTestQueryWithUniqueness(new HashSet(), queryString, startDate, endDate, extraParameters);
    }

    @Test
    public void testUniquenessWithHitTermField() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("query.syntax", "LUCENE");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet(WiseGuysIngest.sopranoUID, WiseGuysIngest.corleoneUID, WiseGuysIngest.caponeUID));
        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");

        String queryString = "UUID:/^[CS].*/ AND #UNIQUE(HIT_TERM)";
        runTestQueryWithUniqueness(expected, queryString, startDate, endDate, extraParameters);
    }

    @Test
    public void testUniquenessWithModelAliases() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("query.syntax", "LUCENE");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet(WiseGuysIngest.sopranoUID, WiseGuysIngest.corleoneUID, WiseGuysIngest.caponeUID));
        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");

        String queryString = "UUID:/^[CS].*/ AND #UNIQUE(BOTH_NULL)";
        runTestQueryWithUniqueness(expected, queryString, startDate, endDate, extraParameters);
    }

    @Test
    public void testRecentUniquenessWithModelAliases() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("query.syntax", "LUCENE");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet(WiseGuysIngest.sopranoUID, WiseGuysIngest.corleoneUID, WiseGuysIngest.caponeUID));
        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");

        String queryString = "UUID:/^[CS].*/ AND #MOST_RECENT_UNIQUE(BOTH_NULL)";
        runTestQueryWithUniqueness(expected, queryString, startDate, endDate, extraParameters);
    }

    @Test
    public void testMostRecentUniqueness() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put(QueryParameters.MOST_RECENT_UNIQUE, "true");

        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");

        String queryString = "UUID =~ '^[CS].*'";

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet(WiseGuysIngest.caponeUID));
        extraParameters.put("unique.fields", "DEATH_DATE,$MAGIC");
        runTestQueryWithUniqueness(expected, queryString, startDate, endDate, extraParameters);

        expected.clear();
        expected.add(Sets.newHashSet(WiseGuysIngest.caponeUID));
        extraParameters.put("unique.fields", "death_date,$magic");
        runTestQueryWithUniqueness(expected, queryString, startDate, endDate, extraParameters);

        expected.clear();
        expected.add(Sets.newHashSet(WiseGuysIngest.sopranoUID));
        expected.add(Sets.newHashSet(WiseGuysIngest.corleoneUID));
        expected.add(Sets.newHashSet(WiseGuysIngest.caponeUID));
        extraParameters.put("unique.fields", "$DEATH_DATE,BIRTH_DATE");
        runTestQueryWithUniqueness(expected, queryString, startDate, endDate, extraParameters);

        expected.clear();
        expected.add(Sets.newHashSet(WiseGuysIngest.sopranoUID));
        expected.add(Sets.newHashSet(WiseGuysIngest.corleoneUID));
        expected.add(Sets.newHashSet(WiseGuysIngest.caponeUID));
        extraParameters.put("unique.fields", "death_date,birth_date");
        runTestQueryWithUniqueness(expected, queryString, startDate, endDate, extraParameters);
    }

    @Test
    public void testHannahHypothesis() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet(WiseGuysIngest.caponeUID));
        extraParameters.put(QueryParameters.MOST_RECENT_UNIQUE, "true");
        extraParameters.put("unique.fields", "DEATH_DATE,$MAGIC");
        String queryString = "UUID =~ '^[CS].*'";
        runTestQueryWithUniqueness(expected, queryString, startDate, endDate, extraParameters);

        extraParameters.clear();
        extraParameters.put(QueryParameters.MOST_RECENT_UNIQUE, "true");
        queryString = "UUID =~ '^[CS].*' && f:unique(death_date,magic)";
        runTestQueryWithUniqueness(expected, queryString, startDate, endDate, extraParameters);

        this.getClass().getMethod("testHannahHypothesis").getName().replace("Hypothesis", "Theory");
    }

}
