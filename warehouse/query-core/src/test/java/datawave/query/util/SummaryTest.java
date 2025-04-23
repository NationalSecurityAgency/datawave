package datawave.query.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import datawave.configuration.spring.SpringBean;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.microservice.query.QueryImpl;
import datawave.query.QueryTestTableHelper;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.function.JexlEvaluation;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.util.TableName;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;

public abstract class SummaryTest {

    @RunWith(Arquillian.class)
    public static class ShardRangeTest extends SummaryTest {
        protected static AccumuloClient connector = null;

        @BeforeClass
        public static void setUp() throws Exception {

            QueryTestTableHelper qtth = new QueryTestTableHelper(ShardRangeTest.class.toString(), log);
            connector = qtth.client;
            WiseGuysIngest.writeItAll(connector, WiseGuysIngest.WhatKindaRange.SHARD);
            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(connector, auths, TableName.SHARD);
            PrintUtility.printTable(connector, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(connector, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        }

        @Override
        @Before
        public void setup() throws ParseException {
            super.setup();
            logic.setCollapseUids(true);
        }

        @Override
        protected void runTestQuery(String queryString) throws Exception {
            super.runTestQuery(connector, queryString);
        }
    }

    @RunWith(Arquillian.class)
    public static class DocumentRangeTest extends SummaryTest {
        protected static AccumuloClient connector = null;

        @BeforeClass
        public static void setUp() throws Exception {

            QueryTestTableHelper qtth = new QueryTestTableHelper(DocumentRangeTest.class.toString(), log);
            connector = qtth.client;

            WiseGuysIngest.writeItAll(connector, WiseGuysIngest.WhatKindaRange.DOCUMENT);
            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(connector, auths, TableName.SHARD);
            PrintUtility.printTable(connector, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(connector, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        }

        @Override
        @Before
        public void setup() throws ParseException {
            super.setup();
            logic.setCollapseUids(false);
        }

        @Override
        protected void runTestQuery(String queryString) throws Exception {
            super.runTestQuery(connector, queryString);
        }
    }

    private static final Logger log = Logger.getLogger(SummaryTest.class);

    protected Authorizations auths = new Authorizations("ALL");

    protected Set<Authorizations> authSet = Set.of(auths);

    @Inject
    @SpringBean(name = "EventQuery")
    protected ShardQueryLogic logic;

    protected KryoDocumentDeserializer deserializer;

    private final DateFormat format = new SimpleDateFormat("yyyyMMdd");
    private Date startDate;
    private Date endDate;

    private final Map<String,String> extraParameters = new HashMap<>();
    private final Set<String> expectedResults = new HashSet<>();

    private boolean shouldReturnSomething;
    private boolean isShouldReturnSet;

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
    public void setup() throws ParseException {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        log.setLevel(Level.TRACE);
        logic.setFullTableScanEnabled(true);
        deserializer = new KryoDocumentDeserializer();
        startDate = format.parse("19000101");
        endDate = format.parse("20240101");
        extraParameters.clear();
        expectedResults.clear();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("return.fields", "SUMMARY");
        extraParameters.put("query.syntax", "LUCENE");
        isShouldReturnSet = false;
    }

    protected void updateQueryParam(String key, String value) {
        if (key != null && value != null && !key.isBlank() && !value.isBlank()) {
            extraParameters.put(key, value);
        }
    }

    protected void addExpectedResult(String result) {
        if (result != null && !result.isBlank()) {
            expectedResults.add(result);
        }
    }

    protected void setShouldReturnSomething(boolean shouldReturnSomething) {
        this.shouldReturnSomething = shouldReturnSomething;
        isShouldReturnSet = true;
    }

    protected abstract void runTestQuery(String queryString) throws Exception;

    protected void runTestQuery(AccumuloClient connector, String queryString) throws Exception {
        if (!isShouldReturnSet) {
            throw new Exception("\"ShouldReturnSomething\" was not set");
        }

        QueryImpl settings = new QueryImpl();
        settings.setBeginDate(startDate);
        settings.setEndDate(endDate);
        settings.setPagesize(Integer.MAX_VALUE);
        settings.setQueryAuthorizations(auths.serialize());
        settings.setQuery(queryString);
        settings.setParameters(extraParameters);
        settings.setId(UUID.randomUUID());

        log.debug("query: " + settings.getQuery());
        log.debug("logic: " + settings.getQueryLogicName());

        GenericQueryConfiguration config = logic.initialize(connector, settings, authSet);
        logic.setupQuery(config);

        Set<Document> docs = new HashSet<>();
        Set<String> unexpectedFields = new HashSet<>();
        for (Map.Entry<Key,Value> entry : logic) {
            Document d = deserializer.apply(entry).getValue();
            log.trace(entry.getKey() + " => " + d);
            docs.add(d);
            Map<String,Attribute<? extends Comparable<?>>> dictionary = d.getDictionary();

            log.debug("dictionary:" + dictionary);
            for (Map.Entry<String,Attribute<? extends Comparable<?>>> dictionaryEntry : dictionary.entrySet()) {

                // skip expected generated fields
                if (dictionaryEntry.getKey().equals(JexlEvaluation.HIT_TERM_FIELD) || dictionaryEntry.getKey().contains("ORIGINAL_COUNT")
                                || dictionaryEntry.getKey().equals("RECORD_ID")) {
                    continue;
                }

                Attribute<? extends Comparable<?>> attribute = dictionaryEntry.getValue();
                if (attribute instanceof Attributes) {
                    for (Attribute attr : ((Attributes) attribute).getAttributes()) {
                        String toFind = dictionaryEntry.getKey() + ":" + attr;
                        boolean found = expectedResults.remove(toFind);
                        if (found)
                            log.debug("removed " + toFind);
                        else {
                            unexpectedFields.add(toFind);
                        }
                    }
                } else {

                    String toFind = dictionaryEntry.getKey() + ":" + dictionaryEntry.getValue();

                    boolean found = expectedResults.remove(toFind);
                    if (found)
                        log.debug("removed " + toFind);
                    else {
                        unexpectedFields.add(toFind);
                    }
                }

            }
        }

        assertTrue("unexpected fields returned: " + unexpectedFields, unexpectedFields.isEmpty());
        assertTrue(expectedResults + " was not empty", expectedResults.isEmpty());

        if (shouldReturnSomething) {
            assertFalse("No docs were returned!", docs.isEmpty());
        } else {
            assertTrue("no docs should be returned!", docs.isEmpty());
        }
    }

    // TODO: remove @ignore after we can except no argument in function
    @Ignore
    @Test
    public void testWithNoArg() throws Exception {
        setShouldReturnSomething(true);

        // not sure why the timestamp and delete flag are present
        addExpectedResult(
                        "SUMMARY:CONTENT: You can get much farther with a kind word and a gun than you can with a kind word alone: : [] 9223372036854775807 false");

        runTestQuery("QUOTE:(farther) #SUMMARY()");
    }

    @Test
    public void testWithOnly() throws Exception {
        setShouldReturnSomething(true);

        // not sure why the timestamp and delete flag are present
        addExpectedResult("SUMMARY:CONTENT: You can get much farther with a kind word and a gu: : [] 9223372036854775807 false");

        runTestQuery("QUOTE:(farther) #SUMMARY(VIEWS:CONTENT/SIZE:50/ONLY)");
    }

    @Test
    public void testWithoutOnly() throws Exception {
        setShouldReturnSomething(true);

        // not sure why the timestamp and delete flag are present
        addExpectedResult("SUMMARY:CONTENT: You can get much farther with a kind word and a gu: : [] 9223372036854775807 false");

        runTestQuery("QUOTE:(farther) #SUMMARY(SIZE:50/VIEWS:CONTENT)");
    }

    @Test
    public void testSize() throws Exception {
        setShouldReturnSomething(true);

        // not sure why the timestamp and delete flag are present
        addExpectedResult("SUMMARY:CONTENT: You can get much farther with a kind word and a gu: : [] 9223372036854775807 false");

        runTestQuery("QUOTE:(farther) #SUMMARY(SIZE:50)");
    }

    @Test
    public void testOverMaxSize() throws Exception {
        setShouldReturnSomething(true);

        // not sure why the timestamp and delete flag are present
        addExpectedResult(
                        "SUMMARY:CONTENT: You can get much farther with a kind word and a gun than you can with a kind word alone: : [] 9223372036854775807 false");

        runTestQuery("QUOTE:(farther) #SUMMARY(SIZE:90000)");
    }

    @Test
    public void testNegativeSize() throws Exception {
        setShouldReturnSomething(true);

        // not sure why the timestamp and delete flag are present
        addExpectedResult("SUMMARY:CONTENT: Y: : [] 9223372036854775807 false");

        runTestQuery("QUOTE:(farther) #SUMMARY(SIZE:-50)");
    }

    @Test
    public void testNoContentFound() throws Exception {
        setShouldReturnSomething(true);

        addExpectedResult("SUMMARY:NO CONTENT FOUND TO SUMMARIZE");

        runTestQuery("QUOTE:(farther) #SUMMARY(SIZE:50/ONLY/VIEWS:CANTFINDME,ORME)");
    }

    @Test
    public void testSizeZero() throws Exception {
        setShouldReturnSomething(false);

        runTestQuery("QUOTE:(farther) #SUMMARY(SIZE:0)");
    }

    @Test
    public void testNoSizeButOtherOptions() throws Exception {
        setShouldReturnSomething(true);

        // not sure why the timestamp and delete flag are present
        addExpectedResult(
                        "SUMMARY:CONTENT: You can get much farther with a kind word and a gun than you can with a kind word alone: : [] 9223372036854775807 false");

        runTestQuery("QUOTE:(farther) #SUMMARY(VIEWS:TEST1,TEST2)");
    }

    @Test
    public void testBadOptionsFormat() throws Exception {
        setShouldReturnSomething(false);

        runTestQuery("QUOTE:(farther) #SUMMARY(SIZE:notanumber)");
    }

    @Test
    public void testOnlyWithNoOtherOptions() throws Exception {
        setShouldReturnSomething(true);

        addExpectedResult("SUMMARY:NO CONTENT FOUND TO SUMMARIZE");

        runTestQuery("QUOTE:(farther) #SUMMARY(ONLY)");
    }

    @Test
    public void testMultiView() throws Exception {
        setShouldReturnSomething(true);

        // not sure why the timestamp and delete flag are present
        addExpectedResult("SUMMARY:CONTENT: You can get much farther with a kind word and a gu"
                        + "\nCONTENT2: A lawyer and his briefcase can steal more than ten: : [] 9223372036854775807 false");

        runTestQuery("QUOTE:(farther) #SUMMARY(SIZE:50/VIEWS:CONTENT*/ONLY)");
    }
}
