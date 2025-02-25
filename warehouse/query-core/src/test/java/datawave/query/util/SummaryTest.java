package datawave.query.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
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
    public static class ShardRange extends SummaryTest {
        protected static AccumuloClient connector = null;

        @BeforeClass
        public static void setUp() throws Exception {

            QueryTestTableHelper qtth = new QueryTestTableHelper(ShardRange.class.toString(), log);
            connector = qtth.client;
            WiseGuysIngest.writeItAll(connector, WiseGuysIngest.WhatKindaRange.SHARD);
            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(connector, auths, TableName.SHARD);
            PrintUtility.printTable(connector, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(connector, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        }

        @Override
        protected void runTestQuery(String queryString, Date startDate, Date endDate, Map<String,String> extraParams, Collection<String> goodResults,
                        boolean shouldReturnSomething) throws Exception {
            super.runTestQuery(connector, queryString, startDate, endDate, extraParams, goodResults, shouldReturnSomething);
        }
    }

    @RunWith(Arquillian.class)
    public static class DocumentRange extends SummaryTest {
        protected static AccumuloClient connector = null;

        @BeforeClass
        public static void setUp() throws Exception {

            QueryTestTableHelper qtth = new QueryTestTableHelper(DocumentRange.class.toString(), log);
            connector = qtth.client;

            WiseGuysIngest.writeItAll(connector, WiseGuysIngest.WhatKindaRange.DOCUMENT);
            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(connector, auths, TableName.SHARD);
            PrintUtility.printTable(connector, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(connector, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        }

        @Override
        protected void runTestQuery(String queryString, Date startDate, Date endDate, Map<String,String> extraParams, Collection<String> goodResults,
                        boolean shouldReturnSomething) throws Exception {
            super.runTestQuery(connector, queryString, startDate, endDate, extraParams, goodResults, shouldReturnSomething);
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
        log.setLevel(Level.TRACE);
        logic.setFullTableScanEnabled(true);
        deserializer = new KryoDocumentDeserializer();
    }

    protected abstract void runTestQuery(String queryString, Date startDate, Date endDate, Map<String,String> extraParams, Collection<String> goodResults,
                    boolean shouldReturnSomething) throws Exception;

    protected void runTestQuery(AccumuloClient connector, String queryString, Date startDate, Date endDate, Map<String,String> extraParams,
                    Collection<String> goodResults, boolean shouldReturnSomething) throws Exception {

        QueryImpl settings = new QueryImpl();
        settings.setBeginDate(startDate);
        settings.setEndDate(endDate);
        settings.setPagesize(Integer.MAX_VALUE);
        settings.setQueryAuthorizations(auths.serialize());
        settings.setQuery(queryString);
        settings.setParameters(extraParams);
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
                        boolean found = goodResults.remove(toFind);
                        if (found)
                            log.debug("removed " + toFind);
                        else {
                            unexpectedFields.add(toFind);
                        }
                    }
                } else {

                    String toFind = dictionaryEntry.getKey() + ":" + dictionaryEntry.getValue();

                    boolean found = goodResults.remove(toFind);
                    if (found)
                        log.debug("removed " + toFind);
                    else {
                        unexpectedFields.add(toFind);
                    }
                }

            }
        }

        assertTrue("unexpected fields returned: " + unexpectedFields, unexpectedFields.isEmpty());
        assertTrue(goodResults + " was not empty", goodResults.isEmpty());

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
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("return.fields", "CONTENT_SUMMARY");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:(farther) #SUMMARY()";

        // not sure why the timestamp and delete flag are present
        Set<String> goodResults = new HashSet<>(Set.of(
                        "CONTENT_SUMMARY:CONTENT: You can get much farther with a kind word and a gun than you can with a kind word alone: : [] 9223372036854775807 false"));

        runTestQuery(queryString, format.parse("19000101"), format.parse("20240101"), extraParameters, goodResults, true);
    }

    @Test
    public void testWithNoActualArg() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("return.fields", "CONTENT_SUMMARY");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:(farther) #SUMMARY(/hello&%526++/@?Sy-;xtVrxHN;%)";

        // not sure why the timestamp and delete flag are present
        Set<String> goodResults = new HashSet<>(Set.of(
                        "CONTENT_SUMMARY:CONTENT: You can get much farther with a kind word and a gun than you can with a kind word alone: : [] 9223372036854775807 false"));

        runTestQuery(queryString, format.parse("19000101"), format.parse("20240101"), extraParameters, goodResults, true);
    }

    @Test
    public void testWithOnly() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("return.fields", "CONTENT_SUMMARY");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:(farther) #SUMMARY(VIEWS:CONTENT/SIZE:50/ONLY)";

        // not sure why the timestamp and delete flag are present
        Set<String> goodResults = new HashSet<>(
                        Set.of("CONTENT_SUMMARY:CONTENT: You can get much farther with a kind word and a gu: : [] 9223372036854775807 false"));

        runTestQuery(queryString, format.parse("19000101"), format.parse("20240101"), extraParameters, goodResults, true);
    }

    @Test
    public void testWithoutOnly() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("return.fields", "CONTENT_SUMMARY");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:(farther) #SUMMARY(SIZE:50/VIEWS:CONTENT)";

        // not sure why the timestamp and delete flag are present
        Set<String> goodResults = new HashSet<>(
                        Set.of("CONTENT_SUMMARY:CONTENT: You can get much farther with a kind word and a gu: : [] 9223372036854775807 false"));

        runTestQuery(queryString, format.parse("19000101"), format.parse("20240101"), extraParameters, goodResults, true);
    }

    @Test
    public void testSize() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("return.fields", "CONTENT_SUMMARY");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:(farther) #SUMMARY(SIZE:50)";

        // not sure why the timestamp and delete flag are present
        Set<String> goodResults = new HashSet<>(
                        Set.of("CONTENT_SUMMARY:CONTENT: You can get much farther with a kind word and a gu: : [] 9223372036854775807 false"));

        runTestQuery(queryString, format.parse("19000101"), format.parse("20240101"), extraParameters, goodResults, true);
    }

    @Test
    public void testOverMaxSize() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("return.fields", "CONTENT_SUMMARY");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:(farther) #SUMMARY(SIZE:90000)";

        // not sure why the timestamp and delete flag are present
        Set<String> goodResults = new HashSet<>(Set.of(
                        "CONTENT_SUMMARY:CONTENT: You can get much farther with a kind word and a gun than you can with a kind word alone: : [] 9223372036854775807 false"));

        runTestQuery(queryString, format.parse("19000101"), format.parse("20240101"), extraParameters, goodResults, true);
    }

    @Test
    public void testNegativeSize() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("return.fields", "CONTENT_SUMMARY");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:(farther) #SUMMARY(SIZE:-50)";

        // not sure why the timestamp and delete flag are present
        Set<String> goodResults = new HashSet<>(Set.of("CONTENT_SUMMARY:CONTENT: Y: : [] 9223372036854775807 false"));

        runTestQuery(queryString, format.parse("19000101"), format.parse("20240101"), extraParameters, goodResults, true);
    }

    @Test
    public void testNoContentFound() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("return.fields", "CONTENT_SUMMARY");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:(farther) #SUMMARY(SIZE:50/ONLY/VIEWS:CANTFINDME,ORME)";

        Set<String> goodResults = new HashSet<>(Set.of("CONTENT_SUMMARY:NO CONTENT FOUND TO SUMMARIZE"));

        runTestQuery(queryString, format.parse("19000101"), format.parse("20240101"), extraParameters, goodResults, true);
    }

    @Test
    public void testSizeZero() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("return.fields", "CONTENT_SUMMARY");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:(farther) #SUMMARY(SIZE:0)";

        // not sure why the timestamp and delete flag are present
        Set<String> goodResults = Collections.emptySet();
        runTestQuery(queryString, format.parse("19000101"), format.parse("20240101"), extraParameters, goodResults, false);
    }

    @Test
    public void testNoSizeButOtherOptions() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("return.fields", "CONTENT_SUMMARY");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:(farther) #SUMMARY(VIEWS:TEST1,TEST2)";

        // not sure why the timestamp and delete flag are present
        Set<String> goodResults = new HashSet<>(Set.of(
                        "CONTENT_SUMMARY:CONTENT: You can get much farther with a kind word and a gun than you can with a kind word alone: : [] 9223372036854775807 false"));

        runTestQuery(queryString, format.parse("19000101"), format.parse("20240101"), extraParameters, goodResults, true);
    }

    @Test
    public void testBadOptionsFormat() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("return.fields", "CONTENT_SUMMARY");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:(farther) #SUMMARY(SIZE:notanumber)";

        Set<String> goodResults = Collections.emptySet();

        runTestQuery(queryString, format.parse("19000101"), format.parse("20240101"), extraParameters, goodResults, false);
    }

    @Test
    public void testOnlyWithNoOtherOptions() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("return.fields", "CONTENT_SUMMARY");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:(farther) #SUMMARY(ONLY)";

        Set<String> goodResults = new HashSet<>(Set.of("CONTENT_SUMMARY:NO CONTENT FOUND TO SUMMARIZE"));

        runTestQuery(queryString, format.parse("19000101"), format.parse("20240101"), extraParameters, goodResults, true);
    }

    @Test
    public void testMultiView() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("return.fields", "CONTENT_SUMMARY");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:(farther) #SUMMARY(SIZE:50/VIEWS:CONTENT*/ONLY)";

        // not sure why the timestamp and delete flag are present
        Set<String> goodResults = new HashSet<>(Set.of("CONTENT_SUMMARY:CONTENT: You can get much farther with a kind word and a gu"
                        + "\nCONTENT2: A lawyer and his briefcase can steal more than ten: : [] 9223372036854775807 false"));

        runTestQuery(queryString, format.parse("19000101"), format.parse("20240101"), extraParameters, goodResults, true);
    }
}
