package datawave.query;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
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
import org.junit.Test;
import org.junit.runner.RunWith;

import datawave.configuration.spring.SpringBean;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.microservice.query.QueryImpl;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.function.JexlEvaluation;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.util.WiseGuysIngest;
import datawave.util.TableName;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;

public abstract class ExcerptTest {

    @RunWith(Arquillian.class)
    public static class ShardRange extends datawave.query.ExcerptTest {
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
        protected void runTestQuery(String queryString, Date startDate, Date endDate, Map<String,String> extraParms, Collection<String> goodResults)
                        throws Exception {
            super.runTestQuery(connector, queryString, startDate, endDate, extraParms, goodResults);
        }
    }

    @RunWith(Arquillian.class)
    public static class DocumentRange extends datawave.query.ExcerptTest {
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
        protected void runTestQuery(String queryString, Date startDate, Date endDate, Map<String,String> extraParms, Collection<String> goodResults)
                        throws Exception {
            super.runTestQuery(connector, queryString, startDate, endDate, extraParms, goodResults);
        }
    }

    private static final Logger log = Logger.getLogger(datawave.query.ExcerptTest.class);

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

    protected abstract void runTestQuery(String queryString, Date startDate, Date endDate, Map<String,String> extraParms, Collection<String> goodResults)
                    throws Exception;

    protected void runTestQuery(AccumuloClient connector, String queryString, Date startDate, Date endDate, Map<String,String> extraParms,
                    Collection<String> goodResults) throws Exception {

        QueryImpl settings = new QueryImpl();
        settings.setBeginDate(startDate);
        settings.setEndDate(endDate);
        settings.setPagesize(Integer.MAX_VALUE);
        settings.setQueryAuthorizations(auths.serialize());
        settings.setQuery(queryString);
        settings.setParameters(extraParms);
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

        assertFalse("No docs were returned!", docs.isEmpty());
    }

    @Test
    public void simpleTest() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("return.fields", "HIT_EXCERPT");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:(farther) #EXCERPT_FIELDS(QUOTE/2)";

        // not sure why the timestamp and delete flag are present
        Set<String> goodResults = new HashSet<>(Set.of("HIT_EXCERPT:get much [farther] with a: : [] 9223372036854775807 false"));

        runTestQuery(queryString, format.parse("19000101"), format.parse("20240101"), extraParameters, goodResults);
    }

    @Test
    public void simpleTestBefore() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("return.fields", "HIT_EXCERPT");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:(farther) #EXCERPT_FIELDS(QUOTE/2/before)";

        // not sure why the timestamp and delete flag are present
        Set<String> goodResults = new HashSet<>(Set.of("HIT_EXCERPT:get much [farther]: : [] 9223372036854775807 false"));

        runTestQuery(queryString, format.parse("19000101"), format.parse("20240101"), extraParameters, goodResults);
    }

    @Test
    public void simpleTestAfter() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("return.fields", "HIT_EXCERPT");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:(farther) #EXCERPT_FIELDS(QUOTE/2/after)";

        // not sure why the timestamp and delete flag are present
        Set<String> goodResults = new HashSet<>(Set.of("HIT_EXCERPT:[farther] with a: : [] 9223372036854775807 false"));

        runTestQuery(queryString, format.parse("19000101"), format.parse("20240101"), extraParameters, goodResults);
    }

    @Test
    public void lessSimpleBeforeTest() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("return.fields", "HIT_EXCERPT");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:(he cant refuse) #EXCERPT_FIELDS(QUOTE/2/before)";

        Set<String> goodResults = new HashSet<>(Set.of("HIT_EXCERPT:an offer [he] [cant] [refuse]: : [] 9223372036854775807 false"));

        runTestQuery(queryString, format.parse("19000101"), format.parse("20240101"), extraParameters, goodResults);
    }

    @Test
    public void lessSimpleAfterTest() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("return.fields", "HIT_EXCERPT");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:(he cant refuse) #EXCERPT_FIELDS(QUOTE/2/after)";

        Set<String> goodResults = new HashSet<>(Set.of("HIT_EXCERPT:[he] [cant] [refuse]: : [] 9223372036854775807 false"));

        runTestQuery(queryString, format.parse("19000101"), format.parse("20240101"), extraParameters, goodResults);
    }

    @Test
    public void lessSimpleTest() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("return.fields", "HIT_EXCERPT");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:(he cant refuse) #EXCERPT_FIELDS(QUOTE/2)";

        Set<String> goodResults = new HashSet<>(Set.of("HIT_EXCERPT:an offer [he] [cant] [refuse]: : [] 9223372036854775807 false"));

        runTestQuery(queryString, format.parse("19000101"), format.parse("20240101"), extraParameters, goodResults);
    }

    @Test
    public void biggerRangeThanQuoteLength() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("return.fields", "HIT_EXCERPT");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:(he cant refuse) #EXCERPT_FIELDS(QUOTE/20)";

        Set<String> goodResults = new HashSet<>(Set.of("HIT_EXCERPT:im gonna make him an offer [he] [cant] [refuse]: : [] 9223372036854775807 false"));

        runTestQuery(queryString, format.parse("19000101"), format.parse("20240101"), extraParameters, goodResults);
    }

    @Test
    public void biggerRangeThanQuoteLengthBeforeTest() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("return.fields", "HIT_EXCERPT");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:(he cant refuse) #EXCERPT_FIELDS(QUOTE/20/before)";

        Set<String> goodResults = new HashSet<>(Set.of("HIT_EXCERPT:im gonna make him an offer [he] [cant] [refuse]: : [] 9223372036854775807 false"));

        runTestQuery(queryString, format.parse("19000101"), format.parse("20240101"), extraParameters, goodResults);
    }

    @Test
    public void biggerRangeThanQuoteLengthAfterTest() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("return.fields", "HIT_EXCERPT");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:(he cant refuse) #EXCERPT_FIELDS(QUOTE/20/after)";

        Set<String> goodResults = new HashSet<>(Set.of("HIT_EXCERPT:[he] [cant] [refuse]: : [] 9223372036854775807 false"));

        runTestQuery(queryString, format.parse("19000101"), format.parse("20240101"), extraParameters, goodResults);
    }

    @Test
    public void wholeQuote() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("return.fields", "HIT_EXCERPT");
        extraParameters.put("query.syntax", "LUCENE");

        String queryString = "QUOTE:(im gonna make him an offer he cant refuse) #EXCERPT_FIELDS(QUOTE/20)";

        Set<String> goodResults = new HashSet<>(
                        Set.of("HIT_EXCERPT:[im] [gonna] [make] [him] [an] [offer] [he] [cant] [refuse]: : [] 9223372036854775807 false"));

        runTestQuery(queryString, format.parse("19000101"), format.parse("20240101"), extraParameters, goodResults);
    }

    @Test
    public void anotherFirstTerm() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("return.fields", "HIT_EXCERPT,UUID");
        extraParameters.put("query.syntax", "LUCENE");

        // "if" is the first term for one event
        String queryString = "QUOTE:(if) #EXCERPT_FIELDS(QUOTE/3)";

        Set<String> goodResults = new HashSet<>(Set.of("UUID.0:SOPRANO", "HIT_EXCERPT:[if] you can quote: : [] 9223372036854775807 false"));

        runTestQuery(queryString, format.parse("19000101"), format.parse("20240101"), extraParameters, goodResults);
    }

    @Test
    public void anotherFirstTermBeforeTest() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("return.fields", "HIT_EXCERPT,UUID");
        extraParameters.put("query.syntax", "LUCENE");

        // "if" is the first term for one event
        String queryString = "QUOTE:(if) #EXCERPT_FIELDS(QUOTE/3/before)";

        Set<String> goodResults = new HashSet<>(Set.of("UUID.0:SOPRANO", "HIT_EXCERPT:[if]: : [] 9223372036854775807 false"));

        runTestQuery(queryString, format.parse("19000101"), format.parse("20240101"), extraParameters, goodResults);
    }

    @Test
    public void anotherFirstTermAfterTest() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("return.fields", "HIT_EXCERPT,UUID");
        extraParameters.put("query.syntax", "LUCENE");

        // "if" is the first term for one event
        String queryString = "QUOTE:(if) #EXCERPT_FIELDS(QUOTE/3/after)";

        Set<String> goodResults = new HashSet<>(Set.of("UUID.0:SOPRANO", "HIT_EXCERPT:[if] you can quote: : [] 9223372036854775807 false"));

        runTestQuery(queryString, format.parse("19000101"), format.parse("20240101"), extraParameters, goodResults);
    }
}
