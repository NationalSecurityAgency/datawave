package datawave.query;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.Sets;

import datawave.configuration.spring.SpringBean;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.function.JexlEvaluation;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.util.CommonalityTokenTestDataIngest;
import datawave.test.JexlNodeAssert;
import datawave.util.TableName;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.configuration.GenericQueryConfiguration;

/**
 * Tests the limit.fields feature to ensure that hit terms are always included and that associated fields at the same grouping context are included along with
 * the field that hit on the query. This test uses a dot delimited token in the event field name as a 'commonality token'. This test also validates that no
 * unexpected fields are returned.
 *
 */
public abstract class NumericListQueryTest {

    @RunWith(Arquillian.class)
    public static class ShardRange extends NumericListQueryTest {
        protected static AccumuloClient connector = null;

        @BeforeClass
        public static void setUp() throws Exception {

            QueryTestTableHelper qtth = new QueryTestTableHelper(ShardRange.class.toString(), log);
            connector = qtth.client;

            CommonalityTokenTestDataIngest.writeItAll(connector, CommonalityTokenTestDataIngest.WhatKindaRange.SHARD);
            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(connector, auths, TableName.SHARD);
            PrintUtility.printTable(connector, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(connector, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        }

        @Override
        protected void runTestQuery(String queryString, String plan, Date startDate, Date endDate, Map<String,String> extraParms,
                        Collection<String> goodResults) throws Exception {
            super.runTestQuery(connector, queryString, plan, startDate, endDate, extraParms, goodResults);
        }
    }

    @RunWith(Arquillian.class)
    public static class DocumentRange extends NumericListQueryTest {
        protected static AccumuloClient connector = null;

        @BeforeClass
        public static void setUp() throws Exception {

            QueryTestTableHelper qtth = new QueryTestTableHelper(DocumentRange.class.toString(), log);
            connector = qtth.client;

            CommonalityTokenTestDataIngest.writeItAll(connector, CommonalityTokenTestDataIngest.WhatKindaRange.DOCUMENT);
            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(connector, auths, TableName.SHARD);
            PrintUtility.printTable(connector, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(connector, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        }

        @Override
        protected void runTestQuery(String queryString, String plan, Date startDate, Date endDate, Map<String,String> extraParms,
                        Collection<String> goodResults) throws Exception {
            super.runTestQuery(connector, queryString, plan, startDate, endDate, extraParms, goodResults);
        }
    }

    private static final Logger log = Logger.getLogger(NumericListQueryTest.class);

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
                        .deleteClass(datawave.query.metrics.QueryMetricQueryLogic.class).deleteClass(datawave.query.metrics.ShardTableQueryMetricHandler.class)
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
        log.setLevel(Level.DEBUG);
        logic.setFullTableScanEnabled(true);
        deserializer = new KryoDocumentDeserializer();
    }

    protected abstract void runTestQuery(String queryString, String plan, Date startDate, Date endDate, Map<String,String> extraParms,
                    Collection<String> goodResults) throws Exception;

    protected void runTestQuery(AccumuloClient connector, String queryString, String plan, Date startDate, Date endDate, Map<String,String> extraParms,
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
        JexlNodeAssert.assertThat(JexlASTHelper.parseJexlQuery(config.getQueryString())).isEqualTo(plan);

        Set<Document> docs = new HashSet<>();
        Set<String> unexpectedFields = new HashSet<>();
        for (Entry<Key,Value> entry : logic) {
            Document d = deserializer.apply(entry).getValue();
            log.trace(entry.getKey() + " => " + d);
            docs.add(d);
            Map<String,Attribute<? extends Comparable<?>>> dictionary = d.getDictionary();

            log.debug("dictionary:" + dictionary);
            for (Entry<String,Attribute<? extends Comparable<?>>> dictionaryEntry : dictionary.entrySet()) {

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

        Assert.assertTrue(goodResults + " was not empty", goodResults.isEmpty());
        Assert.assertTrue("unexpected fields returned: " + unexpectedFields.toString(), unexpectedFields.isEmpty());
    }

    @Test
    public void testEquals() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "SIZE=-1,BIRD=-1,CAT=-1,CANINE=-1,FISH=-1");

        String queryString = "SIZE == '90'";
        String expectedQueryPlan = "SIZE == '+bE9'";

        Set<String> goodResults = Sets.newHashSet("REPTILE.PET.1:snake", "SIZE.CANINE.WILD.1:90,26.5", "DOG.WILD.1:coyote");

        runTestQuery(queryString, expectedQueryPlan, format.parse("20091231"), format.parse("20150101"), extraParameters, goodResults);
    }

    @Test
    public void testOneValGreaterThan() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "SIZE=-1,BIRD=-1,CAT=-1,CANINE=-1,FISH=-1");

        String queryString = "SIZE > '89'";
        String expectedQueryPlan = "SIZE > '+bE8.9'";

        Set<String> goodResults = Sets.newHashSet("REPTILE.PET.1:snake", "SIZE.CANINE.WILD.1:90,26.5", "DOG.WILD.1:coyote");

        runTestQuery(queryString, expectedQueryPlan, format.parse("20091231"), format.parse("20150101"), extraParameters, goodResults);
    }

    @Test
    public void testOneValLessThan() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "SIZE=-1,BIRD=-1,CAT=-1,CANINE=-1,FISH=-1");

        String queryString = "SIZE < '13'";
        String expectedQueryPlan = "SIZE < '+bE1.3'";

        Set<String> goodResults = Sets.newHashSet("SIZE.CANINE.3:20,12.5", "REPTILE.PET.1:snake", "DOG.WILD.1:coyote");

        runTestQuery(queryString, expectedQueryPlan, format.parse("20091231"), format.parse("20150101"), extraParameters, goodResults);
    }

    @Test
    public void testSeveralLessThan() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "SIZE=-1,BIRD=-1,CAT=-1,CANINE=-1,FISH=-1");

        String queryString = "SIZE < '90'";
        String expectedQueryPlan = "SIZE < '+bE9'";

        // only includes one list group because HitListArithmetic exhaustiveHits is false, so it short circuit
        Set<String> goodResults = Sets.newHashSet("SIZE.CANINE.3:20,12.5", "REPTILE.PET.1:snake", "DOG.WILD.1:coyote");

        runTestQuery(queryString, expectedQueryPlan, format.parse("20091231"), format.parse("20150101"), extraParameters, goodResults);
    }

    @Test
    public void testSeveralGreaterThan() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "SIZE=-1,BIRD=-1,CAT=-1,CANINE=-1,FISH=-1");

        String queryString = "SIZE > '19'";
        String expectedQueryPlan = "SIZE > '+bE1.9'";

        // only includes one list group because HitListArithmetic exhaustiveHits is false, so it short circuit
        Set<String> goodResults = Sets.newHashSet("SIZE.CANINE.3:20,12.5", "REPTILE.PET.1:snake", "DOG.WILD.1:coyote");

        runTestQuery(queryString, expectedQueryPlan, format.parse("20091231"), format.parse("20150101"), extraParameters, goodResults);
    }

    @Test
    public void testANDSameField() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "SIZE=-1,BIRD=-1,CAT=-1,CANINE=-1,FISH=-1");

        String queryString = "SIZE == '90' AND SIZE == '26.5'";
        String expectedQueryPlan = "SIZE == '+bE9' && SIZE == '+bE2.65'";

        Set<String> goodResults = Sets.newHashSet("REPTILE.PET.1:snake", "SIZE.CANINE.WILD.1:90,26.5", "DOG.WILD.1:coyote");

        runTestQuery(queryString, expectedQueryPlan, format.parse("20091231"), format.parse("20150101"), extraParameters, goodResults);
    }

    @Test
    public void testANDDifferentField() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "SIZE=-1,BIRD=-1,CAT=-1,CANINE=-1,FISH=-1");

        String queryString = "SIZE == '90' AND SIZE == '20'";
        String expectedQueryPlan = "SIZE == '+bE9' && SIZE == '+bE2'";

        Set<String> goodResults = Sets.newHashSet("SIZE.CANINE.3:20,12.5", "REPTILE.PET.1:snake", "SIZE.CANINE.WILD.1:90,26.5", "DOG.WILD.1:coyote");

        runTestQuery(queryString, expectedQueryPlan, format.parse("20091231"), format.parse("20150101"), extraParameters, goodResults);
    }

    @Test
    public void testFieldEqualsList() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "SIZE=-1,BIRD=-1,CAT=-1,CANINE=-1,FISH=-1");

        String queryString = "SIZE == '90,26.5'";
        String expectedQueryPlan = "SIZE == '+bE9' && SIZE == '+bE2.65'";

        Set<String> goodResults = Sets.newHashSet("REPTILE.PET.1:snake", "SIZE.CANINE.WILD.1:90,26.5", "DOG.WILD.1:coyote");

        runTestQuery(queryString, expectedQueryPlan, format.parse("20091231"), format.parse("20150101"), extraParameters, goodResults);
    }

    @Test
    public void testIncludeList() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "SIZE=-1,BIRD=-1,CAT=-1,CANINE=-1,FISH=-1");

        String queryString = "CANINE == 'coyote' AND filter:includeRegex(SIZE,'90,26.5')";
        String expectedQueryPlan = "CANINE == 'coyote' && filter:includeRegex(SIZE, '90,26.5')";

        Set<String> goodResults = Sets.newHashSet("CAT.WILD.1:tiger", "CANINE.WILD.1:coyote", "REPTILE.PET.1:snake", "FISH.WILD.1:tuna", "BIRD.WILD.1:hawk",
                        "SIZE.CANINE.WILD.1:90,26.5", "DOG.WILD.1:coyote");

        runTestQuery(queryString, expectedQueryPlan, format.parse("20091231"), format.parse("20150101"), extraParameters, goodResults);
    }

    @Test
    public void testMatchesInGroup() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "SIZE=-1,BIRD=-1,CAT=-1,CANINE=-1,FISH=-1");

        String queryString = "SIZE =='90,26.5' AND grouping:matchesInGroup(SIZE, '90', SIZE, '26.5')";
        String expectedQueryPlan = "SIZE == '+bE9' && SIZE == '+bE2.65' && grouping:matchesInGroup(SIZE, '+bE9', SIZE, '+bE2.65')";

        Set<String> goodResults = Sets.newHashSet("REPTILE.PET.1:snake", "SIZE.CANINE.WILD.1:90,26.5", "DOG.WILD.1:coyote");

        runTestQuery(queryString, expectedQueryPlan, format.parse("20091231"), format.parse("20150101"), extraParameters, goodResults);
    }

    @Test
    public void testMatchesInGroupAcrossLists() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "SIZE=-1,BIRD=-1,CAT=-1,CANINE=-1,FISH=-1");

        String queryString = "SIZE =='90' AND grouping:matchesInGroup(SIZE, '90', SIZE, '20')";
        String expectedQueryPlan = "SIZE == '+bE9' && grouping:matchesInGroup(SIZE, '+bE9', SIZE, '+bE2')";

        //should be empty
        Set<String> goodResults = Sets.newHashSet();

        runTestQuery(queryString, expectedQueryPlan, format.parse("20091231"), format.parse("20150101"), extraParameters, goodResults);
    }

}
