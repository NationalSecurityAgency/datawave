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
import datawave.query.util.CommonalityTokenTestDataIngest;
import datawave.util.TableName;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;

/**
 * Tests the limit.fields feature to ensure that hit terms are always included and that associated fields at the same grouping context are included along with
 * the field that hit on the query. This test uses a dot delimited token in the event field name as a 'commonality token'. This test also validates that no
 * unexpected fields are returned.
 *
 */
public abstract class TestLimitReturnedGroupsToHitTermGroups {

    @RunWith(Arquillian.class)
    public static class ShardRange extends TestLimitReturnedGroupsToHitTermGroups {
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
        protected void runTestQuery(String queryString, Date startDate, Date endDate, Map<String,String> extraParms, Collection<String> goodResults)
                        throws Exception {
            super.runTestQuery(connector, queryString, startDate, endDate, extraParms, goodResults);
        }
    }

    @RunWith(Arquillian.class)
    public static class DocumentRange extends TestLimitReturnedGroupsToHitTermGroups {
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
        protected void runTestQuery(String queryString, Date startDate, Date endDate, Map<String,String> extraParms, Collection<String> goodResults)
                        throws Exception {
            super.runTestQuery(connector, queryString, startDate, endDate, extraParms, goodResults);
        }
    }

    private static final Logger log = Logger.getLogger(TestLimitReturnedGroupsToHitTermGroups.class);

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
        log.setLevel(Level.DEBUG);
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
        Assert.assertTrue("No docs were returned!", !docs.isEmpty());
    }

    @Test
    public void testOneGroup() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "BIRD=-1,CAT=-1,CANINE=-1,FISH=-1");

        // group 13
        String queryString = "CANINE == 'shepherd'";

        // definitely should NOT include group 3
        Set<String> goodResults = Sets.newHashSet("CANINE.PET.13:shepherd", "CAT.PET.13:ragdoll", "FISH.PET.13:tetra", "BIRD.PET.13:lovebird",
                        "REPTILE.PET.1:snake", "DOG.WILD.1:coyote", "SIZE.CANINE.3:20,12.5", "SIZE.CANINE.WILD.1:90,26.5");

        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, goodResults);
    }

    @Test
    public void testMultipleGroups() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "_ANYFIELD_=-1");

        String queryString = "filter:getAllMatches(CANINE,'.*e.*')";

        // definitely should NOT include group 2 or 3
        //@formatter:off
        Set<String> goodResults = Sets.newHashSet("CANINE.PET.0:beagle", "CAT.PET.0:tabby", "BIRD.PET.0:parakeet", "FISH.PET.0:beta",
                "CANINE.PET.1:basset", "CAT.PET.1:calico", "BIRD.PET.1:canary", "FISH.PET.1:goldfish",
                "CANINE.PET.12:bernese", "CAT.PET.12:himalayan", "BIRD.PET.12:cockatiel", "FISH.PET.12:swordtail",
                "CANINE.PET.13:shepherd", "CAT.PET.13:ragdoll", "BIRD.PET.13:lovebird", "FISH.PET.13:tetra",
                "CANINE.WILD.1:coyote", "CAT.WILD.1:tiger", "BIRD.WILD.1:hawk", "FISH.WILD.1:tuna",
                "REPTILE.PET.1:snake", "DOG.WILD.1:coyote");

        //@formatter:on
        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, goodResults);
    }

    @Test
    public void testMultipleGroupsWithRegexAndReturnFields() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "CANINE=-1");
        extraParameters.put("return.fields", "CANINE");

        String queryString = "filter:getAllMatches(CANINE,'.*e.*')";

        // definitely should NOT include group 2 or 3
        Set<String> goodResults = Sets.newHashSet("CANINE.PET.0:beagle", "CANINE.PET.1:basset", "CANINE.PET.12:bernese", "CANINE.PET.13:shepherd",
                        "CANINE.WILD.1:coyote");

        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, goodResults);
    }

    @Test
    public void testGroupWithRegexAndMatchesInGroup() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "CANINE=-1,BIRD=-1");
        extraParameters.put("return.fields", "CANINE,BIRD");

        String queryString = "CANINE =~ '.*a.*' AND grouping:matchesInGroup(CANINE, '.*a.*', BIRD, '.*o.*')";

        // definitely should NOT include group 3, 12, or 13. fox are crow are included because matchesInGroup cares about fieldname and subgroup, not group
        Set<String> goodResults = Sets.newHashSet("BIRD.WILD.2:crow", "CANINE.WILD.2:fox", "CANINE.PET.2:chihuahua", "BIRD.PET.2:parrot");
        // added because sorting query causes 'basset' to evaluate first
        goodResults.addAll(Sets.newHashSet("CANINE.PET.1:basset", "BIRD.PET.1:canary"));

        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, goodResults);
    }

    @Test
    public void testGroupWithExpandedRegexNaturalOrderAndMatchesInGroup() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "CANINE=-1,BIRD=-1");
        extraParameters.put("return.fields", "CANINE,BIRD");

        // this is the same query as above with terms in natural order after regex expansion
        String queryString = "(CANINE == 'chihuahua' || CANINE == 'beagle' || CANINE == 'dachshund' || CANINE == 'basset') && grouping:matchesInGroup(CANINE, '.*a.*', BIRD, '.*o.*')";

        // definitely should NOT include group 3, 12, or 13. fox are crow are included because matchesInGroup cares about fieldname and subgroup, not group
        Set<String> goodResults = Sets.newHashSet("BIRD.WILD.2:crow", "CANINE.WILD.2:fox", "CANINE.PET.2:chihuahua", "BIRD.PET.2:parrot");
        // added because sorting query causes 'basset' to evaluate first
        goodResults.addAll(Sets.newHashSet("CANINE.PET.1:basset", "BIRD.PET.1:canary"));

        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, goodResults);
    }

    @Test
    public void testGroupWithExpandedRegexAlphabeticalOrderAndMatchesInGroup() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "CANINE=-1,BIRD=-1");
        extraParameters.put("return.fields", "CANINE,BIRD");

        // this is the same query as above but union terms are ordered alphabetically
        String queryString = "(CANINE == 'basset' || CANINE == 'beagle' || CANINE == 'chihuahua' || CANINE == 'dachshund') && grouping:matchesInGroup(CANINE, '.*a.*', BIRD, '.*o.*')";

        // definitely should NOT include group 3, 12, or 13. fox are crow are included because matchesInGroup cares about fieldname and subgroup, not group
        Set<String> goodResults = Sets.newHashSet("BIRD.WILD.2:crow", "CANINE.WILD.2:fox", "CANINE.PET.2:chihuahua", "BIRD.PET.2:parrot");

        // extra groups when 'basset' sorts first
        // 'chihuahua' is from group 2, so when LimitFields reduces the document it only returns fields from group 2
        // when 'basset' sorts first it is from group 1, so LimitFields reduces the document fields to groups 1 and 2
        goodResults.addAll(Sets.newHashSet("CANINE.PET.1:basset", "BIRD.PET.1:canary"));

        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, goodResults);
    }

    @Test
    public void testGroupWithExpandedRegexAlphabeticalOrderAndMatchesInGroupPartTwo() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "CANINE=-1,BIRD=-1");
        extraParameters.put("return.fields", "CANINE,BIRD");

        // this is the same query as above but 'beagle' is sorted first on purpose to demonstrate group 0 influencing return fields
        String queryString = "(CANINE == 'beagle' || CANINE == 'basset' || CANINE == 'chihuahua' || CANINE == 'dachshund') && grouping:matchesInGroup(CANINE, '.*a.*', BIRD, '.*o.*')";

        // definitely should NOT include group 3, 12, or 13. fox are crow are included because matchesInGroup cares about fieldname and subgroup, not group
        Set<String> goodResults = Sets.newHashSet("BIRD.WILD.2:crow", "CANINE.WILD.2:fox", "CANINE.PET.2:chihuahua", "BIRD.PET.2:parrot");

        // similar to above, when 'beagle' sorts first group 0 is added to the HIT_TERM and LimitFields will return those fields
        goodResults.addAll(Sets.newHashSet("BIRD.PET.0:parakeet", "CANINE.PET.0:beagle"));

        // disable just for this test to prove group 0 can be returned
        logic.setSortQueryPreIndexWithFieldCounts(false);
        logic.setSortQueryPreIndexWithImpliedCounts(false);
        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, goodResults);
        logic.setSortQueryPreIndexWithFieldCounts(true);
        logic.setSortQueryPreIndexWithImpliedCounts(true);
    }

    @Test
    public void testMultipleIncludeGroupingFalse() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "false");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "CANINE=-1,BIRD=2");
        extraParameters.put("return.fields", "CANINE,BIRD");

        String queryString = "filter:getAllMatches(CANINE,'.*e.*')";

        Set<String> goodResults = Sets.newHashSet("BIRD:parakeet", "BIRD:canary", "CANINE:beagle", "CANINE:coyote", "CANINE:basset", "CANINE:shepherd",
                        "CANINE:bernese");

        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, goodResults);
    }

    @Test
    public void testLimiting() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "false");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "CANINE=1,BIRD=1,DOG=1");

        String queryString = "filter:getAllMatches(CANINE,'.*e.*')";

        // Only CANINE hits, 1 bird, and all fish, cats, dog, and reptile
        Set<String> goodResults = Sets.newHashSet("BIRD:parakeet", "CANINE:beagle", "CANINE:coyote", "CANINE:basset", "CANINE:shepherd", "CANINE:bernese",
                        "FISH:tuna", "CAT:tabby", "CAT:tom", "FISH:swordtail", "FISH:angelfish", "CAT:siamese", "FISH:goldfish", "CAT:himalayan", "CAT:leopard",
                        "CAT:cougar", "CAT:calico", "CAT:tiger", "FISH:tetra", "FISH:mackerel", "FISH:shark", "CAT:puma", "CAT:ragdoll", "FISH:beta",
                        "FISH:guppy", "FISH:salmon", "REPTILE:snake", "DOG:coyote", "SIZE:20,12.5", "SIZE:90,26.5");

        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, goodResults);
    }

    @Test
    public void testLimitingToZero() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "false");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "CANINE=0,BIRD=0,DOG=0");

        String queryString = "filter:getAllMatches(CANINE,'.*e.*')";

        // Only CANINE hits, 0 birds, and all fish and cats and reptile. The dog was not a hit.
        Set<String> goodResults = Sets.newHashSet("CANINE:beagle", "CANINE:coyote", "CANINE:basset", "CANINE:shepherd", "CANINE:bernese", "FISH:tuna",
                        "CAT:tabby", "CAT:tom", "FISH:swordtail", "FISH:angelfish", "CAT:siamese", "FISH:goldfish", "CAT:himalayan", "CAT:leopard",
                        "CAT:cougar", "CAT:calico", "CAT:tiger", "FISH:tetra", "FISH:mackerel", "FISH:shark", "CAT:puma", "CAT:ragdoll", "FISH:beta",
                        "FISH:guppy", "FISH:salmon", "REPTILE:snake", "SIZE:20,12.5", "SIZE:90,26.5");

        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, goodResults);
    }

    @Test
    public void testLimitingWithGrouping() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "CANINE=1,BIRD=1,DOG=1");

        String queryString = "filter:getAllMatches(CANINE,'.*e.*')";

        // CANINE hits along with the associated birds, all fish and cats and related dog and reptile
        Set<String> goodResults = Sets.newHashSet("CANINE.PET.0:beagle", "BIRD.PET.0:parakeet", "CANINE.PET.1:basset", "BIRD.PET.1:canary",
                        "CANINE.PET.12:bernese", "BIRD.PET.12:cockatiel", "CANINE.PET.13:shepherd", "BIRD.PET.13:lovebird", "CANINE.WILD.1:coyote",
                        "BIRD.WILD.1:hawk", "FISH.PET.12:swordtail", "CAT.PET.13:ragdoll", "FISH.WILD.0:shark", "CAT.PET.1:calico", "FISH.PET.0:beta",
                        "CAT.WILD.1:tiger", "FISH.PET.2:angelfish", "CAT.PET.0:tabby", "FISH.WILD.2:mackerel", "FISH.PET.13:tetra", "FISH.PET.1:goldfish",
                        "FISH.PET.3:guppy", "CAT.PET.12:himalayan", "FISH.WILD.1:tuna", "FISH.WILD.3:salmon", "CAT.WILD.3:puma", "CAT.WILD.2:leopard",
                        "CAT.PET.3:siamese", "CAT.WILD.0:cougar", "CAT.PET.2:tom", "REPTILE.PET.1:snake", "DOG.WILD.1:coyote", "SIZE.CANINE.3:20,12.5",
                        "SIZE.CANINE.WILD.1:90,26.5");

        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, goodResults);
    }

    @Test
    public void testLimitingToZeroWithGrouping() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "CANINE=0,BIRD=0,DOG=0");

        String queryString = "filter:getAllMatches(CANINE,'.*e.*')";

        // CANINE hits along with the associated birds, all fish and cats and related dog and reptile
        Set<String> goodResults = Sets.newHashSet("CANINE.PET.0:beagle", "BIRD.PET.0:parakeet", "CANINE.PET.1:basset", "BIRD.PET.1:canary",
                        "CANINE.PET.12:bernese", "BIRD.PET.12:cockatiel", "CANINE.PET.13:shepherd", "BIRD.PET.13:lovebird", "CANINE.WILD.1:coyote",
                        "BIRD.WILD.1:hawk", "FISH.PET.12:swordtail", "CAT.PET.13:ragdoll", "FISH.WILD.0:shark", "CAT.PET.1:calico", "FISH.PET.0:beta",
                        "CAT.WILD.1:tiger", "FISH.PET.2:angelfish", "CAT.PET.0:tabby", "FISH.WILD.2:mackerel", "FISH.PET.13:tetra", "FISH.PET.1:goldfish",
                        "FISH.PET.3:guppy", "CAT.PET.12:himalayan", "FISH.WILD.1:tuna", "FISH.WILD.3:salmon", "CAT.WILD.3:puma", "CAT.WILD.2:leopard",
                        "CAT.PET.3:siamese", "CAT.WILD.0:cougar", "CAT.PET.2:tom", "REPTILE.PET.1:snake", "DOG.WILD.1:coyote", "SIZE.CANINE.3:20,12.5",
                        "SIZE.CANINE.WILD.1:90,26.5");

        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, goodResults);
    }
}
