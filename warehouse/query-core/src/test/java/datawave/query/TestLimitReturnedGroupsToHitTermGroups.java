package datawave.query;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.google.common.collect.Sets;

import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.function.JexlEvaluation;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.AbstractQueryTest;
import datawave.query.util.CommonalityTokenTestDataIngest;
import datawave.table.constants.TableName;
import datawave.test.HitTermAssertions;

/**
 * Tests the 'limit.fields' feature to ensure that hit terms are always included and that associated fields at the same grouping context are included along with
 * the field that hit on the query. This test uses a dot delimited token in the event field name as a 'commonality token'. This test also validates that no
 * unexpected fields are returned.
 */
@ExtendWith(SpringExtension.class)
@ComponentScan(basePackages = "datawave.query")
// @formatter:off
@ContextConfiguration(locations = {
        "classpath:datawave/query/QueryLogicFactory.xml",
        "classpath:beanRefContext.xml",
        "classpath:MarkingFunctionsContext.xml",
        "classpath:MetadataHelperContext.xml",
        "classpath:CacheContext.xml"})
// @formatter:on
public class TestLimitReturnedGroupsToHitTermGroups extends AbstractQueryTest {

    private static final Logger log = Logger.getLogger(TestLimitReturnedGroupsToHitTermGroups.class);
    private static final Authorizations auths = new Authorizations("ALL");

    private static AccumuloClient clientForTest;

    @Autowired
    @Qualifier("EventQuery")
    protected ShardQueryLogic logic;

    private final HitTermAssertions hitTermAssertions = new HitTermAssertions();
    private Set<String> expectedResults = new HashSet<>();

    @Override
    public ShardQueryLogic getLogic() {
        return logic;
    }

    @Override
    public Authorizations getAuths() {
        return auths;
    }

    @Override
    protected void extraConfigurations() {
        disableQueryPlanAssertion();
    }

    @Override
    protected void extraAssertions() {
        // planAndExecuteQuery() invokes extraAssertions() once per index table variant, so match against a
        // local copy rather than destructively consuming the shared expectedResults set.
        Set<String> remaining = new HashSet<>(expectedResults);
        Set<String> unexpectedFields = new HashSet<>();

        for (Document result : results) {
            Map<String,Attribute<? extends Comparable<?>>> dictionary = result.getDictionary();

            log.debug("dictionary:" + dictionary);
            for (Entry<String,Attribute<? extends Comparable<?>>> dictionaryEntry : dictionary.entrySet()) {

                // skip expected generated fields
                if (dictionaryEntry.getKey().equals(JexlEvaluation.HIT_TERM_FIELD) || dictionaryEntry.getKey().contains("ORIGINAL_COUNT")
                                || dictionaryEntry.getKey().equals("RECORD_ID")) {
                    continue;
                }

                Attribute<? extends Comparable<?>> attribute = dictionaryEntry.getValue();
                if (attribute instanceof Attributes) {
                    for (Attribute<?> attr : ((Attributes) attribute).getAttributes()) {
                        String toFind = dictionaryEntry.getKey() + ":" + attr;
                        if (!remaining.remove(toFind)) {
                            unexpectedFields.add(toFind);
                        }
                    }
                } else {
                    String toFind = dictionaryEntry.getKey() + ":" + dictionaryEntry.getValue();
                    if (!remaining.remove(toFind)) {
                        unexpectedFields.add(toFind);
                    }
                }
            }
        }

        assertTrue(remaining.isEmpty(), remaining + " was not empty");
        assertTrue(unexpectedFields.isEmpty(), "unexpected fields returned: " + unexpectedFields);
        assertFalse(results.isEmpty(), "No docs were returned!");

        boolean valid = hitTermAssertions.assertHitTerms(results);
        assertTrue(valid);
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));

        QueryTestTableHelper qtth = new QueryTestTableHelper(TestLimitReturnedGroupsToHitTermGroups.class.toString(), log);
        clientForTest = qtth.client;

        // ingest with the document range only; CommonalityTokenTestDataIngest already uses IndexIngestUtil
        // internally to derive the other shard index table variants that AbstractQueryTest iterates over.
        CommonalityTokenTestDataIngest.writeItAll(clientForTest, CommonalityTokenTestDataIngest.WhatKindaRange.DOCUMENT);

        // set to DEBUG if you want table output
        Logger.getLogger(PrintUtility.class).setLevel(Level.INFO);
        PrintUtility.printTable(clientForTest, auths, TableName.SHARD);
        PrintUtility.printTable(clientForTest, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(clientForTest, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
    }

    @AfterAll
    public static void afterAll() {
        TypeRegistry.reset();
    }

    @BeforeEach
    public void setup() {
        setClientForTest(clientForTest);
        log.setLevel(Level.DEBUG);
        logic.setFullTableScanEnabled(true);
        logic.setCollapseUids(false);

        hitTermAssertions.resetState();
        expectedResults = new HashSet<>();

        givenDate("20091231", "20150101");
        // every query requires hit list arithmetic, so set that once here
        givenParameter("hit.list", "true");
    }

    protected void withQuery(String query) {
        givenQuery(query);
    }

    protected void withParameter(String key, String value) {
        givenParameter(key, value);
    }

    protected void runTestQuery(Set<String> goodResults) throws Exception {
        this.expectedResults = goodResults;
        planAndExecuteQuery();
    }

    protected void assertFieldExists(String field) {
        for (Document result : results) {
            assertTrue(result.containsKey(field));
        }
    }

    protected void assertFieldDoesNotExist(String field) {
        for (Document result : results) {
            assertFalse(result.containsKey(field));
        }
    }

    @Test
    public void testOneGroup() throws Exception {
        withParameter("include.grouping.context", "true");
        withParameter("limit.fields", "BIRD=-1,CAT=-1,CANINE=-1,FISH=-1");

        // group 13
        withQuery("CANINE == 'shepherd'");
        hitTermAssertions.withRequiredAllOf("CANINE.PET.13:shepherd");

        // definitely should NOT include group 3
        Set<String> goodResults = Sets.newHashSet("CANINE.PET.13:shepherd", "CAT.PET.13:ragdoll", "FISH.PET.13:tetra", "BIRD.PET.13:lovebird",
                        "REPTILE.PET.1:snake", "DOG.WILD.1:coyote", "SIZE.CANINE.3:20,12.5", "SIZE.CANINE.WILD.1:90,26.5");

        runTestQuery(goodResults);
    }

    @Test
    public void testMultipleGroups() throws Exception {
        withParameter("include.grouping.context", "true");
        withParameter("limit.fields", "_ANYFIELD_=-1");

        withQuery("filter:getAllMatches(CANINE,'.*e.*')");
        hitTermAssertions.expectNoHitTerms();

        // definitely should NOT include group 2 or 3
        //@formatter:off
        Set<String> goodResults = Sets.newHashSet("CANINE.PET.0:beagle", "CAT.PET.0:tabby", "BIRD.PET.0:parakeet", "FISH.PET.0:beta",
                "CANINE.PET.1:basset", "CAT.PET.1:calico", "BIRD.PET.1:canary", "FISH.PET.1:goldfish",
                "CANINE.PET.12:bernese", "CAT.PET.12:himalayan", "BIRD.PET.12:cockatiel", "FISH.PET.12:swordtail",
                "CANINE.PET.13:shepherd", "CAT.PET.13:ragdoll", "BIRD.PET.13:lovebird", "FISH.PET.13:tetra",
                "CANINE.WILD.1:coyote", "CAT.WILD.1:tiger", "BIRD.WILD.1:hawk", "FISH.WILD.1:tuna",
                "REPTILE.PET.1:snake", "DOG.WILD.1:coyote", "CAT.PET.50:sphynx", "CANINE.PET.50:doberman");

        //@formatter:on
        runTestQuery(goodResults);
        assertFieldExists("HIT_TERM_ORIGINAL_COUNT"); // HIT_TERM got limited
        assertFieldDoesNotExist("HIT_TERM");
    }

    @Test
    public void testMultipleGroupsWithRegexAndReturnFields() throws Exception {
        withParameter("include.grouping.context", "true");
        withParameter("limit.fields", "CANINE=-1");
        withParameter("return.fields", "CANINE");

        withQuery("filter:getAllMatches(CANINE,'.*e.*')");

        hitTermAssertions.withOptionalAllOf("CANINE.PET.50:doberman");
        hitTermAssertions.withOptionalAllOf("CANINE.PET.1:basset", "CANINE.PET.13:shepherd", "CANINE.PET.0:beagle", "CANINE.PET.12:bernese",
                        "CANINE.WILD.1:coyote");

        // definitely should NOT include group 2 or 3
        Set<String> goodResults = Sets.newHashSet("CANINE.PET.0:beagle", "CANINE.PET.1:basset", "CANINE.PET.12:bernese", "CANINE.PET.13:shepherd",
                        "CANINE.WILD.1:coyote", "CANINE.PET.50:doberman");

        runTestQuery(goodResults);
    }

    @Test
    public void testGroupWithRegexAndMatchesInGroup() throws Exception {
        withParameter("include.grouping.context", "true");
        withParameter("limit.fields", "CANINE=-1,BIRD=-1");
        withParameter("return.fields", "CANINE,BIRD");

        withQuery("CANINE =~ '.*a.*' AND grouping:matchesInGroup(CANINE, '.*a.*', BIRD, '.*o.*')");
        hitTermAssertions.withRequiredAllOf("BIRD.WILD.2:crow", "CANINE.PET.1:basset", "CANINE.PET.2:chihuahua", "BIRD.PET.2:parrot");

        // definitely should NOT include group 3, 12, or 13. fox are crow are included because matchesInGroup cares about fieldname and subgroup, not group
        Set<String> goodResults = Sets.newHashSet("BIRD.WILD.2:crow", "CANINE.WILD.2:fox", "CANINE.PET.2:chihuahua", "BIRD.PET.2:parrot");
        // added because sorting query causes 'basset' to evaluate first
        goodResults.addAll(Sets.newHashSet("CANINE.PET.1:basset", "BIRD.PET.1:canary"));

        runTestQuery(goodResults);
    }

    @Test
    public void testGroupWithExpandedRegexNaturalOrderAndMatchesInGroup() throws Exception {
        withParameter("include.grouping.context", "true");
        withParameter("limit.fields", "CANINE=-1,BIRD=-1");
        withParameter("return.fields", "CANINE,BIRD");

        // this is the same query as above with terms in natural order after regex expansion
        withQuery("(CANINE == 'chihuahua' || CANINE == 'beagle' || CANINE == 'dachshund' || CANINE == 'basset') && grouping:matchesInGroup(CANINE, '.*a.*', BIRD, '.*o.*')");

        hitTermAssertions.withRequiredAllOf("BIRD.WILD.2:crow", "CANINE.PET.1:basset", "CANINE.PET.2:chihuahua", "BIRD.PET.2:parrot");

        // definitely should NOT include group 3, 12, or 13. fox are crow are included because matchesInGroup cares about fieldname and subgroup, not group
        Set<String> goodResults = Sets.newHashSet("BIRD.WILD.2:crow", "CANINE.WILD.2:fox", "CANINE.PET.2:chihuahua", "BIRD.PET.2:parrot");
        // added because sorting query causes 'basset' to evaluate first
        goodResults.addAll(Sets.newHashSet("CANINE.PET.1:basset", "BIRD.PET.1:canary"));

        runTestQuery(goodResults);
    }

    @Test
    public void testGroupWithExpandedRegexAlphabeticalOrderAndMatchesInGroup() throws Exception {
        withParameter("include.grouping.context", "true");
        withParameter("limit.fields", "CANINE=-1,BIRD=-1");
        withParameter("return.fields", "CANINE,BIRD");

        // this is the same query as above but union terms are ordered alphabetically
        withQuery("(CANINE == 'basset' || CANINE == 'beagle' || CANINE == 'chihuahua' || CANINE == 'dachshund') && grouping:matchesInGroup(CANINE, '.*a.*', BIRD, '.*o.*')");

        hitTermAssertions.withRequiredAllOf("BIRD.WILD.2:crow", "CANINE.PET.1:basset", "CANINE.PET.2:chihuahua", "BIRD.PET.2:parrot");

        // definitely should NOT include group 3, 12, or 13. fox are crow are included because matchesInGroup cares about fieldname and subgroup, not group
        Set<String> goodResults = Sets.newHashSet("BIRD.WILD.2:crow", "CANINE.WILD.2:fox", "CANINE.PET.2:chihuahua", "BIRD.PET.2:parrot");

        // extra groups when 'basset' sorts first
        // 'chihuahua' is from group 2, so when LimitFields reduces the document it only returns fields from group 2
        // when 'basset' sorts first it is from group 1, so LimitFields reduces the document fields to groups 1 and 2
        goodResults.addAll(Sets.newHashSet("CANINE.PET.1:basset", "BIRD.PET.1:canary"));

        runTestQuery(goodResults);
    }

    @Test
    public void testGroupWithExpandedRegexAlphabeticalOrderAndMatchesInGroupPartTwo() throws Exception {
        withParameter("include.grouping.context", "true");
        withParameter("limit.fields", "CANINE=-1,BIRD=-1");
        withParameter("return.fields", "CANINE,BIRD");

        // this is the same query as above but 'beagle' is sorted first on purpose to demonstrate group 0 influencing return fields
        withQuery("(CANINE == 'beagle' || CANINE == 'basset' || CANINE == 'chihuahua' || CANINE == 'dachshund') && grouping:matchesInGroup(CANINE, '.*a.*', BIRD, '.*o.*')");

        hitTermAssertions.withRequiredAllOf("BIRD.WILD.2:crow", "CANINE.PET.0:beagle", "CANINE.PET.2:chihuahua", "BIRD.PET.2:parrot");

        // definitely should NOT include group 3, 12, or 13. fox are crow are included because matchesInGroup cares about fieldname and subgroup, not group
        Set<String> goodResults = Sets.newHashSet("BIRD.WILD.2:crow", "CANINE.WILD.2:fox", "CANINE.PET.2:chihuahua", "BIRD.PET.2:parrot");

        // similar to above, when 'beagle' sorts first group 0 is added to the HIT_TERM and LimitFields will return those fields
        goodResults.addAll(Sets.newHashSet("BIRD.PET.0:parakeet", "CANINE.PET.0:beagle"));

        // disable just for this test to prove group 0 can be returned
        logic.setSortQueryPreIndexWithFieldCounts(false);
        logic.setSortQueryPreIndexWithImpliedCounts(false);
        runTestQuery(goodResults);
        logic.setSortQueryPreIndexWithFieldCounts(true);
        logic.setSortQueryPreIndexWithImpliedCounts(true);
    }

    @Test
    public void testMultipleIncludeGroupingFalse() throws Exception {
        withParameter("include.grouping.context", "false");
        withParameter("limit.fields", "CANINE=-1,BIRD=2");
        withParameter("return.fields", "CANINE,BIRD");

        withQuery("filter:getAllMatches(CANINE,'.*e.*')");

        hitTermAssertions.withOptionalAllOf("CANINE:coyote", "CANINE:shepherd", "CANINE:basset", "CANINE:bernese", "CANINE:beagle");
        hitTermAssertions.withOptionalAllOf("CANINE:doberman");

        Set<String> goodResults = Sets.newHashSet("BIRD:parakeet", "BIRD:canary", "CANINE:beagle", "CANINE:coyote", "CANINE:basset", "CANINE:shepherd",
                        "CANINE:bernese", "CANINE:doberman");

        runTestQuery(goodResults);
    }

    @Test
    public void testLimiting() throws Exception {
        withParameter("include.grouping.context", "false");
        withParameter("limit.fields", "CANINE=1,BIRD=1,DOG=1");

        withQuery("filter:getAllMatches(CANINE,'.*e.*')");

        hitTermAssertions.withOptionalAllOf("CANINE:coyote", "CANINE:shepherd", "CANINE:basset", "CANINE:bernese", "CANINE:beagle");
        hitTermAssertions.withOptionalAllOf("CANINE:doberman");

        // Only CANINE hits, 1 bird, and all fish, cats, dog, and reptile
        Set<String> goodResults = Sets.newHashSet("BIRD:parakeet", "CANINE:beagle", "CANINE:coyote", "CANINE:basset", "CANINE:shepherd", "CANINE:bernese",
                        "FISH:tuna", "CAT:tabby", "CAT:tom", "FISH:swordtail", "FISH:angelfish", "CAT:siamese", "FISH:goldfish", "CAT:himalayan", "CAT:leopard",
                        "CAT:cougar", "CAT:calico", "CAT:tiger", "FISH:tetra", "FISH:mackerel", "FISH:shark", "CAT:puma", "CAT:ragdoll", "FISH:beta",

                        "FISH:guppy", "FISH:salmon", "REPTILE:snake", "DOG:coyote", "SIZE:20,12.5", "SIZE:90,26.5", "CAT:sphynx", "CANINE:doberman");

        runTestQuery(goodResults);
    }

    @Test
    public void testLimitingToZero() throws Exception {
        withParameter("include.grouping.context", "false");
        withParameter("limit.fields", "CANINE=0,BIRD=0,DOG=0");

        withQuery("filter:getAllMatches(CANINE,'.*e.*')");

        hitTermAssertions.withOptionalAllOf("CANINE:coyote", "CANINE:shepherd", "CANINE:basset", "CANINE:bernese", "CANINE:beagle");
        hitTermAssertions.withOptionalAllOf("CANINE:doberman");

        // Only CANINE hits, 0 birds, and all fish and cats and reptile. The dog was not a hit.
        Set<String> goodResults = Sets.newHashSet("CANINE:beagle", "CANINE:coyote", "CANINE:basset", "CANINE:shepherd", "CANINE:bernese", "FISH:tuna",
                        "CAT:tabby", "CAT:tom", "FISH:swordtail", "FISH:angelfish", "CAT:siamese", "FISH:goldfish", "CAT:himalayan", "CAT:leopard",
                        "CAT:cougar", "CAT:calico", "CAT:tiger", "FISH:tetra", "FISH:mackerel", "FISH:shark", "CAT:puma", "CAT:ragdoll", "FISH:beta",

                        "FISH:guppy", "FISH:salmon", "REPTILE:snake", "SIZE:20,12.5", "SIZE:90,26.5", "CAT:sphynx", "CANINE:doberman");

        runTestQuery(goodResults);
    }

    @Test
    public void testLimitingWithGrouping() throws Exception {
        withParameter("include.grouping.context", "true");
        withParameter("limit.fields", "CANINE=1,BIRD=1,DOG=1");

        withQuery("filter:getAllMatches(CANINE,'.*e.*')");

        hitTermAssertions.withOptionalAllOf("CANINE.PET.1:basset", "CANINE.PET.13:shepherd", "CANINE.PET.0:beagle", "CANINE.PET.12:bernese",
                        "CANINE.WILD.1:coyote");
        hitTermAssertions.withOptionalAllOf("CANINE.PET.50:doberman");

        // CANINE hits along with the associated birds, all fish and cats and related dog and reptile
        Set<String> goodResults = Sets.newHashSet("CANINE.PET.0:beagle", "BIRD.PET.0:parakeet", "CANINE.PET.1:basset", "BIRD.PET.1:canary",
                        "CANINE.PET.12:bernese", "BIRD.PET.12:cockatiel", "CANINE.PET.13:shepherd", "BIRD.PET.13:lovebird", "CANINE.WILD.1:coyote",
                        "BIRD.WILD.1:hawk", "FISH.PET.12:swordtail", "CAT.PET.13:ragdoll", "FISH.WILD.0:shark", "CAT.PET.1:calico", "FISH.PET.0:beta",
                        "CAT.WILD.1:tiger", "FISH.PET.2:angelfish", "CAT.PET.0:tabby", "FISH.WILD.2:mackerel", "FISH.PET.13:tetra", "FISH.PET.1:goldfish",
                        "FISH.PET.3:guppy", "CAT.PET.12:himalayan", "FISH.WILD.1:tuna", "FISH.WILD.3:salmon", "CAT.WILD.3:puma", "CAT.WILD.2:leopard",

                        "CAT.PET.3:siamese", "CAT.WILD.0:cougar", "CAT.PET.2:tom", "REPTILE.PET.1:snake", "DOG.WILD.1:coyote", "SIZE.CANINE.3:20,12.5",
                        "SIZE.CANINE.WILD.1:90,26.5", "CAT.PET.50:sphynx", "CANINE.PET.50:doberman");

        runTestQuery(goodResults);
    }

    @Test
    public void testLimitingToZeroWithGrouping() throws Exception {
        withParameter("include.grouping.context", "true");
        withParameter("limit.fields", "CANINE=0,BIRD=0,DOG=0");

        withQuery("filter:getAllMatches(CANINE,'.*e.*')");
        hitTermAssertions.withOptionalAllOf("CANINE.PET.1:basset", "CANINE.PET.13:shepherd", "CANINE.PET.0:beagle", "CANINE.PET.12:bernese",
                        "CANINE.WILD.1:coyote");
        hitTermAssertions.withOptionalAllOf("CANINE.PET.50:doberman");

        // CANINE hits along with the associated birds, all fish and cats and related dog and reptile
        Set<String> goodResults = Sets.newHashSet("CANINE.PET.0:beagle", "BIRD.PET.0:parakeet", "CANINE.PET.1:basset", "BIRD.PET.1:canary",
                        "CANINE.PET.12:bernese", "BIRD.PET.12:cockatiel", "CANINE.PET.13:shepherd", "BIRD.PET.13:lovebird", "CANINE.WILD.1:coyote",
                        "BIRD.WILD.1:hawk", "FISH.PET.12:swordtail", "CAT.PET.13:ragdoll", "FISH.WILD.0:shark", "CAT.PET.1:calico", "FISH.PET.0:beta",
                        "CAT.WILD.1:tiger", "FISH.PET.2:angelfish", "CAT.PET.0:tabby", "FISH.WILD.2:mackerel", "FISH.PET.13:tetra", "FISH.PET.1:goldfish",
                        "FISH.PET.3:guppy", "CAT.PET.12:himalayan", "FISH.WILD.1:tuna", "FISH.WILD.3:salmon", "CAT.WILD.3:puma", "CAT.WILD.2:leopard",

                        "CAT.PET.3:siamese", "CAT.WILD.0:cougar", "CAT.PET.2:tom", "REPTILE.PET.1:snake", "DOG.WILD.1:coyote", "SIZE.CANINE.3:20,12.5",
                        "SIZE.CANINE.WILD.1:90,26.5", "CAT.PET.50:sphynx", "CANINE.PET.50:doberman");

        runTestQuery(goodResults);
    }
}
