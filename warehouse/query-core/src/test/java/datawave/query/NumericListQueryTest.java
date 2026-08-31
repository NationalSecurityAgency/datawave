package datawave.query;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
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

/**
 * Tests the limit.fields feature to ensure that hit terms are always included and that associated fields at the same grouping context are included along with
 * the field that hit on the query. This test uses a dot delimited token in the event field name as a 'commonality token'. This test also validates that no
 * unexpected fields are returned.
 *
 */
@ExtendWith(SpringExtension.class)
@ComponentScan(basePackages = {"datawave.configuration.spring", "datawave.query"})
// @formatter:off
@ContextConfiguration(locations = {
        "classpath:datawave/query/QueryLogicFactory.xml",
        "classpath:beanRefContext.xml",
        "classpath:MarkingFunctionsContext.xml",
        "classpath:MetadataHelperContext.xml",
        "classpath:CacheContext.xml"})
// @formatter:on
public class NumericListQueryTest extends AbstractQueryTest {

    private static final Logger log = Logger.getLogger(NumericListQueryTest.class);
    private static final Authorizations auths = new Authorizations("ALL");

    private static AccumuloClient clientForTest;

    @Autowired
    @Qualifier("EventQuery")
    protected ShardQueryLogic logic;

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
        // no-op; plan assertion enabled via expectPlan(...) in each test
    }

    @Override
    protected void extraAssertions() {
        // planAndExecuteQuery() invokes extraAssertions() once per index table variant, so match against a
        // local copy rather than destructively consuming the shared expectedResults set.
        Set<String> remaining = new HashSet<>(expectedResults);
        Set<String> unexpectedFields = new HashSet<>();

        for (Document d : results) {
            Map<String,Attribute<? extends Comparable<?>>> dictionary = d.getDictionary();

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
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));

        QueryTestTableHelper qtth = new QueryTestTableHelper(NumericListQueryTest.class.toString(), log);
        clientForTest = qtth.client;

        // ingest with the document range only; CommonalityTokenTestDataIngest already uses IndexIngestUtil
        // internally to derive the other shard index table variants that AbstractQueryTest iterates over.
        CommonalityTokenTestDataIngest.writeItAll(clientForTest, CommonalityTokenTestDataIngest.WhatKindaRange.DOCUMENT);
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

        givenDate("20091231", "20150101");
    }

    private void runTestQuery(String queryString, String plan, Map<String,String> extraParms, Set<String> goodResults) throws Exception {
        this.expectedResults = goodResults;
        givenQuery(queryString);
        givenParameters(extraParms);
        expectPlan(plan);

        planAndExecuteQuery();
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

        runTestQuery(queryString, expectedQueryPlan, extraParameters, goodResults);
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

        runTestQuery(queryString, expectedQueryPlan, extraParameters, goodResults);
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

        runTestQuery(queryString, expectedQueryPlan, extraParameters, goodResults);
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

        runTestQuery(queryString, expectedQueryPlan, extraParameters, goodResults);
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

        runTestQuery(queryString, expectedQueryPlan, extraParameters, goodResults);
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

        runTestQuery(queryString, expectedQueryPlan, extraParameters, goodResults);
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

        runTestQuery(queryString, expectedQueryPlan, extraParameters, goodResults);
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

        runTestQuery(queryString, expectedQueryPlan, extraParameters, goodResults);
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

        runTestQuery(queryString, expectedQueryPlan, extraParameters, goodResults);
    }

    @Test
    public void testMatchesInGroup() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "SIZE=-1,BIRD=-1,CAT=-1,CANINE=-1,FISH=-1");

        String queryString = "SIZE =='90,26.5' AND grouping:matchesInGroup(SIZE, '90', SIZE, '26\\.5')";
        String expectedQueryPlan = "SIZE == '+bE9' && SIZE == '+bE2.65' && grouping:matchesInGroup(SIZE, '\\+bE9', SIZE, '\\+bE2\\.65')";

        Set<String> goodResults = Sets.newHashSet("REPTILE.PET.1:snake", "SIZE.CANINE.WILD.1:90,26.5", "DOG.WILD.1:coyote");

        runTestQuery(queryString, expectedQueryPlan, extraParameters, goodResults);
    }

    @Test
    public void testPushDown() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "SIZE=-1,CANINE=-1");
        extraParameters.put("return.fields", "SIZE,CANINE");

        String queryString = "((_Eval_ = true) && (SIZE == 90)) && CANINE == 'coyote'";
        String expectedQueryPlan = "((_Eval_ = true) && (SIZE == '+bE9')) && CANINE == 'coyote'";

        Set<String> goodResults = Sets.newHashSet("SIZE.CANINE.WILD.1:90,26.5", "CANINE.WILD.1:coyote");

        runTestQuery(queryString, expectedQueryPlan, extraParameters, goodResults);
    }

    @Test
    public void testWildcards() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "SIZE=-1,BIRD=-1,CAT=-1,CANINE=-1,FISH=-1");

        // this only works because the entire string '90,26.5' is included in the jexl context and we match against that
        String queryString = "SIZE =~'.*0.*' AND CANINE == 'coyote'";
        String expectedQueryPlan = "((_Delayed_ = true) && (SIZE =~ '\\+[a-zA-Z]E.*0?\\.?.*|\\+AE0|![A-Za-z]E(.+|.*9\\.?.+)')) && ((_Eval_ = true) && (SIZE =~ '.*0.*')) && CANINE == 'coyote'";

        Set<String> goodResults = Sets.newHashSet("REPTILE.PET.1:snake", "DOG.WILD.1:coyote", "CAT.WILD.1:tiger", "SIZE.CANINE.3:20,12.5",
                        "CANINE.WILD.1:coyote", "FISH.WILD.1:tuna", "BIRD.WILD.1:hawk");

        runTestQuery(queryString, expectedQueryPlan, extraParameters, goodResults);
    }

    @Test
    public void testMoreWildcards() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "SIZE=-1,BIRD=-1,CAT=-1,CANINE=-1,FISH=-1");

        String queryString = "SIZE =~ '20*'";
        String expectedQueryPlan = "((_Eval_ = true) && (SIZE =~ '20*'))";

        Set<String> goodResults = Sets.newHashSet();

        runTestQuery(queryString, expectedQueryPlan, extraParameters, goodResults);
    }

    @Test
    public void testLeadingWildcardNonReverseIndexed() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("limit.fields", "SIZE=-1,BIRD=-1,CAT=-1,CANINE=-1,FISH=-1");

        // this only works because the entire string '90,26.5' ends in a 5. A query of .*0 will not return anything because the full string does not match and
        // 90 only exists by itself in the jexl context in its normalized form. Numeric regex handling should some day rectify this or perhaps we should
        // consider adding the non-normalized tokens to the context for certain OneToMany types.
        String queryString = "SIZE =~'.*5' AND CANINE == 'coyote'";
        String expectedQueryPlan = "((_Eval_ = true) && (SIZE =~ '.*5')) && CANINE == 'coyote'";

        // AbstractQueryTest always forces hit-list on (unlike this test's original hit.list-less setup), which
        // widens limit.fields' group-retention so the CANINE=='coyote' hit's whole grouping-context group is
        // retained too, not just the fields the original hit.list-less assertion expected.
        Set<String> goodResults = Sets.newHashSet("REPTILE.PET.1:snake", "DOG.WILD.1:coyote", "CAT.WILD.1:tiger", "CANINE.WILD.1:coyote", "FISH.WILD.1:tuna",
                        "BIRD.WILD.1:hawk", "SIZE.CANINE.3:20,12.5");

        runTestQuery(queryString, expectedQueryPlan, extraParameters, goodResults);
    }

    @Test
    public void testMatchesInGroupAcrossLists() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "SIZE=-1,BIRD=-1,CAT=-1,CANINE=-1,FISH=-1");

        String queryString = "SIZE =='90' AND grouping:matchesInGroup(SIZE, '90', SIZE, '20')";
        String expectedQueryPlan = "SIZE == '+bE9' && grouping:matchesInGroup(SIZE, '\\+bE9', SIZE, '\\+bE2')";

        // should be empty
        Set<String> goodResults = Sets.newHashSet();

        runTestQuery(queryString, expectedQueryPlan, extraParameters, goodResults);
    }

}
