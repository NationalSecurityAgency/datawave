package datawave.query;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import datawave.ingest.data.TypeRegistry;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.query.planner.DatePartitionedQueryPlanner;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.AbstractQueryTest;
import datawave.query.util.WiseGuysIngest;

/**
 * Tests the composite functions, the #JEXL lucene function, the matchesAtLeastCountOf function. and others
 *
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
public class CompositeFunctionsTest extends AbstractQueryTest {

    private static final Logger log = Logger.getLogger(CompositeFunctionsTest.class);

    @TempDir
    public static Path folder;

    protected static AccumuloClient client = null;

    @Autowired
    @Qualifier("EventQuery")
    protected ShardQueryLogic eventQueryLogic;

    @Autowired
    @Qualifier("TLDEventQuery")
    protected ShardQueryLogic tldEventQueryLogic;

    @BeforeAll
    public static void beforeAll() throws Exception {
        // this will get property substituted into the TypeMetadataBridgeContext.xml file
        // for the injection test (when this unit test is first created)
        System.setProperty("type.metadata.dir", folder.toFile().getAbsolutePath());

        QueryTestTableHelper qtth = new QueryTestTableHelper(CompositeFunctionsTest.class.toString(), log);
        client = qtth.client;

        WiseGuysIngest.writeItAll(client, WiseGuysIngest.WhatKindaRange.SHARD);
        // PrintUtility.printTable(client, auths, TableName.SHARD);
        // PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
        // PrintUtility.printTable(client, auths, TableName.SHARD_DAY_INDEX);
        // PrintUtility.printTable(client, auths, TableName.SHARD_YEAR_INDEX);
        // PrintUtility.printTable(client, auths, QueryTestTableHelper.METADATA_TABLE_NAME);
        // PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
    }

    @BeforeEach
    public void beforeEach() {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));

        eventQueryLogic.setFullTableScanEnabled(true);
        eventQueryLogic.setMaxDepthThreshold(7);
        tldEventQueryLogic.setFullTableScanEnabled(true);
        tldEventQueryLogic.setMaxDepthThreshold(7);

        setClientForTest(client);
        givenDate("20091231", "20150101");
    }

    @AfterAll
    public static void afterAll() {
        TypeRegistry.reset();
    }

    @Override
    public ShardQueryLogic getLogic() {
        return eventQueryLogic;
    }

    @Override
    protected void extraConfigurations() {
        // no-op
    }

    @Override
    protected void extraAssertions() {
        // no-op
    }

    public void givenGroupingContext() {
        givenParameter("include.grouping.context", "true");
    }

    @Test
    public void testMatchesAtLeastCountOf_NAM() throws Exception {
        givenGroupingContext();
        givenQuery("UUID =~ '^[CS].*' AND filter:matchesAtLeastCountOf(3,NAM,'MICHAEL','VINCENT','FREDO','TONY')");
        expectPlan("(UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano') && filter:matchesAtLeastCountOf(3, (NOME || NAME), 'MICHAEL', 'VINCENT', 'FREDO', 'TONY')");
        expectUUIDs(Set.of("CORLEONE"));
        planAndExecuteQuery();
    }

    @Test
    public void testMatchesAtLeastCountOf_NAME() throws Exception {
        givenGroupingContext();
        givenQuery("UUID =~ '^[CS].*' AND filter:matchesAtLeastCountOf(3,NAME,'MICHAEL','VINCENT','FRED','TONY')");
        expectPlan("(UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano') && filter:matchesAtLeastCountOf(3, NAME, 'MICHAEL', 'VINCENT', 'FRED', 'TONY')");
        expectUUIDs(Collections.emptySet());
        planAndExecuteQuery();
    }

    @Test
    public void testMatchesAtLeastCountOfWithOptionsFunction_NAM() throws Exception {
        givenQuery("UUID =~ '^[CS].*' AND filter:matchesAtLeastCountOf(3,NAM,'MICHAEL','VINCENT','FREDO','TONY') AND f:options('include.grouping.context','true','hit.list','true')");
        expectPlan("(UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano') && filter:matchesAtLeastCountOf(3, (NOME || NAME), 'MICHAEL', 'VINCENT', 'FREDO', 'TONY')");
        expectUUIDs(Set.of("CORLEONE"));
        planAndExecuteQuery();
    }

    @Test
    public void testMatchesAtLeastCountOfWithOptionsFunction_NAME() throws Exception {
        givenQuery("UUID =~ '^[CS].*' AND filter:matchesAtLeastCountOf(3,NAME,'MICHAEL','VINCENT','FRED','TONY') OR f:options('include.grouping.context','true','hit.list','true')");
        expectPlan("(UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano') && filter:matchesAtLeastCountOf(3, NAME, 'MICHAEL', 'VINCENT', 'FRED', 'TONY')");
        expectUUIDs(Collections.emptySet());
        planAndExecuteQuery();
    }

    @Test
    public void testDateDeltaArithmeticEightyYearsOneResult() throws Exception {
        givenGroupingContext();
        givenQuery("UUID =~ '^[CS].*' AND filter:getMaxTime(DEATH_DATE) - filter:getMinTime(BIRTH_DATE) > 2522880000000L"); // 80+ years
        expectPlan("filter:getMaxTime(DEATH_DATE) - filter:getMinTime(BIRTH_DATE) > 2522880000000 && (UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano')");
        expectUUIDs(Set.of("CAPONE"));
        planAndExecuteQuery();
    }

    @Test
    public void testDateDeltaArithmeticSixtyYearsTwoResults() throws Exception {
        givenGroupingContext();
        givenQuery("UUID =~ '^[CS].*' AND filter:getMaxTime(DEATH_DATE) - filter:getMinTime(BIRTH_DATE) > 1892160000000L"); // 60+ years
        expectPlan("filter:getMaxTime(DEATH_DATE) - filter:getMinTime(BIRTH_DATE) > 1892160000000 && (UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano')");
        expectUUIDs(Set.of("CAPONE", "CORLEONE"));
        planAndExecuteQuery();
    }

    @Test
    public void testDateDeltaTimeFunction() throws Exception {
        givenGroupingContext();
        givenQuery("UUID =~ '^[CS].*' AND filter:timeFunction(DEATH_DATE,BIRTH_DATE,'-','>',2522880000000L)"); // 80+ years
        expectPlan("(UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano') && filter:timeFunction(DEATH_DATE, BIRTH_DATE, '-', '>', 2522880000000)");
        expectUUIDs(Set.of("CAPONE"));
        planAndExecuteQuery();
    }

    @Test
    public void testDateDeltaLucene() throws Exception {
        givenGroupingContext();
        givenParameter(QueryParameters.QUERY_SYNTAX, "LUCENE");
        givenQuery("(UUID:C* OR UUID:S*) AND #TIME_FUNCTION(DEATH_DATE,BIRTH_DATE,'-','>','2522880000000L')"); // 80+ years
        expectPlan("filter:timeFunction(DEATH_DATE, BIRTH_DATE, '-', '>', 2522880000000) && (((_Eval_ = true) && (UUID =~ 'c.*?')) || ((_Eval_ = true) && (UUID =~ 's.*?')))");
        expectUUIDs(Set.of("CAPONE"));
        planAndExecuteQuery();
    }

    @Test
    public void testAgainstUnsupportedCompositeStructures() {
        givenGroupingContext();
        givenQuery("UUID == 'CORLEONE' AND filter:getAllMatches(NAME,'SANTINO').add('NAME:GROUCHO') == true");
        assertThrows(DatawaveFatalQueryException.class, this::planAndExecuteQuery);
    }

    @Test
    public void testAgainstUnsupportedCompositeStructures2() {
        givenGroupingContext();
        givenQuery("UUID == 'CORLEONE' AND  filter:getAllMatches(NAME,'SANTINO').clear() == false");
        assertThrows(DatawaveFatalQueryException.class, this::planAndExecuteQuery);
    }

    @Test
    public void testWithIndexOnlyFieldsAndModelExpansion() throws Exception {
        givenGroupingContext();
        givenQuery("UUID =~ '^[CS].*' AND filter:includeRegex(LOCATION,'chicago')"); // LOCATION is index-only
        expectPlan("LOCATION == 'chicago' && (UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano')");
        expectUUIDs(Set.of("CAPONE"));
        planAndExecuteQuery();
    }

    @Test
    public void testWithIndexOnlyFieldsAndModelExpansionNewYork() throws Exception {
        givenGroupingContext();
        givenQuery("UUID =~ '^[CS].*' AND filter:includeRegex(LOC,'newyork')"); // LOC model-maps to LOCATION and POSIZIONE, both are index-only
        expectPlan("(POSIZIONE == 'newyork' || LOCATION =~ 'newyork') && (UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano')");
        expectUUIDs(Set.of("CORLEONE"));
        planAndExecuteQuery();
    }

    @Test
    public void testWithIndexOnlyFieldsAndModelExpansionNewYorkAndNewJersey() throws Exception {
        givenGroupingContext();
        givenQuery("UUID =~ '^[CS].*' AND filter:includeRegex(LOC,'new.*')"); // LOC model-maps to LOCATION and POSIZIONE, both are index-only
        expectPlan("(LOCATION == 'newjersey' || POSIZIONE == 'newyork') && (UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano')");
        expectUUIDs(Set.of("CORLEONE", "SOPRANO"));
        planAndExecuteQuery();
    }

    /**
     * The {@link datawave.query.jexl.visitors.RegexFunctionVisitor} enforces the rule that a regex filter function cannot contain an index-only field. The
     * excludeRegex function is rewritten as a negated regex, which is not executable.
     */
    @Test
    public void testWithIndexOnlyFieldsAndModelExpansionFailureState() {
        givenGroupingContext();
        // see above, but this will fail with the correct exception,because index-only fields
        // are mixed with expressions that cannot be run against the index
        givenQuery("UUID =~ '^[CS].*' AND filter:excludeRegex(LOC,'new.*')");
        // rewritten query is not executable:
        // (UUID == 'soprano' || UUID == 'corleone' || UUID == 'capone') && LOCATION !~ 'new.*' && POSIZIONE !~ 'new.*'
        expectUUIDs(Set.of("CAPONE"));
        assertThrows(DatawaveFatalQueryException.class, this::planAndExecuteQuery);
    }

    @Test
    public void testWithIndexOnlyFieldsAndModelExpansion5() throws Exception {
        givenGroupingContext();
        // this will expand to excludeRegex(NAME||NOME, 'A.*') which will become !includeRegex(NAME||NOME, 'A.*')
        givenQuery("UUID =~ '^[CS].*' AND filter:excludeRegex(NAM,'A.*')");
        expectPlan("(UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano') && filter:excludeRegex((NOME || NAME), 'A.*')");
        expectUUIDs(Set.of("CORLEONE"));
        planAndExecuteQuery();
    }

    @Test
    public void testArithmeticAddition() throws Exception {
        givenGroupingContext();
        givenQuery("UUID == 'CORLEONE' AND 1 + 1 + 1 == 3");
        expectPlan("1 + 1 + 1 == 3 && UUID == 'corleone'");
        expectUUIDs(Set.of("CORLEONE"));
        planAndExecuteQuery();
    }

    @Test
    public void testArithmeticMultiplication() throws Exception {
        givenGroupingContext();
        givenQuery("UUID =~ 'CORLEONE' AND 1 * 2 * 3 == 6");
        expectPlan("1 * 2 * 3 == 6 && UUID == 'corleone'");
        expectUUIDs(Set.of("CORLEONE"));
        planAndExecuteQuery();
    }

    @Test
    public void testArithmeticDivision() throws Exception {
        givenGroupingContext();
        givenQuery("UUID =~ 'CORLEONE' AND 12 / 2 / 3 == 2");
        expectPlan("12 / 2 / 3 == 2 && UUID == 'corleone'");
        expectUUIDs(Set.of("CORLEONE"));
        planAndExecuteQuery();
    }

    @Test
    public void testArithmeticAdditionMiss() throws Exception {
        givenGroupingContext();
        givenQuery("UUID == 'CORLEONE' AND 1 + 1 + 1 == 4");
        expectPlan("1 + 1 + 1 == 4 && UUID == 'corleone'");
        expectUUIDs(Collections.emptySet());
        planAndExecuteQuery();
    }

    @Test
    public void testArithmeticMultiplicationMiss() throws Exception {
        givenGroupingContext();
        givenQuery("UUID == 'CORLEONE' AND 1 * 2 * 3 == 7");
        expectPlan("1 * 2 * 3 == 7 && UUID == 'corleone'");
        expectUUIDs(Collections.emptySet());
        planAndExecuteQuery();
    }

    @Test
    public void testArithmeticDivisionMiss() throws Exception {
        givenGroupingContext();
        givenQuery("UUID == 'CORLEONE' AND 12 / 2 / 3 == 3");
        expectPlan("12 / 2 / 3 == 3 && UUID == 'corleone'");
        expectUUIDs(Collections.emptySet());
        planAndExecuteQuery();
    }

    @Test
    public void testArithmeticSetIsEmpty() throws Exception {
        givenGroupingContext();
        givenQuery("UUID == 'CORLEONE' AND filter:getAllMatches(NAM,'hubert').isEmpty() == true");
        expectPlan("UUID == 'corleone' && filter:getAllMatches((NOME || NAME), 'hubert').isEmpty() == true");
        expectUUIDs(Set.of("CORLEONE"));
        planAndExecuteQuery();
    }

    @Test
    public void testArithmeticSetSize() throws Exception {
        givenGroupingContext();
        givenQuery("UUID == 'CORLEONE' AND filter:getAllMatches(NAM,'hubert').size() == 0");
        expectPlan("UUID == 'corleone' && filter:getAllMatches((NOME || NAME), 'hubert').size() == 0");
        expectUUIDs(Set.of("CORLEONE"));
        planAndExecuteQuery();
    }

    @Test
    public void testNulls() throws Exception {
        // @formatter:off
        String[] queryStrings = {
                "UUID =~ '^[CS].*' AND filter:isNull(NULL1)", // no model expansion, NULL1 is not in the event(s)
                "UUID =~ '^[CS].*' AND filter:isNull(UUID)", // no model expansion, UUID is non-null in all events
                "UUID =~ '^[CS].*' AND filter:isNull(BOTH_NULL)", // expands to NULL1||NULL2, neither are in any events
                "filter:isNull(NULL2||NULL1)",
                "filter:isNull(BOTH_NULL)",
                "filter:isNull(UUID||NULL1)",
                "filter:isNull(UUID) && filter:isNull(NULL1)",
                "filter:isNull(NULL1||NULL2)",
                "filter:isNull(NULL1) && filter:isNull(NULL2)",
                "UUID =~ '^[CS].*' AND filter:isNull(ONE_NULL)",
                "UUID =~ '^[CS].*' AND filter:isNull(UUID||NULL1)",
                "UUID =~ '^[CS].*' AND filter:isNull(UUID) && filter:isNull(NULL1)"
        };
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {
                Arrays.asList("CORLEONE", "CAPONE", "SOPRANO"),
                Collections.emptyList(),
                Arrays.asList("CORLEONE", "CAPONE", "SOPRANO"),
                Arrays.asList("CORLEONE", "CAPONE", "SOPRANO", "ANDOLINI", "TATTAGLIA"),
                Arrays.asList("CORLEONE", "CAPONE", "SOPRANO", "ANDOLINI", "TATTAGLIA"),
                Collections.emptyList(),
                Collections.emptyList(),
                Arrays.asList("CORLEONE", "CAPONE", "SOPRANO", "ANDOLINI", "TATTAGLIA"),
                Arrays.asList("CORLEONE", "CAPONE", "SOPRANO", "ANDOLINI", "TATTAGLIA"),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList()
        };
        //  @formatter:on

        for (int i = 0; i < queryStrings.length; i++) {
            // filter must be reset between each run when pruning ingest types
            eventQueryLogic.getConfig().setDatatypeFilter(Collections.emptySet());
            beforeEach();
            givenGroupingContext();
            givenQuery(queryStrings[i]);
            disableQueryPlanAssertion();
            expectUUIDs(new HashSet<>(expectedLists[i]));
            planAndExecuteQuery();
            afterEach();
        }
    }

    @Test
    public void testNotNulls() throws Exception {
        // @formatter:off
        String[] queryStrings = {
                "filter:isNotNull(UUID)",
                "filter:isNotNull(NULL1)",
                // these are equivalent:
                "filter:isNotNull(NULL1||NULL2)",
                "filter:isNotNull(NULL1) || filter:isNotNull(NULL2)",
                "filter:isNotNull(BOTH_NULL)",
                // these are equivalent:
                "filter:isNotNull(UUID||NULL1)",
                "filter:isNotNull(UUID) || filter:isNotNull(NULL1)",
                "filter:isNotNull(ONE_NULL)",
                "UUID =~ '^[CS].*' AND filter:isNotNull(UUID)",
                "UUID =~ '^[CS].*' AND filter:isNotNull(NULL1)",
                "UUID =~ '^[CS].*' AND filter:isNotNull(NULL1||NULL2)",
                "UUID =~ '^[CS].*' AND filter:isNotNull(BOTH_NULL)",
                "UUID =~ '^[CS].*' AND filter:isNotNull(UUID||NULL1)",
                "UUID =~ '^[CS].*' AND filter:isNotNull(ONE_NULL)"
        };
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {
                Arrays.asList("CORLEONE", "CAPONE", "SOPRANO", "ANDOLINI", "TATTAGLIA"),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Arrays.asList("CORLEONE", "CAPONE", "SOPRANO", "ANDOLINI", "TATTAGLIA"),
                Arrays.asList("CORLEONE", "CAPONE", "SOPRANO", "ANDOLINI", "TATTAGLIA"),
                Arrays.asList("CORLEONE", "CAPONE", "SOPRANO", "ANDOLINI", "TATTAGLIA"),
                Arrays.asList("CORLEONE", "CAPONE", "SOPRANO"),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Arrays.asList("CORLEONE", "CAPONE", "SOPRANO"),
                Arrays.asList("CORLEONE", "CAPONE", "SOPRANO")
        };
        //  @formatter:on

        for (int i = 0; i < queryStrings.length; i++) {
            // filter must be reset between each run when pruning ingest types
            eventQueryLogic.getConfig().setDatatypeFilter(Collections.emptySet());
            beforeEach();
            givenGroupingContext();
            givenQuery(queryStrings[i]);
            disableQueryPlanAssertion();
            expectUUIDs(new HashSet<>(expectedLists[i]));
            planAndExecuteQuery();
            afterEach();
        }
    }

    @Test
    public void composeFunctionsInsteadOfMatchesAtLeastCountOf() throws Exception {
        givenGroupingContext();
        givenQuery("UUID =~ '^[CS].*' AND filter:includeRegex(NAM,'MICHAEL').size() + filter:includeRegex(NAM,'VINCENT').size() + filter:includeRegex(NAM,'FREDO').size() + filter:includeRegex(NAM,'TONY').size() >= 3");
        expectPlan("filter:includeRegex((NOME || NAME), 'MICHAEL').size() + filter:includeRegex((NOME || NAME), 'VINCENT').size() + filter:includeRegex((NOME || NAME), 'FREDO').size() + filter:includeRegex((NOME || NAME), 'TONY').size() >= 3 && (UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano')");
        expectUUIDs(Set.of("CORLEONE"));
        planAndExecuteQuery();
    }

    @Test
    public void composeFunctionsInsteadOfMatchesAtLeastCountOf2() throws Exception {
        givenGroupingContext();
        givenQuery("UUID =~ '^[CS].*' AND filter:includeRegex(NAM,'MICHAEL').size() + filter:includeRegex(NAM,'VINCENT').size() + filter:includeRegex(NAM,'FRED').size() + filter:includeRegex(NAM,'TONY').size() >= 3");
        expectPlan("filter:includeRegex((NOME || NAME), 'MICHAEL').size() + filter:includeRegex((NOME || NAME), 'VINCENT').size() + filter:includeRegex((NOME || NAME), 'FRED').size() + filter:includeRegex((NOME || NAME), 'TONY').size() >= 3 && (UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano')");
        expectUUIDs(Collections.emptySet());
        planAndExecuteQuery();
    }

    @Test
    public void testCompositeFunctions() throws Exception {
        // @formatter:off
        String[] queryStrings = {
                "UUID == 'SOPRANO' AND  1 + 1 == 2",
                "UUID == 'SOPRANO' AND  1 * 1 == 1",
                "filter:getAllMatches(NAM,'MICHAEL').size() + filter:getAllMatches(NAM,'SANTINO').size() >= 1  AND UUID =~ '^[CS].*'",
                "UUID =~ '^[CS].*' AND filter:getAllMatches(NAM,'MICHAEL').size() > 0",
                "UUID =~ '^[CS].*' AND filter:includeRegex(NAM,'MICHAEL').size() == 1",
                "UUID == 'CORLEONE' AND filter:getAllMatches(NAM,'SANTINO').size() == 1",
                "UUID =~ '^[CS].*' AND filter:getAllMatches(NAM,'MICHAEL').size() > 0 AND filter:getAllMatches(NAM,'MICHAEL').size() < 2",
                "UUID == 'SOPRANO' AND  filter:getAllMatches(NAM,'MICHAEL').contains('foo') == false",
                "UUID == 'SOPRANO' AND  filter:getAllMatches(NAM,'ANTHONY').contains('NAME.0:ANTHONY') == true",
                "UUID =~ '^[CS].*' AND  filter:getAllMatches(NAM,'.*O').contains('NOME.0:SANTINO') == true"
        };
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {
                Collections.singletonList("SOPRANO"), // family name starts with C or S
                Collections.singletonList("SOPRANO"), // family name starts with C or S
                Arrays.asList("CORLEONE", "CAPONE"),
                Arrays.asList("CORLEONE", "CAPONE"),
                Arrays.asList("CORLEONE", "CAPONE"),
                Collections.singletonList("CORLEONE"),
                Arrays.asList("CORLEONE", "CAPONE"),
                Collections.singletonList("SOPRANO"),
                Collections.singletonList("SOPRANO"),
                Collections.singletonList("CORLEONE")
        };
        //  @formatter:on

        for (int i = 0; i < queryStrings.length; i++) {
            beforeEach();
            givenGroupingContext();
            givenQuery(queryStrings[i]);
            disableQueryPlanAssertion();
            expectUUIDs(new HashSet<>(expectedLists[i]));
            planAndExecuteQuery();
            afterEach();
        }
    }

    @Test
    public void testMatchesAtLeastCountOfWithLucene() throws Exception {
        eventQueryLogic.setParser(new LuceneToJexlQueryParser());
        givenGroupingContext();
        givenQuery("(UUID:C* OR UUID:S*) AND #MATCHES_AT_LEAST_COUNT_OF('3',NAM,'MICHAEL','VINCENT','FREDO','TONY')");
        expectPlan("filter:matchesAtLeastCountOf(3, (NOME || NAME), 'MICHAEL', 'VINCENT', 'FREDO', 'TONY') && (((_Eval_ = true) && (UUID =~ 'c.*?')) || ((_Eval_ = true) && (UUID =~ 's.*?')))");
        expectUUIDs(Set.of("CORLEONE"));
        planAndExecuteQuery();
    }

    @Test
    public void testWithLucene() throws Exception {
        // @formatter:off
        String[] queryStrings = {
                "UUID:C*", // family name starts with 'C'
                "UUID:SOPRANO", // family name is SOPRANO
                "UUID:C* OR UUID:S* ", // family name starts with C or S
                "(UUID:C* OR UUID:S*) AND #INCLUDE(NAM, 'CONSTANZIA') ", // family has child CONSTANZIA
                "(UUID:C* OR UUID:S*) AND #INCLUDE(NAM, 'MICHAEL') ", // family has child MICHAEL
                "#JEXL(\"$UUID == 'CORLEONE' AND filter:getAllMatches(NAM,'SANTINO').size() == 1\")", // test LUCENE function to deliver jexl
                "UUID:CORLEONE AND #JEXL(\"filter:getAllMatches(NAM,'SANTINO').size() == 1\")"
        };
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {
                Arrays.asList("CAPONE", "CORLEONE"), // family name starts with 'C'
                Collections.singletonList("SOPRANO"), // family name is SOPRANO
                Arrays.asList("SOPRANO", "CORLEONE", "CAPONE"), // family name starts with C or S
                Collections.singletonList("CORLEONE"), // family has child CONSTANZIA
                Arrays.asList("CORLEONE", "CAPONE"), // family has child MICHAEL
                Collections.singletonList("CORLEONE"),
                Collections.singletonList("CORLEONE")
        };
        //  @formatter:on

        for (int i = 0; i < queryStrings.length; i++) {
            eventQueryLogic.setParser(new LuceneToJexlQueryParser());
            // filter must be reset between each run when pruning ingest types
            eventQueryLogic.getConfig().setDatatypeFilter(Collections.emptySet());
            beforeEach();
            givenGroupingContext();
            givenQuery(queryStrings[i]);
            disableQueryPlanAssertion();
            expectUUIDs(new HashSet<>(expectedLists[i]));
            planAndExecuteQuery();
            afterEach();
        }
    }

    @Test
    public void testTLDWithLuceneAndIdentifierToLiteralLTJexl() throws Exception {
        tldEventQueryLogic.setParser(new LuceneToJexlQueryParser());
        givenGroupingContext();
        givenQuery("UUID:ANDOLINI AND #JEXL(ETA < 15)"); // family name is ANDOLINI
        expectPlan("UUID == 'andolini' && ETA < '+bE1.5'");
        expectUUIDs(Set.of("CORLEONE"));
        planAndExecuteQuery(tldEventQueryLogic);
    }

    @Test
    public void testTLDWithLuceneAndIdentifierToLiteralEQJexl() throws Exception {
        tldEventQueryLogic.setParser(new LuceneToJexlQueryParser());
        givenGroupingContext();
        givenQuery("UUID:ANDOLINI AND #JEXL(ETA == 12)");
        expectPlan("ETA == '+bE1.2' && UUID == 'andolini'");
        expectUUIDs(Set.of("CORLEONE"));
        planAndExecuteQuery(tldEventQueryLogic);
    }

    @Test
    public void testWithHoles() throws Exception {
        // uncomment for full planning logs
        // log.setLevel(Level.DEBUG);
        // Logger.getLogger(DefaultQueryPlanner.class).setLevel(Level.DEBUG);
        // Logger.getLogger(RangeStream.class).setLevel(Level.DEBUG);
        eventQueryLogic.setQueryPlanner(new DatePartitionedQueryPlanner());
        givenGroupingContext();
        givenQuery("UUID == 'CORLEONE' AND  HOLE == 'FOO'");
        disableQueryPlanAssertion();
        expectUUIDs(Set.of("CORLEONE"));
        planAndExecuteQuery();

        String finalQueryPlan = getLogic().getConfig().getQueryString();
        assertTrue(finalQueryPlan.contains("(plan = 1) && (UUID == 'corleone' && (HOLE == 'FOO' || HOLE == 'foo')))"));
        assertTrue(finalQueryPlan
                        .contains("((plan = 2) && (UUID == 'corleone' && (((_Eval_ = true) && (HOLE == 'FOO')) || ((_Eval_ = true) && (HOLE == 'foo'))))"));
    }

    @Test
    public void testTLDWithLuceneAndIdentifierToIdentifierJexl() throws Exception {
        tldEventQueryLogic.setParser(new LuceneToJexlQueryParser());
        givenGroupingContext();
        givenQuery("UUID:ANDOLINI AND #JEXL(ETA < MAGIC)");
        expectPlan("UUID == 'andolini' && ((_Eval_ = true) && (ETA < MAGIC))");
        expectUUIDs(Set.of("CORLEONE"));
        planAndExecuteQuery(tldEventQueryLogic);
    }

    @Test
    public void testWithLuceneAndOptionsFunction() throws Exception {
        // @formatter:off
        String[] queryStrings = {
                "UUID:C* AND #OPTIONS('include.grouping.context', 'true')", // family name starts with 'C'
                "UUID:SOPRANO AND #OPTIONS('include.grouping.context', 'true')", // family name is SOPRANO
                "UUID:C* OR UUID:S*  AND #OPTIONS('include.grouping.context', 'true')", // family name starts with C or S
                "(UUID:C* OR UUID:S*) AND #INCLUDE(NAM, 'CONSTANZIA')  AND #OPTIONS('include.grouping.context', 'true')", // family has child CONSTANZIA
                "(UUID:C* OR UUID:S*) AND #INCLUDE(NAM, 'MICHAEL')  AND #OPTIONS('include.grouping.context', 'true')", // family has child MICHAEL
                "#JEXL(\"$UUID == 'CORLEONE' AND filter:getAllMatches(NAM,'SANTINO').size() == 1\") AND #OPTIONS('include.grouping.context', 'true')", // test LUCENE function to deliver jexl
                "UUID:CORLEONE AND #JEXL(\"filter:getAllMatches(NAM,'SANTINO').size() == 1\") AND #OPTIONS('include.grouping.context', 'true')"
        };
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {
                Arrays.asList("CAPONE", "CORLEONE"), // family name starts with 'C'
                Collections.singletonList("SOPRANO"), // family name is SOPRANO
                Arrays.asList("SOPRANO", "CORLEONE", "CAPONE"), // family name starts with C or S
                Collections.singletonList("CORLEONE"), // family has child CONSTANZIA
                Arrays.asList("CORLEONE", "CAPONE"), // family has child MICHAEL
                Collections.singletonList("CORLEONE"),
                Collections.singletonList("CORLEONE")
        };
        // @formatter:on
        for (int i = 0; i < queryStrings.length; i++) {
            beforeEach();
            getLogic().setParser(new LuceneToJexlQueryParser());
            givenQuery(queryStrings[i]);
            disableQueryPlanAssertion();
            expectUUIDs(new HashSet<>(expectedLists[i]));
            planAndExecuteQuery();
            afterEach();
        }
    }

    /**
     * Filter functions cannot be run against index-only fields. Support was added that will generate the index piece, which is fine, but ultimately defeats the
     * purpose of pushing a particular term into a filter function.
     */
    @Test
    public void testFilterFunctionsInvalidatedByIndexOnlyFields() {
        // @formatter:off
        String[] queries = {
                //  isNull
                "UUID == 'SOPRANO' && filter:isNull(LOCATION)",
                "UUID == 'SOPRANO' && filter:isNull(POSIZIONE)",
                "UUID == 'SOPRANO' && filter:isNull(NAM||LOCATION)",
                "UUID == 'SOPRANO' && filter:isNull((NAME||POSIZIONE))",
                //  isNotNull
                "UUID == 'SOPRANO' && filter:isNotNull(LOCATION)",  //  index-only field
                "UUID == 'SOPRANO' && filter:isNotNull(POSIZIONE)", //  alternate index-only field
                "UUID == 'SOPRANO' && filter:isNotNull(LOCATION||POSIZIONE)",   //  both index-only fields
                "UUID == 'SOPRANO' && filter:isNotNull(NAM||LOCATION)", //  one index-only field still fails validation
                "UUID == 'SOPRANO' && filter:isNotNull((NAME||POSIZIONE))",
                //  matchesAtLeastCountOf
                "UUID == 'SOPRANO' && filter:matchesAtLeastCountOf(2, LOCATION, 'chicago', 'newyork')",
                //  compare
                "UUID == 'SOPRANO' && filter:compare(LOCATION, '==', ANY, POSIZIONE)",
                "UUID == 'SOPRANO' && filter:compare(LOCATION, '==', ANY, NAME)",
                "UUID == 'SOPRANO' && filter:compare(NAME, '==', ANY, POSIZIONE)"
                //  includeRegex and excludeRegex handled by RegexFunctionVisitor
                //  includeText can handle index-only fields and should be a query function (see pr #1534)
        };
        //  @formatter:on

        for (String query : queries) {
            try {
                beforeEach();
                givenGroupingContext();
                givenQuery(query);
                planAndExecuteQuery();
                fail("query should not have run without throwing an exception: " + query);
            } catch (DatawaveFatalQueryException e) {
                String correctErrMsg = "datawave.webservice.query.exception.BadRequestQueryException: Invalid arguments to function. Filter function cannot evaluate against index-only field";
                assertTrue(e.getMessage().startsWith(correctErrMsg), "Expected query to fail: " + query);
            } catch (Exception e) {
                fail("Expected filter function with index-only fields to fail validation: " + query);
            } finally {
                afterEach();
            }
        }
    }

    @Test
    public void testMultiFieldInclude() throws Exception {
        eventQueryLogic.setParser(new LuceneToJexlQueryParser());
        givenQuery("UUID:SOPRANO AND #INCLUDE(LOCATION || POSIZIONE || NAME, 'newjersey')");
        expectPlan("(UUID == 'soprano' && ((_Delayed_ = true) && (NAME =~ 'newjersey'))) || (LOCATION == 'newjersey' && UUID == 'soprano') || (UUID == 'soprano' && POSIZIONE =~ 'newjersey')");
        expectUUIDs(Set.of("SOPRANO"));
        planAndExecuteQuery();
    }

    @Test
    public void testDelayedExceededValueThresholdRegexTFField() throws Exception {
        tldEventQueryLogic.setMaxDepthThreshold(9);
        tldEventQueryLogic.setMaxValueExpansionThreshold(1);

        givenQuery("UUID == 'CORLEONE' && ((_Delayed_ = true) && ((((_Value_ = true) && (QUOTE =~ 'h.*?')))))");
        expectPlan("UUID == 'corleone' && ((_Delayed_ = true) && ((((_Value_ = true) && (QUOTE =~ 'h.*?')))))");
        expectUUIDs(Set.of("CORLEONE"));
        planAndExecuteQuery(tldEventQueryLogic);
    }
}
