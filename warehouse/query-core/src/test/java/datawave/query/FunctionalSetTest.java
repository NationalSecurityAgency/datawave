package datawave.query;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import datawave.helpers.PrintUtility;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.AbstractQueryTest;
import datawave.query.util.WiseGuysIngest;
import datawave.table.constants.TableName;

/**
 * Loads some data in a mock accumulo table and then issues queries against the table using the shard query table.
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
public class FunctionalSetTest extends AbstractQueryTest {

    private static final Logger log = Logger.getLogger(FunctionalSetTest.class);
    private static final Authorizations auths = new Authorizations("ALL");

    private static AccumuloClient client;

    @Autowired
    @Qualifier("EventQuery")
    protected ShardQueryLogic logic;

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
        // no-op
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        QueryTestTableHelper qtth = new QueryTestTableHelper(FunctionalSetTest.class.toString(), log);
        client = qtth.client;

        WiseGuysIngest.writeItAll(client, WiseGuysIngest.WhatKindaRange.DOCUMENT);
        PrintUtility.printTable(client, auths, TableName.SHARD);
        PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
    }

    @BeforeEach
    public void setup() {
        setClientForTest(client);
        logic.setFullTableScanEnabled(true);
        logic.setRebuildDatatypeFilter(true);
        logic.setMaxEvaluationPipelines(1);
        logic.setMaxDepthThreshold(7);
    }

    private void expectResults(List<String> expected) {
        expectResultCount(expected.size());
        if (!expected.isEmpty()) {
            expectUUIDs(new HashSet<>(expected));
        }
    }

    private static Stream<Arguments> methodAsArgumentToMethodArgs() {
        // @formatter:off
        return Stream.of(
                // this makes sure that the JexlStringBuildingVisitor will parse the method as a method argument
                Arguments.of("AG.getValuesForGroups(grouping:getGroupsForMatchesInGroup(NAM, 'MEADOW', GEN, 'FEMALE')).isEmpty() == false && "
                        + "AG.getValuesForGroups(grouping:getGroupsForMatchesInGroup(NAM, 'MEADOW', GEN, 'FEMALE')).containsAll(AG.getValuesForGroups(grouping:getGroupsForMatchesInGroup(NAM, 'MEADOW', GEN, 'FEMALE'))) == true",
                        Arrays.asList("SOPRANO")));
        // @formatter:on
    }

    @ParameterizedTest
    @MethodSource("methodAsArgumentToMethodArgs")
    public void testMethodAsArgumentToMethod(String query, List<String> expected) throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        givenDate("20091231", "20150101");
        givenQuery(query);
        givenParameters(extraParameters);
        expectResults(expected);

        planAndExecuteQuery();
    }

    private static Stream<Arguments> minMaxArgs() {
        // @formatter:off
        return Stream.of(
                Arguments.of("AG.min() > 10", Arrays.asList("ANDOLINI", "SOPRANO", "CORLEONE", "CAPONE", "TATTAGLIA")), // model expands to AGE.min() > 10 || ETA.min() > 10
                Arguments.of("AG.max() == 40", Arrays.asList("CORLEONE", "CAPONE")),
                Arguments.of("AG.max() >= 40", Arrays.asList("CORLEONE", "CAPONE", "TATTAGLIA")),
                Arguments.of("AG.min() < 10", Arrays.asList()),

                Arguments.of("AG.greaterThan(39).size() >= 1", Arrays.asList("CORLEONE", "CAPONE", "TATTAGLIA")),
                Arguments.of("AG.compareWith(40,'==').size() == 1", Arrays.asList("CORLEONE", "CAPONE")),

                Arguments.of("BIRTH_DATE.min() < '1920-12-28T00:00:05.000Z'", Arrays.asList("CAPONE")),
                Arguments.of("DEATH_DATE.max() - BIRTH_DATE.min() > 1000*60*60*24", Arrays.asList("SOPRANO", "CORLEONE", "CAPONE", "ANDOLINI")), // one day
                Arguments.of("DEATH_DATE.max() - BIRTH_DATE.min() > 1000*60*60*24*5 + 1000*60*60*24*7", Arrays.asList("SOPRANO", "CORLEONE", "CAPONE", "ANDOLINI")), // 5 plus 7 days for the calculator-deprived
                Arguments.of("DEATH_DATE.min() < '20160301120000'", Arrays.asList("SOPRANO", "CORLEONE", "CAPONE", "ANDOLINI")),

                Arguments.of("AG.size() > 0", Arrays.asList("SOPRANO", "CORLEONE", "CAPONE", "ANDOLINI", "TATTAGLIA")), // model expands to AGE.size() > 0 || ETA.size() > 0
                Arguments.of("ETA.size() > 0", Arrays.asList("CORLEONE", "ANDOLINI")),
                Arguments.of("AGE.size() > 0", Arrays.asList("SOPRANO", "CAPONE", "TATTAGLIA")));
        // @formatter:on
    }

    @ParameterizedTest
    @MethodSource("minMaxArgs")
    public void testMinMax(String query, List<String> expected) throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        givenDate("20091231", "20150101");
        givenQuery(query);
        givenParameters(extraParameters);
        expectResults(expected);

        planAndExecuteQuery();
    }

    private static Stream<Arguments> functionsAsArgumentsArgs() {
        // @formatter:off
        return Stream.of(
                Arguments.of("((_Bounded_ = true) && (10 <= AG && AG <= 18))", Arrays.asList("SOPRANO", "CORLEONE", "ANDOLINI")), // "10 <= AG && AG <= 18"
                Arguments.of("((_Bounded_ = true) && (AG <= 18 && AG >= 10))", Arrays.asList("SOPRANO", "CORLEONE", "ANDOLINI")), // "10 <= AG && AG <= 18",
                Arguments.of("((_Bounded_ = true) && (18 >= AG && 10 <= AG))", Arrays.asList("SOPRANO", "CORLEONE", "ANDOLINI")), // "18 >= AG && 10 <= AG",
                Arguments.of("AG == 18", Arrays.asList("SOPRANO", "CORLEONE")), // "AGE == 18"
                Arguments.of("18 == AG", Arrays.asList("SOPRANO", "CORLEONE")), // "18 == AGE"
                // this succeeds because the literal 'FEMALE' is normalized to 'female' based on the type (LcNoDiacritics) of the GENDER field
                Arguments.of("GEN == 'FEMALE'", Arrays.asList("SOPRANO", "CORLEONE")), // "GENDER == 'FEMALE'"
                // this succeeds for the same reason as above. normalization was a no-op.
                Arguments.of("GEN == 'female'", Arrays.asList("SOPRANO", "CORLEONE")), // "GENDER == 'female'"
                Arguments.of("'female' == GEN", Arrays.asList("SOPRANO", "CORLEONE")), // "'female' == GENDER" - this succeeds because no normalization is necessary
                Arguments.of("'FEMALE' == GEN", Arrays.asList("SOPRANO", "CORLEONE")), // "'FEMALE' == GENDER"

                // the next one matches Meadow Soprano, age 18, because the 'MAGIC' value is 18 (we don't know/care what the actual value
                // of MAGIC is, only that whatever it is, it matches AGE in the same group as the other matches)
                Arguments.of("((_Bounded_ = true) && (AG > 10 && AG < 100)) && AG.getValuesForGroups(grouping:getGroupsForMatchesInGroup(NAM, 'MEADOW', GEN, 'FEMALE')) == MAGIC",
                        Arrays.asList("SOPRANO")), // "AGE.getValuesForGroups(grouping:getGroupsForMatchesInGroup(NAME, 'MEADOW', GENDER, 'FEMALE')) == MAGIC",

                // the next one matches Meadow Soprano, GENDER female, age 18 but not Constanza Corleone, Gender female, age 18
                // the < part of this is what is special. Other comparison operators should work the same way
                Arguments.of("((_Bounded_ = true) && (AG > 10 && AG < 100)) && AG.getValuesForGroups(grouping:getGroupsForMatchesInGroup(NAM, 'MEADOW', GEN, 'FEMALE')) < 19",
                        Arrays.asList("SOPRANO")), // "AGE.getValuesForGroups(grouping:getGroupsForMatchesInGroup(NAME, 'MEADOW', GENDER, 'FEMALE')) < 19"

                // the next 2 queries are equivalent. the reason for the functional query stuff is for when we
                // want to query with an operator other than '=='
                Arguments.of("((_Bounded_ = true) && (AG > 10 && AG < 100)) && AG.getValuesForGroups(grouping:getGroupsForMatchesInGroup(NAM, 'ALPHONSE', GEN, 'MALE')) == 30",
                        Arrays.asList("CAPONE")), // "AGE.getValuesForGroups(grouping:getGroupsForMatchesInGroup(NAME, 'ALPHONSE', GENDER, 'MALE')) == 30"
                Arguments.of("((_Bounded_ = true) && (AG > 10 && AG < 100)) && grouping:matchesInGroup(NAM, 'ALPHONSE', GEN, 'MALE', AG, 30)",
                        Arrays.asList("CAPONE")), // "grouping:matchesInGroup(NAME, 'ALPHONSE', GENDER, 'MALE', AGE, 30)"

                Arguments.of("((_Bounded_ = true) && (AG > 10 && AG < 100)) && filter:occurrence(AG, '==', filter:getAllMatches(AG, '16').size() + filter:getAllMatches(AG, '18').size())",
                        Arrays.asList("SOPRANO")), // will match only the sopranos
                Arguments.of("((_Bounded_ = true) && (AG > 10 && AG < 100)) && filter:occurrence(AG, '==', filter:getAllMatches(AG, '19').size() + filter:getAllMatches(AG, '18').size())",
                        Arrays.asList())); // will match none
        // @formatter:on
    }

    @ParameterizedTest
    @MethodSource("functionsAsArgumentsArgs")
    public void testFunctionsAsArguments(String query, List<String> expected) throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        // stat must be reset between each run when pruning ingest types
        logic.getConfig().setDatatypeFilter(Collections.emptySet());
        logic.getConfig().setIntermediateMaxTermThreshold(25);
        logic.getConfig().setIndexedMaxTermThreshold(25);
        logic.getConfig().setFinalMaxTermThreshold(25);

        givenDate("20091231", "20150101");
        givenQuery(query);
        givenParameters(extraParameters);
        expectResults(expected);

        planAndExecuteQuery();
    }

    private static Stream<Arguments> concatMethodsArgs() {
        // @formatter:off
        return Stream.of(
                Arguments.of("UUID == 'SOPRANO' && NAM.min().hashCode() != 0", Arrays.asList("SOPRANO")));
        // @formatter:on
    }

    @ParameterizedTest
    @MethodSource("concatMethodsArgs")
    public void testConcatMethods(String query, List<String> expected) throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");

        givenDate("20091231", "20150101");
        givenQuery(query);
        givenParameters(extraParameters);
        expectResults(expected);

        planAndExecuteQuery();
    }
}
