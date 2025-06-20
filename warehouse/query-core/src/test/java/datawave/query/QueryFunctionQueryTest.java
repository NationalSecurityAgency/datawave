package datawave.query;

import java.nio.file.Path;
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
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import datawave.helpers.PrintUtility;
import datawave.query.jexl.functions.QueryFunctions;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.AbstractQueryTest;
import datawave.query.util.WiseGuysIngest;
import datawave.table.constants.TableName;

/**
 * Integration test for {@link QueryFunctions}.
 * <p>
 * The following functions are tested
 * <ul>
 * <li>{@link QueryFunctions#INCLUDE_TEXT}</li>
 * <li>{@link QueryFunctions#MATCH_REGEX}</li>
 * </ul>
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
public class QueryFunctionQueryTest extends AbstractQueryTest {

    private static final Logger log = Logger.getLogger(QueryFunctionQueryTest.class);
    private static final Authorizations auths = new Authorizations("ALL");

    private static AccumuloClient client;

    @Autowired
    @Qualifier("EventQuery")
    protected ShardQueryLogic eventQueryLogic;

    @Override
    public ShardQueryLogic getLogic() {
        return eventQueryLogic;
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
    public static void beforeAll(@TempDir Path tempDir) throws Exception {
        System.setProperty("type.metadata.dir", tempDir.toFile().getCanonicalPath());

        QueryTestTableHelper qtth = new QueryTestTableHelper(QueryFunctionQueryTest.class.toString(), log);
        client = qtth.client;

        WiseGuysIngest.writeItAll(client, WiseGuysIngest.WhatKindaRange.DOCUMENT);
        PrintUtility.printTable(client, auths, TableName.SHARD);
        PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(client, auths, QueryTestTableHelper.METADATA_TABLE_NAME);
        PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
    }

    @BeforeEach
    public void setup() {
        setClientForTest(client);
        eventQueryLogic.setMaxEvaluationPipelines(1);
    }

    private void expectResults(List<String> expected) {
        expectResultCount(expected.size());
        if (!expected.isEmpty()) {
            expectUUIDs(new HashSet<>(expected));
        }
    }

    private static Stream<Arguments> phraseFunctionsWithHashesArgs() {
        // @formatter:off
        return Stream.of(
                // this is added to the new tfs added with dot notation check if they can even be queried
                Arguments.of("QUOTE == 'never' && QUOTE == 'refuse'", List.of("CORLEONE")),
                // check if a content phrase across these same dot notation fields can get a hit
                Arguments.of("content:phrase(termOffsetMap, 'i', 'never', 'refuse') && QUOTE == 'i' && QUOTE == 'never' && QUOTE == 'refuse'", List.of("CORLEONE")),
                // check that there is no cross contamination between the dot notation tfs and normal tfs
                Arguments.of("content:phrase(termOffsetMap, 'gonna', 'refuse') && QUOTE == 'gonna' && QUOTE == 'refuse'", Collections.emptyList()),
                // check that if no tf set with dot notation satisfies a query it will be short circuited in tf eval
                Arguments.of("content:phrase(termOffsetMap, 'never', 'offer') && QUOTE == 'never' && QUOTE == 'offer'", Collections.emptyList()),
                // verify tfs from another section of the query don't help this resolve
                Arguments.of("content:phrase(QUOTE, termOffsetMap, 'never', 'refuse') && QUOTE == 'never' && QUOTE == 'refuse' && content:phrase(PHILOSOPHY, termOffsetMap, 'absolute', 'power') && PHILOSOPHY == 'absolute' && PHILOSOPHY == 'power'", List.of("CORLEONE")),
                // invert the targets
                Arguments.of("content:phrase(PHILOSOPHY, termOffsetMap, 'never', 'refuse') && QUOTE == 'never' && QUOTE == 'refuse' && content:phrase(QUOTE, termOffsetMap, 'absolute', 'power') && PHILOSOPHY == 'absolute' && PHILOSOPHY == 'power'", Collections.emptyList()));
        // @formatter:on
    }

    @ParameterizedTest
    @MethodSource("phraseFunctionsWithHashesArgs")
    public void testPhraseFunctionsWithHashes(String query, List<String> expected) throws Exception {
        eventQueryLogic.setInitialMaxTermThreshold(20);
        eventQueryLogic.setIntermediateMaxTermThreshold(20);
        eventQueryLogic.setFinalMaxTermThreshold(20);

        givenDate("20091231", "20150101");
        givenQuery(query);
        expectResults(expected);

        planAndExecuteQuery();
    }

    private static Stream<Arguments> includeTextArgs() {
        // @formatter:off
        return Stream.of(
                Arguments.of("UUID == 'corleone' && f:includeText(GENERE, 'FEMALE')", List.of("CORLEONE")),
                Arguments.of("UUID == 'corleone' && f:includeText(GENERE, 'male')", Collections.emptyList()), //  misses because includeText is case-sensitive
                Arguments.of("UUID == 'corleone' && f:includeText(NUMBER, '25')", List.of("CORLEONE")));
        // @formatter:on
    }

    @ParameterizedTest
    @MethodSource("includeTextArgs")
    public void testIncludeText(String query, List<String> expected) throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("hit.list", "true");

        givenDate("20091231", "20150101");
        givenQuery(query);
        givenParameters(extraParameters);
        expectResults(expected);

        planAndExecuteQuery();
    }

    private static Stream<Arguments> matchRegexArgs() {
        // @formatter:off
        return Stream.of(
                Arguments.of("UUID == 'corleone' && f:matchRegex(GENERE, '.*MALE')", List.of("CORLEONE")),
                Arguments.of("UUID == 'corleone' && f:matchRegex(GENERE, '.*male')", List.of("CORLEONE")),
                Arguments.of("UUID == 'corleone' && f:matchRegex(NUMBER, '2.*')", List.of("CORLEONE")),
                Arguments.of("UUID == 'corleone' && f:matchRegex(GENERE, '[A-Z]+')", List.of("CORLEONE")));
        // @formatter:on
    }

    @ParameterizedTest
    @MethodSource("matchRegexArgs")
    public void testMatchRegex(String query, List<String> expected) throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("hit.list", "true");

        givenDate("20091231", "20150101");
        givenQuery(query);
        givenParameters(extraParameters);
        expectResults(expected);

        planAndExecuteQuery();
    }
}
