package datawave.query;

import java.nio.file.Path;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
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
import datawave.query.util.WiseGuysIngest.WhatKindaRange;
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
@ComponentScan(basePackages = "datawave.query")
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

    private static final Map<WhatKindaRange,AccumuloClient> clients = new EnumMap<>(WhatKindaRange.class);

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
        for (WhatKindaRange range : WhatKindaRange.values()) {
            System.setProperty("type.metadata.dir", tempDir.toFile().getCanonicalPath() + "/" + range.name());

            QueryTestTableHelper qtth = new QueryTestTableHelper(QueryFunctionQueryTest.class.toString() + "-" + range, log);
            AccumuloClient client = qtth.client;

            WiseGuysIngest.writeItAll(client, range);
            PrintUtility.printTable(client, auths, TableName.SHARD);
            PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(client, auths, QueryTestTableHelper.METADATA_TABLE_NAME);
            PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);

            clients.put(range, client);
        }
    }

    private void setupLogic(WhatKindaRange range) {
        setClientForTest(clients.get(range));
        eventQueryLogic.setCollapseUids(range == WhatKindaRange.SHARD);
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
        Object[][] cases = {
                // this is added to the new tfs added with dot notation check if they can even be queried
                {"QUOTE == 'never' && QUOTE == 'refuse'", List.of("CORLEONE")},
                // check if a content phrase across these same dot notation fields can get a hit
                {"content:phrase(termOffsetMap, 'i', 'never', 'refuse') && QUOTE == 'i' && QUOTE == 'never' && QUOTE == 'refuse'", List.of("CORLEONE")},
                // check that there is no cross contamination between the dot notation tfs and normal tfs
                {"content:phrase(termOffsetMap, 'gonna', 'refuse') && QUOTE == 'gonna' && QUOTE == 'refuse'", Collections.emptyList()},
                // check that if no tf set with dot notation satisfies a query it will be short circuited in tf eval
                {"content:phrase(termOffsetMap, 'never', 'offer') && QUOTE == 'never' && QUOTE == 'offer'", Collections.emptyList()},
                // verify tfs from another section of the query don't help this resolve
                {"content:phrase(QUOTE, termOffsetMap, 'never', 'refuse') && QUOTE == 'never' && QUOTE == 'refuse' && content:phrase(PHILOSOPHY, termOffsetMap, 'absolute', 'power') && PHILOSOPHY == 'absolute' && PHILOSOPHY == 'power'", List.of("CORLEONE")},
                // invert the targets
                {"content:phrase(PHILOSOPHY, termOffsetMap, 'never', 'refuse') && QUOTE == 'never' && QUOTE == 'refuse' && content:phrase(QUOTE, termOffsetMap, 'absolute', 'power') && PHILOSOPHY == 'absolute' && PHILOSOPHY == 'power'", Collections.emptyList()},
        };
        // @formatter:on
        return Stream.of(WhatKindaRange.values()).flatMap(range -> Stream.of(cases).map(c -> Arguments.of(range, c[0], c[1])));
    }

    @ParameterizedTest
    @MethodSource("phraseFunctionsWithHashesArgs")
    public void testPhraseFunctionsWithHashes(WhatKindaRange range, String query, List<String> expected) throws Exception {
        setupLogic(range);
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
        Object[][] cases = {
                {"UUID == 'corleone' && f:includeText(GENERE, 'FEMALE')", List.of("CORLEONE")},
                {"UUID == 'corleone' && f:includeText(GENERE, 'male')", Collections.emptyList()}, //  misses because includeText is case-sensitive
                {"UUID == 'corleone' && f:includeText(NUMBER, '25')", List.of("CORLEONE")},
        };
        // @formatter:on
        return Stream.of(WhatKindaRange.values()).flatMap(range -> Stream.of(cases).map(c -> Arguments.of(range, c[0], c[1])));
    }

    @ParameterizedTest
    @MethodSource("includeTextArgs")
    public void testIncludeText(WhatKindaRange range, String query, List<String> expected) throws Exception {
        setupLogic(range);

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
        Object[][] cases = {
                {"UUID == 'corleone' && f:matchRegex(GENERE, '.*MALE')", List.of("CORLEONE")},
                {"UUID == 'corleone' && f:matchRegex(GENERE, '.*male')", List.of("CORLEONE")},
                {"UUID == 'corleone' && f:matchRegex(NUMBER, '2.*')", List.of("CORLEONE")},
                {"UUID == 'corleone' && f:matchRegex(GENERE, '[A-Z]+')", List.of("CORLEONE")},
        };
        // @formatter:on
        return Stream.of(WhatKindaRange.values()).flatMap(range -> Stream.of(cases).map(c -> Arguments.of(range, c[0], c[1])));
    }

    @ParameterizedTest
    @MethodSource("matchRegexArgs")
    public void testMatchRegex(WhatKindaRange range, String query, List<String> expected) throws Exception {
        setupLogic(range);

        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("hit.list", "true");

        givenDate("20091231", "20150101");
        givenQuery(query);
        givenParameters(extraParameters);
        expectResults(expected);

        planAndExecuteQuery();
    }
}
