package datawave.query;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.google.common.collect.Sets;

import datawave.core.query.iterator.DatawaveTransformIterator;
import datawave.helpers.PrintUtility;
import datawave.query.exceptions.InvalidQueryException;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.transformer.DocumentTransformer;
import datawave.query.util.AbstractQueryTest;
import datawave.query.util.WiseGuysIngest;
import datawave.table.constants.TableName;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.DefaultEventQueryResponse;

/**
 * Applies uniqueness to queries
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
public class UniqueTest extends AbstractQueryTest {

    private static final Logger log = Logger.getLogger(UniqueTest.class);
    private static final Authorizations auths = new Authorizations("ALL");

    private static AccumuloClient client;

    @Autowired
    @Qualifier("EventQuery")
    protected ShardQueryLogic logic;

    private Set<Set<String>> expectedGroups;
    private List<EventBase> events;

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
    protected void executeQuery(ShardQueryLogic logic) throws Exception {
        try {
            DocumentTransformer transformer = (DocumentTransformer) (logic.getTransformer(logic.getConfig().getQuery()));
            TransformIterator iter = new DatawaveTransformIterator(logic.iterator(), transformer);
            List<Object> eventList = new ArrayList<>();
            while (iter.hasNext()) {
                Object o = iter.next();
                if (o != null) {
                    eventList.add(o);
                }
            }

            BaseQueryResponse response = transformer.createResponse(eventList);
            assertTrue(response instanceof DefaultEventQueryResponse);
            this.events = ((DefaultEventQueryResponse) response).getEvents();
        } finally {
            logic.close();
        }
    }

    @Override
    protected void extraAssertions() {
        // planAndExecuteQuery() invokes extraAssertions() once per index table variant, so check
        // against a local copy rather than destructively consuming the shared expectedGroups set.
        Set<Set<String>> remaining = new HashSet<>(expectedGroups);

        for (EventBase event : events) {
            boolean found = false;
            for (Iterator<Set<String>> it = remaining.iterator(); it.hasNext();) {
                Set<String> expectedSet = it.next();

                if (expectedSet.contains(event.getMetadata().getInternalId())) {
                    it.remove();
                    found = true;
                    break;
                }
            }
            assertTrue(found, "Failed to find " + event.getMetadata().getInternalId() + " in expected results");
        }
        assertTrue(remaining.isEmpty(), "Failed to find all expected results.  Missing " + remaining);
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        // testing tear downs but without consistency, because when we tear it down then we lose the ongoing bloom filter and subsequently the rebuild
        // will start returning different keys.
        QueryTestTableHelper qtth = new QueryTestTableHelper(UniqueTest.class.toString(), log,
                        RebuildingScannerTestHelper.TEARDOWN.EVERY_OTHER_SANS_CONSISTENCY, RebuildingScannerTestHelper.INTERRUPT.EVERY_OTHER);
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

        // setup the hadoop configuration
        URL hadoopConfig = this.getClass().getResource("/testhadoop.config");
        logic.setHdfsSiteConfigURLs(hadoopConfig.toExternalForm());

        logic.setQueryExecutionForPageTimeout(300000000000000L);
    }

    private void runTestQueryWithUniqueness(Set<Set<String>> expected, String querystr, Map<String,String> extraParms) throws Exception {
        this.expectedGroups = expected;

        givenDate("20091231", "20150101");
        givenQuery(querystr);
        givenParameters(extraParms);

        planAndExecuteQuery();
    }

    private static Stream<Arguments> uniquenessArgs() {
        // @formatter:off
        return Stream.of(
                Arguments.of("DEATH_DATE,$MAGIC", Set.of(Sets.newHashSet(WiseGuysIngest.sopranoUID, WiseGuysIngest.corleoneUID, WiseGuysIngest.caponeUID))),
                Arguments.of("$DEATH_DATE,BIRTH_DATE", Set.of(Sets.newHashSet(WiseGuysIngest.sopranoUID), Sets.newHashSet(WiseGuysIngest.corleoneUID),
                        Sets.newHashSet(WiseGuysIngest.caponeUID))),
                Arguments.of("death_date,birth_date", Set.of(Sets.newHashSet(WiseGuysIngest.sopranoUID), Sets.newHashSet(WiseGuysIngest.corleoneUID),
                        Sets.newHashSet(WiseGuysIngest.caponeUID))));
        // @formatter:on
    }

    @ParameterizedTest
    @MethodSource("uniquenessArgs")
    public void testUniqueness(String uniqueFields, Set<Set<String>> expected) throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("unique.fields", uniqueFields);

        String queryString = "UUID =~ '^[CS].*'";

        runTestQueryWithUniqueness(expected, queryString, extraParameters);
    }

    @Test
    public void testUniquenessWithMissingField() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("unique.fields", "NUMBER");

        String queryString = "UUID =~ '^[CS].*'";

        // both capone and corleone contain NUMBER:25, only one document is expected to be returned
        // soprano uid -1kfeoq.-80b5fs.r0262j does NOT contain a NUMBER field
        Set<Set<String>> expected = Set.of(Sets.newHashSet(WiseGuysIngest.caponeUID, WiseGuysIngest.corleoneUID), Sets.newHashSet(WiseGuysIngest.sopranoUID));

        runTestQueryWithUniqueness(expected, queryString, extraParameters);
    }

    private static Stream<Arguments> uniquenessUsingFunctionArgs() {
        // @formatter:off
        return Stream.of(
                Arguments.of("UUID =~ '^[CS].*' && f:unique($DEATH_DATE,MAGIC)",
                        Set.of(Sets.newHashSet(WiseGuysIngest.sopranoUID, WiseGuysIngest.corleoneUID, WiseGuysIngest.caponeUID))),
                Arguments.of("UUID =~ '^[CS].*' && f:unique('DEATH_DATE','$BIRTH_DATE')", Set.of(Sets.newHashSet(WiseGuysIngest.sopranoUID),
                        Sets.newHashSet(WiseGuysIngest.corleoneUID), Sets.newHashSet(WiseGuysIngest.caponeUID))),
                Arguments.of("UUID =~ '^[CS].*' && f:unique('death_date','$birth_date')", Set.of(Sets.newHashSet(WiseGuysIngest.sopranoUID),
                        Sets.newHashSet(WiseGuysIngest.corleoneUID), Sets.newHashSet(WiseGuysIngest.caponeUID))));
        // @formatter:on
    }

    @ParameterizedTest
    @MethodSource("uniquenessUsingFunctionArgs")
    public void testUniquenessUsingFunction(String queryString, Set<Set<String>> expected) throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");

        runTestQueryWithUniqueness(expected, queryString, extraParameters);
    }

    private static Stream<Arguments> uniquenessUsingLuceneFunctionArgs() {
        // @formatter:off
        return Stream.of(
                Arguments.of("UUID:/^[CS].*/ AND #UNIQUE(DEATH_DATE,$MAGIC)",
                        Set.of(Sets.newHashSet(WiseGuysIngest.sopranoUID, WiseGuysIngest.corleoneUID, WiseGuysIngest.caponeUID))),
                Arguments.of("UUID:/^[CS].*/ AND #UNIQUE(DEATH_DATE,$BIRTH_DATE)", Set.of(Sets.newHashSet(WiseGuysIngest.sopranoUID),
                        Sets.newHashSet(WiseGuysIngest.corleoneUID), Sets.newHashSet(WiseGuysIngest.caponeUID))),
                Arguments.of("UUID:/^[CS].*/ AND #UNIQUE(death_date,birth_date)", Set.of(Sets.newHashSet(WiseGuysIngest.sopranoUID),
                        Sets.newHashSet(WiseGuysIngest.corleoneUID), Sets.newHashSet(WiseGuysIngest.caponeUID))));
        // @formatter:on
    }

    @ParameterizedTest
    @MethodSource("uniquenessUsingLuceneFunctionArgs")
    public void testUniquenessUsingLuceneFunction(String queryString, Set<Set<String>> expected) throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("query.syntax", "LUCENE");

        runTestQueryWithUniqueness(expected, queryString, extraParameters);
    }

    @Test
    public void testUniquenessWithBadField() {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("query.syntax", "LUCENE");

        assertThrows(InvalidQueryException.class, () -> {
            String queryString = "UUID:/^[CS].*/ AND #UNIQUE(FOO_BAR,$MAGIC)";
            runTestQueryWithUniqueness(Set.of(), queryString, extraParameters);

            queryString = "UUID:/^[CS].*/ AND #UNIQUE(foo_bar,$magic)";
            runTestQueryWithUniqueness(Set.of(), queryString, extraParameters);
        });
    }

    @Test
    public void testUniquenessWithHitTermField() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("query.syntax", "LUCENE");

        // HIT_TERM legitimately differs per event (the matched term text includes the UUID value itself,
        // e.g. "UUID:SOPRANO" vs "UUID:CORLEONE" vs "UUID:CAPONE"), so uniqueness on HIT_TERM should not
        // collapse these three into one result the way BOTH_NULL does in the sibling tests below.
        Set<Set<String>> expected = Set.of(Sets.newHashSet(WiseGuysIngest.sopranoUID), Sets.newHashSet(WiseGuysIngest.corleoneUID),
                        Sets.newHashSet(WiseGuysIngest.caponeUID));

        String queryString = "UUID:/^[CS].*/ AND #UNIQUE(HIT_TERM)";
        runTestQueryWithUniqueness(expected, queryString, extraParameters);
    }

    @Test
    public void testUniquenessWithModelAliases() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("query.syntax", "LUCENE");

        Set<Set<String>> expected = Set.of(Sets.newHashSet(WiseGuysIngest.sopranoUID, WiseGuysIngest.corleoneUID, WiseGuysIngest.caponeUID));

        String queryString = "UUID:/^[CS].*/ AND #UNIQUE(BOTH_NULL)";
        runTestQueryWithUniqueness(expected, queryString, extraParameters);
    }

    @Test
    public void testRecentUniquenessWithModelAliases() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("query.syntax", "LUCENE");

        Set<Set<String>> expected = Set.of(Sets.newHashSet(WiseGuysIngest.sopranoUID, WiseGuysIngest.corleoneUID, WiseGuysIngest.caponeUID));

        String queryString = "UUID:/^[CS].*/ AND #MOST_RECENT_UNIQUE(BOTH_NULL)";
        runTestQueryWithUniqueness(expected, queryString, extraParameters);
    }

    private static Stream<Arguments> mostRecentUniquenessArgs() {
        // @formatter:off
        return Stream.of(
                Arguments.of("DEATH_DATE,$MAGIC", Set.of(Sets.newHashSet(WiseGuysIngest.caponeUID))),
                Arguments.of("death_date,$magic", Set.of(Sets.newHashSet(WiseGuysIngest.caponeUID))),
                Arguments.of("$DEATH_DATE,BIRTH_DATE", Set.of(Sets.newHashSet(WiseGuysIngest.sopranoUID), Sets.newHashSet(WiseGuysIngest.corleoneUID),
                        Sets.newHashSet(WiseGuysIngest.caponeUID))),
                Arguments.of("death_date,birth_date", Set.of(Sets.newHashSet(WiseGuysIngest.sopranoUID), Sets.newHashSet(WiseGuysIngest.corleoneUID),
                        Sets.newHashSet(WiseGuysIngest.caponeUID))));
        // @formatter:on
    }

    @ParameterizedTest
    @MethodSource("mostRecentUniquenessArgs")
    public void testMostRecentUniquenessWebServerOnly(String uniqueFields, Set<Set<String>> expected) throws Exception {
        boolean originalSetting = logic.isDisableIteratorMostRecentUniqueFields();
        try {
            logic.setDisableIteratorMostRecentUniqueFields(true);

            Map<String,String> extraParameters = new HashMap<>();
            extraParameters.put("include.grouping.context", "true");
            extraParameters.put(QueryParameters.MOST_RECENT_UNIQUE, "true");
            extraParameters.put("unique.fields", uniqueFields);

            String queryString = "UUID =~ '^[CS].*'";

            runTestQueryWithUniqueness(expected, queryString, extraParameters);
        } finally {
            logic.setDisableIteratorMostRecentUniqueFields(originalSetting);
        }
    }

    @ParameterizedTest
    @MethodSource("mostRecentUniquenessArgs")
    public void testMostRecentUniquenessWithIterator(String uniqueFields, Set<Set<String>> expected) throws Exception {
        boolean originalSetting = logic.isDisableIteratorMostRecentUniqueFields();
        try {
            logic.setDisableIteratorMostRecentUniqueFields(false);

            Map<String,String> extraParameters = new HashMap<>();
            extraParameters.put("include.grouping.context", "true");
            extraParameters.put(QueryParameters.MOST_RECENT_UNIQUE, "true");
            extraParameters.put("unique.fields", uniqueFields);

            String queryString = "UUID =~ '^[CS].*'";

            runTestQueryWithUniqueness(expected, queryString, extraParameters);
        } finally {
            logic.setDisableIteratorMostRecentUniqueFields(originalSetting);
        }
    }

    private static Stream<Arguments> hannahHypothesisArgs() {
        // @formatter:off
        return Stream.of(
                Arguments.of("UUID =~ '^[CS].*'", true),
                Arguments.of("UUID =~ '^[CS].*' && f:unique(death_date,magic)", false));
        // @formatter:on
    }

    @ParameterizedTest
    @MethodSource("hannahHypothesisArgs")
    public void testHannahHypothesis(String queryString, boolean withUniqueFieldsParam) throws Exception {
        Set<Set<String>> expected = Set.of(Sets.newHashSet(WiseGuysIngest.caponeUID));

        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put(QueryParameters.MOST_RECENT_UNIQUE, "true");
        if (withUniqueFieldsParam) {
            extraParameters.put("unique.fields", "DEATH_DATE,$MAGIC");
        }

        runTestQueryWithUniqueness(expected, queryString, extraParameters);
    }

    private static Stream<Arguments> uniquenessWithoutIteratorLevelTransformerArgs() {
        // @formatter:off
        return Stream.of(
                Arguments.of("DEATH_DATE,$MAGIC", Set.of(Sets.newHashSet(WiseGuysIngest.sopranoUID, WiseGuysIngest.corleoneUID, WiseGuysIngest.caponeUID))),
                Arguments.of("$DEATH_DATE,BIRTH_DATE", Set.of(Sets.newHashSet(WiseGuysIngest.sopranoUID), Sets.newHashSet(WiseGuysIngest.corleoneUID),
                        Sets.newHashSet(WiseGuysIngest.caponeUID))),
                Arguments.of("death_date,birth_date", Set.of(Sets.newHashSet(WiseGuysIngest.sopranoUID), Sets.newHashSet(WiseGuysIngest.corleoneUID),
                        Sets.newHashSet(WiseGuysIngest.caponeUID))));
        // @formatter:on
    }

    @ParameterizedTest
    @MethodSource("uniquenessWithoutIteratorLevelTransformerArgs")
    public void testUniquenessWithoutIteratorLevelTransformer(String uniqueFields, Set<Set<String>> expected) throws Exception {
        boolean originalSetting = logic.isDisableIteratorUniqueFields();
        try {
            // disable the unique transform on the query iterator
            logic.setDisableIteratorUniqueFields(true);

            Map<String,String> extraParameters = new HashMap<>();
            extraParameters.put("include.grouping.context", "true");
            extraParameters.put("unique.fields", uniqueFields);

            String queryString = "UUID =~ '^[CS].*'";

            runTestQueryWithUniqueness(expected, queryString, extraParameters);
        } finally {
            logic.setDisableIteratorUniqueFields(originalSetting);
        }
    }
}
