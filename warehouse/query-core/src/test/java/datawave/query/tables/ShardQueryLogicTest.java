package datawave.query.tables;

import static datawave.query.util.WiseGuysIngest.caponeUID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.iterators.TransformIterator;
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

import datawave.core.query.iterator.DatawaveTransformIterator;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.query.QueryParameters;
import datawave.query.QueryTestTableHelper;
import datawave.query.RebuildingScannerTestHelper;
import datawave.query.transformer.DocumentTransformer;
import datawave.query.util.AbstractQueryTest;
import datawave.query.util.WiseGuysIngest;
import datawave.table.constants.TableName;
import datawave.webservice.query.result.event.DefaultField;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.DefaultEventQueryResponse;

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
public class ShardQueryLogicTest extends AbstractQueryTest {

    private static final Logger log = Logger.getLogger(ShardQueryLogicTest.class);

    private static final Authorizations auths = new Authorizations("ALL");
    private static final Set<Authorizations> authSet = Collections.singleton(auths);

    @Autowired
    @Qualifier("EventQuery")
    protected ShardQueryLogic logic;

    private static AccumuloClient sharedClient;

    protected AccumuloClient client;

    protected final Map<String,Map<String,String>> expectedFields = new HashMap<>();
    protected final Map<String,List<String>> expectNoField = new HashMap<>();
    protected Set<Set<String>> expectedGroups = new HashSet<>();
    protected List<EventBase> events = new ArrayList<>();

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
        // No annotation-table setup is required by the core query-logic tests.
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

    @SuppressWarnings("unchecked")
    @Override
    protected void extraAssertions() {
        if (expectedGroups.isEmpty()) {
            assertTrue(events == null || events.isEmpty());
            return;
        }

        // planAndExecuteQuery() invokes extraAssertions() once per index table variant, so match against a
        // deep local copy rather than destructively consuming the shared expectedGroups sets.
        Set<Set<String>> remaining = new HashSet<>();
        for (Set<String> group : expectedGroups) {
            remaining.add(new HashSet<>(group));
        }

        for (Iterator<Set<String>> it = remaining.iterator(); it.hasNext();) {
            Set<String> expectedSet = it.next();
            boolean found = false;

            for (EventBase event : events) {
                if (expectedSet.contains("UID:" + event.getMetadata().getInternalId())) {
                    expectedSet.remove("UID:" + event.getMetadata().getInternalId());
                    ((List<DefaultField>) event.getFields()).forEach((f) -> expectedSet.remove(f.getName() + ":" + f.getValueString()));
                    if (expectedSet.isEmpty()) {
                        found = true;
                        it.remove();
                    }

                    // check for any expected fields
                    Map<String,String> expectedFieldsForDoc = expectedFields.computeIfAbsent(event.getMetadata().getInternalId(), x -> new HashMap<>());
                    List<String> expectedNoFieldsForDoc = expectNoField.computeIfAbsent(event.getMetadata().getInternalId(), x -> new ArrayList<>());

                    int foundCount = 0;
                    for (DefaultField field : (List<DefaultField>) event.getFields()) {
                        for (Entry<String,String> fieldValue : expectedFieldsForDoc.entrySet()) {
                            if (field.getName().equals(fieldValue.getKey())) {
                                assertEquals(fieldValue.getValue(), field.getValueString());
                                foundCount++;
                            }
                        }
                        for (String noField : expectedNoFieldsForDoc) {
                            if (field.getName().equals(noField)) {
                                fail("Encountered field which should not have been present in doc: " + event.getMetadata().getInternalId() + " field: "
                                                + noField);
                            }
                        }
                    }

                    assertEquals(expectedFieldsForDoc.size(), foundCount);
                    break;
                }
            }
            assertTrue(found, "field not found " + expectedSet);
        }
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));

        sharedClient = new QueryTestTableHelper(ShardQueryLogicTest.class.toString(), log, RebuildingScannerTestHelper.TEARDOWN.EVERY_OTHER_SANS_CONSISTENCY,
                        RebuildingScannerTestHelper.INTERRUPT.EVERY_OTHER).client;
        WiseGuysIngest.writeItAll(sharedClient, WiseGuysIngest.WhatKindaRange.DOCUMENT);
        PrintUtility.printTable(sharedClient, auths, TableName.SHARD);
        PrintUtility.printTable(sharedClient, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(sharedClient, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
    }

    @AfterAll
    public static void afterAll() {
        TypeRegistry.reset();
    }

    @BeforeEach
    public void setup() {
        this.client = sharedClient;
        setClientForTest(this.client);
        this.logic.setFullTableScanEnabled(true);

        this.expectedFields.clear();
        this.expectNoField.clear();
        this.expectedGroups = new HashSet<>();
        this.events = new ArrayList<>();
    }

    private void runTestQuery(Set<Set<String>> expected) throws Exception {
        this.expectedGroups = expected;
        givenDate("20091231", "20150101");
        planAndExecuteQuery();
    }

    @Test
    public void testFieldMappingTransformViaProfile() throws Exception {
        givenQuery("UUID =~ '^[CS].*'");
        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "false");
        givenParameter(QueryParameters.QUERY_PROFILE, "copyFieldEventQuery");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.sopranoUID, "MAGIC_COPY:18"));
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.corleoneUID, "MAGIC_COPY:18"));
        expected.add(Sets.newHashSet("UID:" + caponeUID, "MAGIC_COPY:18"));
        runTestQuery(expected);
    }

    @Test
    public void testRegex() throws Exception {
        givenQuery("UUID=='CAPONE' AND QUOTE=~'.*kind'");
        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");

        Set<Set<String>> expected = new HashSet<>();
        // todo: make this work someday
        // expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));

        runTestQuery(expected);
    }

    @Test
    public void testFwdRegex() throws Exception {
        givenQuery("UUID=='CAPONE' AND QUOTE=~'kin.*'");
        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");

        Set<Set<String>> expected = new HashSet<>();
        // todo: make this work someday
        // expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.caponeUID));

        runTestQuery(expected);
    }

    @Test
    public void testEvalRegex() throws Exception {
        givenQuery("UUID=='CAPONE' AND ((_Eval_ = true) && QUOTE=~'.*alone')");
        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));

        runTestQuery(expected);
    }

    @Test
    public void testNegativeEvalRegex() throws Exception {
        givenQuery("UUID=='CAPONE' AND ((_Eval_ = true) && QUOTE!~'.*alone')");
        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");

        Set<Set<String>> expected = new HashSet<>();
        runTestQuery(expected);
    }

    @Test
    public void testNegativeEvalRegexV2() throws Exception {
        givenQuery("UUID=='CAPONE' AND ((_Eval_ = true) && !(QUOTE=~'.*alone'))");
        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");

        Set<Set<String>> expected = new HashSet<>();
        runTestQuery(expected);
    }

    @Test
    public void testDoubeWildcard() throws Exception {
        givenQuery("UUID=='CAPONE' AND QUOTE=~'.*ind.*'");
        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));

        runTestQuery(expected);
    }

    @Test
    public void testNegativeRegex() throws Exception {
        givenQuery("UUID=='CAPONE' AND QUOTE!~'.*ind'");
        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));

        runTestQuery(expected);
    }

    @Test
    public void testNegativeRegexV2() throws Exception {
        givenQuery("UUID=='CAPONE' AND !(QUOTE=~'.*ind')");
        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));

        runTestQuery(expected);
    }

    @Test
    public void testFilterRegex() throws Exception {
        givenQuery("UUID=='CAPONE' AND filter:includeRegex(QUOTE,'.*kind word alone.*')");
        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");

        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + caponeUID));

        runTestQuery(expected);
    }

    @Test
    public void testNegativeFilterRegex() throws Exception {
        givenQuery("UUID=='CAPONE' AND !filter:includeRegex(QUOTE,'.*kind word alone.*')");
        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");

        Set<Set<String>> expected = new HashSet<>();

        runTestQuery(expected);
    }

    @Test
    public void testNegativeFilterRegexV2() throws Exception {
        givenQuery("UUID=='CAPONE' AND !(filter:includeRegex(QUOTE,'.*kind word alone.*'))");
        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");
        Set<Set<String>> expected = new HashSet<>();

        runTestQuery(expected);
    }

    @Test
    public void testExcludeDataTypesBangDataType() throws Exception {
        givenQuery("UUID=='TATTAGLIA'");
        givenParameter(QueryParameters.DATATYPE_FILTER_SET, "!test2");

        Set<Set<String>> expected = new HashSet<>();
        // No results expected
        runTestQuery(expected);
    }

    @Test
    public void testExcludeDataTypesNegateDataType() throws Exception {
        givenQuery("UUID=='TATTAGLIA'");
        givenParameter(QueryParameters.DATATYPE_FILTER_SET, "test2,!test2");

        Set<Set<String>> expected = new HashSet<>();
        // Expect one result, since the negated data type results in empty set, which is treated by Datawave as all data types
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.tattagliaUID));

        runTestQuery(expected);
    }

    @Test
    public void testExcludeDataTypesIncludeOneTypeExcludeOneType() throws Exception {
        givenQuery("UUID=='TATTAGLIA' || UUID=='CAPONE'");
        givenParameter(QueryParameters.DATATYPE_FILTER_SET, "test2,!test");
        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet("UID:" + WiseGuysIngest.tattagliaUID));

        runTestQuery(expected);
    }

}
