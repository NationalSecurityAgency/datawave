package datawave.query.jexl.functions;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.TimeZone;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
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

import com.google.common.collect.Lists;

import datawave.core.query.iterator.DatawaveTransformIterator;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.query.QueryTestTableHelper;
import datawave.query.RebuildingScannerTestHelper;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.transformer.DocumentTransformer;
import datawave.query.util.AbstractQueryTest;
import datawave.table.constants.TableName;
import datawave.webservice.result.DefaultEventQueryResponse;

/**
 * Base test set up for functions in {@link GroupingRequiredFilterFunctions}.
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
public class GroupingRequiredFilterFunctionsIntegrationTest extends AbstractQueryTest {

    private static final Logger log = Logger.getLogger(GroupingRequiredFilterFunctionsIntegrationTest.class);
    private static final Authorizations auths = new Authorizations("ALL", "E", "I");

    private static AccumuloClient clientForTest;

    @Autowired
    @Qualifier("EventQuery")
    protected ShardQueryLogic logic;

    private DefaultEventQueryResponse response;
    private boolean expectNoResults;
    private int expectedEventCount;

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
            List<Object> eventList = Lists.newArrayList(new DatawaveTransformIterator<>(logic.iterator(), transformer));
            response = (DefaultEventQueryResponse) transformer.createResponse(eventList);
        } finally {
            logic.close();
        }
    }

    @Override
    protected void extraAssertions() {
        if (expectNoResults) {
            assertThat(response.getEvents()).isNull();
        } else {
            assertThat(response.getEvents()).hasSize(expectedEventCount);
        }
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));

        QueryTestTableHelper qtth = new QueryTestTableHelper(GroupingRequiredFilterFunctionsIntegrationTest.class.toString(), log,
                        RebuildingScannerTestHelper.TEARDOWN.NEVER, RebuildingScannerTestHelper.INTERRUPT.NEVER);
        clientForTest = qtth.client;

        // ingest with the document range only; GroupingFiltersIngest already calls IndexIngestUtil
        // internally to derive the other shard index table variants that AbstractQueryTest iterates over.
        GroupingFiltersIngest.writeItAll(clientForTest, GroupingFiltersIngest.Range.DOCUMENT);
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
        logic.setFullTableScanEnabled(true);
        logic.setMaxEvaluationPipelines(1);
        logic.setQueryExecutionForPageTimeout(300000000000000L);
        logic.setCollapseUids(true);

        givenDate("20091231", "20150101");
    }

    private void givenLuceneParserForLogic() {
        logic.setParser(new LuceneToJexlQueryParser());
    }

    private void assertNoResults() throws Exception {
        this.expectNoResults = true;
        planAndExecuteQuery();
    }

    private void assertTotalEvents(int expected) throws Exception {
        this.expectNoResults = false;
        this.expectedEventCount = expected;
        planAndExecuteQuery();
    }

    @Test
    public void testMatchInGroupWithNoMatches() throws Exception {
        givenLuceneParserForLogic();

        // Will match on name, but not on gender or age.
        givenQuery("NAME:Santino AND #MATCHES_IN_GROUP(GENDER,'FEMALE',AGE,99)");

        assertNoResults();
    }

    @Test
    public void testMatchInGroupWithPartialMatches() throws Exception {
        givenLuceneParserForLogic();

        // Will match on name and gender, but not age.
        givenQuery("NAME:Santino AND #MATCHES_IN_GROUP(GENDER,'MALE',AGE,99)");

        assertNoResults();
    }

    @Test
    public void testMatchInGroupWithMatches() throws Exception {
        givenLuceneParserForLogic();

        // Will match on name, gender, and age.
        givenQuery("NAME:Santino AND #MATCHES_IN_GROUP(GENDER,'MALE',AGE,24)");

        assertTotalEvents(1);
    }

    @Test
    public void testMatchInGroupWithNumericRegex() throws Exception {
        givenQuery("UUID =~ '^[CS].*' && grouping:matchesInGroup(GENDER,'MALE',AGE,'2*')");

        assertTotalEvents(2);
    }

    @Test
    public void testMatchInGroupWithRegex_Lucene() throws Exception {
        givenLuceneParserForLogic();

        givenQuery("(UUID:C* OR UUID:S*) AND #MATCHES_IN_GROUP(GENDER,'MALE',AGE,2*)");

        assertTotalEvents(2);
    }
}
