package datawave.query.transformer;

import java.text.ParseException;
import java.util.Collections;
import java.util.Set;
import java.util.TimeZone;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.query.QueryParameters;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.AbstractQueryTest;
import datawave.query.util.VisibilityWiseGuysIngestWithModel;

/**
 * Tests for usage of #NO_EXPANSION in queries.
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
public class NoExpansionTests extends AbstractQueryTest {

    private static final Authorizations auths = new Authorizations("ALL", "E", "I");
    protected Set<Authorizations> authSet = Collections.singleton(auths);
    private static AccumuloClient clientForSetup;

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
        // no-op
    }

    @Override
    protected void extraAssertions() {
        // no-op
    }

    /**
     * Helper method for configuring {@link QueryParameters#NO_EXPANSION_FIELDS}
     *
     * @param field
     *            the field or fields to set
     */
    protected void givenNoExpansion(String field) {
        givenParameter(QueryParameters.NO_EXPANSION_FIELDS, field);
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        InMemoryInstance instance = new InMemoryInstance(NoExpansionTests.class.getName());
        clientForSetup = new InMemoryAccumuloClient("", instance);

        // this helper class will generate the extra index tables
        VisibilityWiseGuysIngestWithModel.writeItAll(clientForSetup);
    }

    @BeforeEach
    public void setup() throws ParseException {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        givenDate("20091231", "20150101");
        setClientForTest(clientForSetup);
    }

    /**
     * Base test to verify expansion happens by default.
     */
    @Test
    public void testDefaultQueryModelExpansion() throws Exception {
        givenQuery("COLOR == 'blue' && FASTENER == 'bolt'");
        expectPlan("(COLOR == 'blue' || HUE == 'blue') && (FASTENER == 'bolt' || FIXTURE == 'bolt')");
        planAndExecuteQuery();
    }

    /**
     * Verify that when #NO_EXPANSION is specified in the query string itself, expansion does not occur.
     */
    @Test
    public void testNoExpansionViaFunction() throws Exception {
        givenQuery("COLOR == 'blue' && f:noExpansion(COLOR)");
        expectPlan("COLOR == 'blue'");
        planAndExecuteQuery();
    }

    /**
     * Verify that when #NO_EXPANSION is specified in the query string itself with multiple fields, expansion does not occur.
     */
    @Test
    public void testNoExpansionViaFunctionWithMultipleFields() throws Exception {
        givenQuery("COLOR == 'blue' && FASTENER == 'bolt' && f:noExpansion(COLOR,FASTENER)");
        expectPlan("COLOR == 'blue' && FASTENER == 'bolt'");
        planAndExecuteQuery();
    }

    /**
     * Verify that when #NO_EXPANSION is specified via the query parameters, expansion does not occur.
     */
    @Test
    public void testNoExpansionViaQueryParameters() throws Exception {
        givenNoExpansion("COLOR");
        givenQuery("COLOR == 'blue'");
        expectPlan("COLOR == 'blue'");
        planAndExecuteQuery();
    }

    /**
     * Verify that when #NO_EXPANSION is specified via the query parameters, expansion does not occur.
     */
    @Test
    public void testNoExpansionViaQueryParametersWithMultipleFields() throws Exception {
        givenNoExpansion("COLOR,FASTENER");
        givenQuery("COLOR == 'blue' && FASTENER == 'bolt'");
        expectPlan("COLOR == 'blue' && FASTENER == 'bolt'");
        planAndExecuteQuery();
    }

    /**
     * Verify that when #NO_EXPANSION is specified in the query string itself and in query parameters, expansion does not occur.
     */
    @Test
    public void testNoExpansionViaFunctionAndQueryParameters() throws Exception {
        givenNoExpansion("COLOR");
        givenQuery("COLOR == 'blue' && f:noExpansion(COLOR)");
        expectPlan("COLOR == 'blue'");
        planAndExecuteQuery();
    }

    /**
     * Verify that when #NO_EXPANSION is specified with the correct field in the query string, and the wrong field in the query parameters, that the correct
     * field is retained.
     */
    @Test
    public void testConflictingNoExpansionFields() throws Exception {
        givenNoExpansion("HUE");
        givenQuery("COLOR == 'blue' && f:noExpansion(COLOR)");
        expectPlan("COLOR == 'blue'");
        planAndExecuteQuery();
    }
}
