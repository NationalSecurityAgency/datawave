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
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.AbstractQueryTest;
import datawave.query.util.VisibilityWiseGuysIngestWithModel;

/**
 * Tests the expansion of queries from the query model.
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
public class QueryModelExpansionTests extends AbstractQueryTest {

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
     * Verify that expansion occurs for a standard forward mapping.
     */
    @Test
    public void testBasicExpansion() throws Exception {
        givenQuery("COLOR == 'blue'");
        expectPlan("COLOR == 'blue' || HUE == 'blue'");
        planAndExecuteQuery();
    }

    /**
     * Verifies that a patterned model mapping expands the query.
     */
    @Test
    public void testPatternedModelExpansion() throws Exception {
        givenQuery("NAM == 'amy'");
        expectPlan("NAME == 'amy' || NOME == 'amy'");
        planAndExecuteQuery();
    }
}
