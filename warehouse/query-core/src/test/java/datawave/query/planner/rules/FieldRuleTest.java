package datawave.query.planner.rules;

import java.text.ParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.log4j.Logger;
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
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.JexlFunctionArgumentDescriptorFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.AbstractQueryTest;
import datawave.query.util.MetadataHelper;
import datawave.query.util.VisibilityWiseGuysIngestWithModel;

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
public class FieldRuleTest extends AbstractQueryTest {

    private static final Logger log = Logger.getLogger(FieldRuleTest.class);
    private static final Authorizations auths = new Authorizations("ALL", "E", "I");
    protected Set<Authorizations> authSet = Collections.singleton(auths);

    private static AccumuloClient clientForTest;

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
        InMemoryInstance instance = new InMemoryInstance(FieldRuleTest.class.getSimpleName());
        clientForTest = new InMemoryAccumuloClient("", instance);
        VisibilityWiseGuysIngestWithModel.writeItAll(clientForTest);
    }

    @BeforeEach
    public void setup() throws ParseException {
        this.logic.setFullTableScanEnabled(true);
        this.logic.setMaxEvaluationPipelines(1);
        this.logic.setQueryExecutionForPageTimeout(300000000000000L);
        setClientForTest(clientForTest);

        givenDate("20091231", "20150101");
        getLogic().getConfig().setFieldRuleClassName(FieldRuleTest.NoSoupForYouRule.class.getName());
    }

    @Test
    public void testNoEffect() throws Exception {
        givenQuery("COLOR == 'blue'");
        expectPlan("COLOR == 'blue' || HUE == 'blue'");
        planAndExecuteQuery();
    }

    @Test
    public void testNoSoupForYou() throws Exception {
        givenQuery("COLOR == 'blue' || SOUP == 'chicken noodle'");
        expectPlan("COLOR == 'blue' || HUE == 'blue'");
        planAndExecuteQuery();
    }

    @Test
    public void testNoSoupForYouEither() throws Exception {
        givenQuery("COLOR == 'blue' || FOOD == 'soup'");
        expectPlan("COLOR == 'blue' || HUE == 'blue'");
        planAndExecuteQuery();
    }

    @Test
    public void testNegatedSoup() throws Exception {
        // allow it because the query is self-enforcing a rule
        givenQuery("COLOR == 'blue' && FOOD != 'soup'");
        expectPlan("(COLOR == 'blue' || HUE == 'blue') && !(FOOD == 'soup')");
        planAndExecuteQuery();
    }

    @Test
    public void testNoSoupForYouAND() throws Exception {
        givenQuery("COLOR == 'blue' && SOUP == 'chicken noodle'");
        expectPlan("false");
        planAndExecuteQuery();
    }

    @Test
    public void testSoupNotIncluded() throws Exception {
        givenQuery("COLOR == 'blue' && filter:includeRegex(SOUP, 'chicken nood*')");
        expectPlan("false");
        planAndExecuteQuery();
    }

    public static class NoSoupForYouRule extends FieldRule {
        Set<String> pruneFields;
        Map<String,Set<String>> pruneFVPairs;

        public NoSoupForYouRule(GenericQueryConfiguration config) {
            super(config);

        }

        @Override
        public void parseRules(GenericQueryConfiguration config) {
            this.pruneFields = new HashSet<>();
            this.pruneFVPairs = new HashMap<>();

            pruneFields.add("SOUP");
            // Note: real implementations should use Normalizers
            pruneFVPairs.put("FOOD", Collections.singleton("soup"));
        }

        @Override
        public boolean shouldPrune(JexlNode node, MetadataHelper helper) {
            try {
                boolean isNegated = JexlASTHelper.isDescendantOfNot(node);
                if (node instanceof ASTFunctionNode) {
                    JexlArgumentDescriptor desc = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor((ASTFunctionNode) node);
                    Set<String> fields = desc.fields(helper, null);
                    for (String field : fields) {
                        if (pruneFields.contains(field) || pruneFVPairs.containsKey(field)) {
                            return true;
                        }
                    }

                } else {
                    String identifier = JexlASTHelper.getIdentifier(node);
                    String value = String.valueOf(JexlASTHelper.getLiteralValue(node));

                    return pruneFields.contains(identifier)
                                    || (pruneFVPairs.containsKey(identifier) && (pruneFVPairs.get(identifier).contains(value) && !isNegated));

                }
            } catch (NoSuchElementException e) {
                // do nothing for now
            }
            return false;
        }

        @Override
        public boolean shouldModify(JexlNode node, MetadataHelper helper) {
            return false;
        }

        @Override
        public JexlNode modify(JexlNode node, MetadataHelper helper) {
            return null;
        }
    }
}
