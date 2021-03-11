package datawave.query.jexl.visitors;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.MetadataHelper;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

@RunWith(Parameterized.class)
public class PushdownUnexecutableNodesVisitorTest extends EasyMockSupport {
    private ShardQueryConfiguration config;
    private MetadataHelper helper;
    
    /**
     * Collection of tests
     * 
     * @return
     */
    @Parameterized.Parameters(name = "{0}")
    public static Collection testCases() {
        // @formatter:off
        return Arrays.asList(new Object[][] {
            // executable against the global index
            // executable against the field index after adding a delay
            {
                "AndNegatedAndPartial",
                "INDEXED_FIELD == 'a' && !(INDEX_ONLY_FIELD == 'b' && filter:includeRegex(INDEXED_FIELD, '.*'))",
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                "INDEXED_FIELD == 'a' && ((!(INDEX_ONLY_FIELD == 'b') || !(filter:includeRegex(INDEXED_FIELD, '.*'))))",
                ExecutableDeterminationVisitor.STATE.PARTIAL,
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                "INDEXED_FIELD == 'a' && ((_Delayed_ = true) && ((!(INDEX_ONLY_FIELD == 'b') || !(filter:includeRegex(INDEXED_FIELD, '.*')))))"
            },
            // executable against the global index
            // executable against the field index after adding a delay
            {
                "AndNegatedAndPartialExtended",
                "INDEXED_FIELD == 'a' && !(INDEX_ONLY_FIELD == 'b' && filter:includeRegex(INDEXED_FIELD, '.*')) && EVENT_FIELD == 'd'",
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                "INDEXED_FIELD == 'a' && ((!(INDEX_ONLY_FIELD == 'b') || !(filter:includeRegex(INDEXED_FIELD, '.*')))) && EVENT_FIELD == 'd'",
                ExecutableDeterminationVisitor.STATE.PARTIAL,
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                "INDEXED_FIELD == 'a' && ((_Delayed_ = true) && ((!(INDEX_ONLY_FIELD == 'b') || !(filter:includeRegex(INDEXED_FIELD, '.*'))))) && EVENT_FIELD == 'd'",
            },
            // executable against the global index,
            // executable against the field index after adding a delay
            {
                "AndNegatedAndPartialEventField",
                "INDEXED_FIELD == 'a' && !(INDEX_ONLY_FIELD == 'b' && EVENT_FIELD == 'c')",
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                "INDEXED_FIELD == 'a' && ((!(INDEX_ONLY_FIELD == 'b') || !(EVENT_FIELD == 'c')))",
                ExecutableDeterminationVisitor.STATE.PARTIAL,
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                "INDEXED_FIELD == 'a' && ((_Delayed_ = true) && ((!(INDEX_ONLY_FIELD == 'b') || !(EVENT_FIELD == 'c'))))"
            },
            // no action necessary, should be straight up executable
            {
                "AndIndexedEvent",
                "INDEXED_FIELD == 'a' && EVENT_FIELD == 'b'",
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                "INDEXED_FIELD == 'a' && EVENT_FIELD == 'b'",
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                "INDEXED_FIELD == 'a' && EVENT_FIELD == 'b'",
            },
            // executable against the global index, and executable against the field index since the OR is negated its sufficient for a single term to exclude, the other term
            // would be found and excluded at document evaluation
            {
                "AndNegatedORExecutable",
                "INDEXED_FIELD == 'a' && !(INDEX_ONLY_FIELD == 'b' || EVENT_FIELD == 'c')",
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                "INDEXED_FIELD == 'a' && !(INDEX_ONLY_FIELD == 'b') && !(EVENT_FIELD == 'c')",
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                "INDEXED_FIELD == 'a' && !(INDEX_ONLY_FIELD == 'b') && !(EVENT_FIELD == 'c')"
            },
            // not executable against the global index because it will always mark a negation as non-executable instead of evaluating the subtree, the field index is executable
            // because the negations result in EXECUTABLE || (!(EXECUTABLE) && !(NON_EXECUTABLE)) == EXECUTABLE || (EXECUTABLE) == EXECUTABLE
            {
                "OrAndNegatedLeaf",
                "INDEXED_FIELD == 'a' || (!(INDEX_ONLY_FIELD == 'b') && !(EVENT_FIELD == 'c'))",
                ExecutableDeterminationVisitor.STATE.PARTIAL,
                ExecutableDeterminationVisitor.STATE.PARTIAL,
                "INDEXED_FIELD == 'a' || (!(INDEX_ONLY_FIELD == 'b') && !(EVENT_FIELD == 'c'))",
                ExecutableDeterminationVisitor.STATE.NEGATED_EXECUTABLE,
                ExecutableDeterminationVisitor.STATE.NEGATED_EXECUTABLE,
                "INDEXED_FIELD == 'a' || (!(INDEX_ONLY_FIELD == 'b') && !(EVENT_FIELD == 'c'))"
            },
            // top level or against the global index is non-executable due to the negation
            // against the field index is partial because the negation requires that all components of the AND are executable and the EVENT_FIELD is not
            {
                "OrNegatedAnd",
                "INDEXED_FIELD == 'a' || !(INDEX_ONLY_FIELD == 'b' && EVENT_FIELD == 'c')",
                ExecutableDeterminationVisitor.STATE.PARTIAL,
                ExecutableDeterminationVisitor.STATE.PARTIAL,
                "INDEXED_FIELD == 'a' || !(INDEX_ONLY_FIELD == 'b') || !(EVENT_FIELD == 'c')",
                ExecutableDeterminationVisitor.STATE.PARTIAL,
                ExecutableDeterminationVisitor.STATE.PARTIAL,
                "INDEXED_FIELD == 'a' || !(INDEX_ONLY_FIELD == 'b') || !(EVENT_FIELD == 'c')",
            },
            // Global index cannot be executed due to partial result (no negations)
            // Field index is executable becuase INDEX_ONLY_FIELD == 'b' is EXECUTABLE and the negation attached to the OR makes that sufficient for exclusion
            {
                "OrNegatedOr",
                "INDEXED_FIELD == 'a' || !(INDEX_ONLY_FIELD == 'b' || EVENT_FIELD == 'c')",
                ExecutableDeterminationVisitor.STATE.PARTIAL,
                ExecutableDeterminationVisitor.STATE.PARTIAL,
                "INDEXED_FIELD == 'a' || ((!(INDEX_ONLY_FIELD == 'b') && !(EVENT_FIELD == 'c')))",
                ExecutableDeterminationVisitor.STATE.NEGATED_EXECUTABLE,
                ExecutableDeterminationVisitor.STATE.NEGATED_EXECUTABLE,
                "INDEXED_FIELD == 'a' || ((!(INDEX_ONLY_FIELD == 'b') && !(EVENT_FIELD == 'c')))",
            },
            // Both global and field can be run
            {
                "AndOr",
                "INDEXED_FIELD == 'a' && (INDEXED_FIELD == 'b' || TF_FIELD == 'c')",
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                "INDEXED_FIELD == 'a' && (INDEXED_FIELD == 'b' || TF_FIELD == 'c')",
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                "INDEXED_FIELD == 'a' && (INDEXED_FIELD == 'b' || TF_FIELD == 'c')",
            },
            // Both global and field index must add a delay to become executable
            {
                "AndOrDelayed",
                "INDEXED_FIELD == 'a' && (INDEXED_FIELD == 'b' || EVENT_FIELD == 'c')",
                ExecutableDeterminationVisitor.STATE.PARTIAL,
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                "INDEXED_FIELD == 'a' && ((_Delayed_ = true) && (INDEXED_FIELD == 'b' || EVENT_FIELD == 'c'))",
                ExecutableDeterminationVisitor.STATE.PARTIAL,
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                "INDEXED_FIELD == 'a' && ((_Delayed_ = true) && (INDEXED_FIELD == 'b' || EVENT_FIELD == 'c'))",
            },
            // Both global and field index are executable
            {
                "AndOrNested",
                "INDEXED_FIELD == 'a' && (INDEXED_FIELD == 'b' || (TF_FIELD == 'd' && EVENT_FIELD == 'c'))",
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                "INDEXED_FIELD == 'a' && (INDEXED_FIELD == 'b' || (TF_FIELD == 'd' && EVENT_FIELD == 'c'))",
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                "INDEXED_FIELD == 'a' && (INDEXED_FIELD == 'b' || (TF_FIELD == 'd' && EVENT_FIELD == 'c'))",
            },
            // Both global and field index result in a delayed node to become executable
            {
                "AndOrNestedDelayed",
                "INDEXED_FIELD == 'a' && (INDEXED_FIELD == 'b' || !(INDEXED_FIELD == 'd' && EVENT_FIELD == 'c'))",
                ExecutableDeterminationVisitor.STATE.PARTIAL,
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                "INDEXED_FIELD == 'a' && ((_Delayed_ = true) && (INDEXED_FIELD == 'b' || !(INDEXED_FIELD == 'd') || !(EVENT_FIELD == 'c')))",
                ExecutableDeterminationVisitor.STATE.PARTIAL,
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                "INDEXED_FIELD == 'a' && ((_Delayed_ = true) && (INDEXED_FIELD == 'b' || !(INDEXED_FIELD == 'd') || !(EVENT_FIELD == 'c')))",
            },
            // after adding delays these are both executable
            {
                "AndOrNestedDelayedError",
                "INDEXED_FIELD == 'a' && (INDEXED_FIELD == 'b' || !(TF_FIELD == 'd' && EVENT_FIELD == 'c'))",
                ExecutableDeterminationVisitor.STATE.PARTIAL,
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                "INDEXED_FIELD == 'a' && ((_Delayed_ = true) && (INDEXED_FIELD == 'b' || !(TF_FIELD == 'd') || !(EVENT_FIELD == 'c')))",
                ExecutableDeterminationVisitor.STATE.PARTIAL,
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                "INDEXED_FIELD == 'a' && ((_Delayed_ = true) && (INDEXED_FIELD == 'b' || !(TF_FIELD == 'd') || !(EVENT_FIELD == 'c')))",
            },
            // Global index results in a delayed node because it will not evaluate the negation
            // field index is executable with no delays because the negation of the OR means that if any term is found it can be removed as a candidate
            // When INDEXED_FIELD is found the document can be filtered, but otherwise the document would be filtered during jexl evaluation
            {
                "AndOrNestedOrDelayed",
                "INDEXED_FIELD == 'a' && (INDEXED_FIELD == 'b' || !(INDEXED_FIELD == 'd' || EVENT_FIELD == 'c'))",
                ExecutableDeterminationVisitor.STATE.PARTIAL,
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                "INDEXED_FIELD == 'a' && ((_Delayed_ = true) && (INDEXED_FIELD == 'b' || ((!(INDEXED_FIELD == 'd') && !(EVENT_FIELD == 'c')))))",
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                "INDEXED_FIELD == 'a' && (INDEXED_FIELD == 'b' || ((!(INDEXED_FIELD == 'd') && !(EVENT_FIELD == 'c'))))",
            },
            // Global can be executed after adding a delay
            // Field index will allow this to be executed with no delay because the context of the negation, see above
            {
                "AndOrNestedOrDelayedError",
                "INDEXED_FIELD == 'a' && (INDEXED_FIELD == 'b' || !(INDEX_ONLY_FIELD == 'd' || EVENT_FIELD == 'c'))",
                ExecutableDeterminationVisitor.STATE.PARTIAL,
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                "INDEXED_FIELD == 'a' && ((_Delayed_ = true) && (INDEXED_FIELD == 'b' || ((!(INDEX_ONLY_FIELD == 'd') && !(EVENT_FIELD == 'c')))))",
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                "INDEXED_FIELD == 'a' && (INDEXED_FIELD == 'b' || ((!(INDEX_ONLY_FIELD == 'd') && !(EVENT_FIELD == 'c'))))",
            },
        });
        // @formatter:on
    }
    
    // for parameterized set
    private String baseQuery;
    private ExecutableDeterminationVisitor.STATE expectedPreGlobalState;
    private ExecutableDeterminationVisitor.STATE expectedPostGlobalState;
    private String expectedGlobalIndexPushdown;
    private ExecutableDeterminationVisitor.STATE expectedPreFieldState;
    private ExecutableDeterminationVisitor.STATE expectedPostFieldState;
    private String expectedFieldIndexPushdown;
    
    // internal
    private HashSet<String> indexedFields;
    private HashSet<String> indexOnlyFields;
    private HashSet<String> nonEventFields;
    
    public PushdownUnexecutableNodesVisitorTest(String testName, String baseQuery, ExecutableDeterminationVisitor.STATE expectedPreGlobalState,
                    ExecutableDeterminationVisitor.STATE expectedPostGlobalState, String expectedGlobalIndexPushdown,
                    ExecutableDeterminationVisitor.STATE expectedPreFieldState, ExecutableDeterminationVisitor.STATE expectedPostFieldState,
                    String expectedFieldIndexPushdown) {
        this.baseQuery = baseQuery;
        this.expectedPreGlobalState = expectedPreGlobalState;
        this.expectedPostGlobalState = expectedPostGlobalState;
        this.expectedGlobalIndexPushdown = expectedGlobalIndexPushdown;
        this.expectedPreFieldState = expectedPreFieldState;
        this.expectedPostFieldState = expectedPostFieldState;
        this.expectedFieldIndexPushdown = expectedFieldIndexPushdown;
    }
    
    @Before
    public void setup() throws TableNotFoundException {
        config = createMock(ShardQueryConfiguration.class);
        helper = createMock(MetadataHelper.class);
        
        EasyMock.expect(config.getDatatypeFilter()).andReturn(null).anyTimes();
        
        indexedFields = new HashSet();
        indexedFields.add("INDEX_ONLY_FIELD");
        indexedFields.add("INDEXED_FIELD");
        indexedFields.add("TF_FIELD");
        
        indexOnlyFields = new HashSet<>();
        indexOnlyFields.add("INDEX_ONLY_FIELD");
        
        nonEventFields = new HashSet<>();
        nonEventFields.add("INDEX_ONLY_FIELD");
        nonEventFields.add("TF_FIELD");
        
        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        EasyMock.expect(helper.getIndexOnlyFields(null)).andReturn(indexOnlyFields).anyTimes();
        EasyMock.expect(helper.getNonEventFields(null)).andReturn(nonEventFields).anyTimes();
    }
    
    @Test
    public void testGlobalIndex() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery(baseQuery);
        
        replayAll();
        
        // global index
        Assert.assertEquals(expectedPreGlobalState, ExecutableDeterminationVisitor.getState(query, config, helper, false));
        JexlNode result = PushdownUnexecutableNodesVisitor.pushdownPredicates(query, false, config, indexedFields, indexOnlyFields, nonEventFields, helper);
        Assert.assertEquals(expectedGlobalIndexPushdown, JexlStringBuildingVisitor.buildQuery(result));
        Assert.assertEquals(expectedPostGlobalState, ExecutableDeterminationVisitor.getState(result, config, helper, false));
        
        verifyAll();
    }
    
    @Test
    public void testFieldIndex() throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery(baseQuery);
        
        replayAll();
        
        // field index
        Assert.assertEquals(expectedPreFieldState, ExecutableDeterminationVisitor.getState(query, config, helper, true));
        JexlNode result = PushdownUnexecutableNodesVisitor.pushdownPredicates(query, true, config, indexedFields, indexOnlyFields, nonEventFields, helper);
        Assert.assertEquals(expectedFieldIndexPushdown, JexlStringBuildingVisitor.buildQuery(result));
        Assert.assertEquals(expectedPostFieldState, ExecutableDeterminationVisitor.getState(result, config, helper, true));
        
        verifyAll();
    }
}
