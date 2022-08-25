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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashSet;
import java.util.stream.Stream;

public class PushdownUnexecutableNodesVisitorTest extends EasyMockSupport {
    private ShardQueryConfiguration config;
    private MetadataHelper helper;
    
    /**
     * Collection of tests
     *
     */
    static Stream<Arguments> data() {
        return Stream.of(
        // executable against the global index
        // executable against the field index after adding a delay
                        Arguments.of("AndNegatedAndPartial", "INDEXED_FIELD == 'a' && !(INDEX_ONLY_FIELD == 'b' && filter:includeRegex(INDEXED_FIELD, '.*'))",
                                        ExecutableDeterminationVisitor.STATE.EXECUTABLE, ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                                        "INDEXED_FIELD == 'a' && (!(INDEX_ONLY_FIELD == 'b') || !(filter:includeRegex(INDEXED_FIELD, '.*')))",
                                        ExecutableDeterminationVisitor.STATE.PARTIAL, ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                                        "INDEXED_FIELD == 'a' && ((_Delayed_ = true) && (!(INDEX_ONLY_FIELD == 'b') || !(filter:includeRegex(INDEXED_FIELD, '.*'))))"),
                        
                        // executable against the global index
                        // executable against the field index after adding a delay
                        Arguments.of("AndNegatedAndPartialExtended",
                                        "INDEXED_FIELD == 'a' && !(INDEX_ONLY_FIELD == 'b' && filter:includeRegex(INDEXED_FIELD, '.*')) && EVENT_FIELD == 'd'",
                                        ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                                        ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                                        "INDEXED_FIELD == 'a' && (!(INDEX_ONLY_FIELD == 'b') || !(filter:includeRegex(INDEXED_FIELD, '.*'))) && EVENT_FIELD == 'd'",
                                        ExecutableDeterminationVisitor.STATE.PARTIAL, ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                                        "INDEXED_FIELD == 'a' && ((_Delayed_ = true) && (!(INDEX_ONLY_FIELD == 'b') || !(filter:includeRegex(INDEXED_FIELD, '.*')))) && EVENT_FIELD == 'd'"),
                        // executable against the global index,
                        // executable against the field index after adding a delay
                        Arguments.of("AndNegatedAndPartialEventField", "INDEXED_FIELD == 'a' && !(INDEX_ONLY_FIELD == 'b' && EVENT_FIELD == 'c')",
                                        ExecutableDeterminationVisitor.STATE.EXECUTABLE, ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                                        "INDEXED_FIELD == 'a' && (!(INDEX_ONLY_FIELD == 'b') || !(EVENT_FIELD == 'c'))",
                                        ExecutableDeterminationVisitor.STATE.PARTIAL, ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                                        "INDEXED_FIELD == 'a' && ((_Delayed_ = true) && (!(INDEX_ONLY_FIELD == 'b') || !(EVENT_FIELD == 'c')))"),
                        
                        // no action necessary, should be straight up executable
                        Arguments.of("AndIndexedEvent", "INDEXED_FIELD == 'a' && EVENT_FIELD == 'b'", ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                                        ExecutableDeterminationVisitor.STATE.EXECUTABLE, "INDEXED_FIELD == 'a' && EVENT_FIELD == 'b'",
                                        ExecutableDeterminationVisitor.STATE.EXECUTABLE, ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                                        "INDEXED_FIELD == 'a' && EVENT_FIELD == 'b'"),
                        // executable against the global index, and executable against the field index since the OR is negated its sufficient for a single term
                        // to exclude, the other term
                        // would be found and excluded at document evaluation
                        Arguments.of("AndNegatedORExecutable", "INDEXED_FIELD == 'a' && !(INDEX_ONLY_FIELD == 'b' || EVENT_FIELD == 'c')",
                                        ExecutableDeterminationVisitor.STATE.EXECUTABLE, ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                                        "INDEXED_FIELD == 'a' && !(INDEX_ONLY_FIELD == 'b') && !(EVENT_FIELD == 'c')",
                                        ExecutableDeterminationVisitor.STATE.EXECUTABLE, ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                                        "INDEXED_FIELD == 'a' && !(INDEX_ONLY_FIELD == 'b') && !(EVENT_FIELD == 'c')"),
                        // not executable against the global index because it will always mark a negation as non-executable instead of evaluating the subtree,
                        // the field index is executable
                        // because the negations result in EXECUTABLE || (!(EXECUTABLE) && !(NON_EXECUTABLE)) == EXECUTABLE || (EXECUTABLE) == EXECUTABLE
                        Arguments.of("OrAndNegatedLeaf", "INDEXED_FIELD == 'a' || (!(INDEX_ONLY_FIELD == 'b') && !(EVENT_FIELD == 'c'))",
                                        ExecutableDeterminationVisitor.STATE.PARTIAL, ExecutableDeterminationVisitor.STATE.PARTIAL,
                                        "INDEXED_FIELD == 'a' || (!(INDEX_ONLY_FIELD == 'b') && !(EVENT_FIELD == 'c'))",
                                        ExecutableDeterminationVisitor.STATE.NEGATED_EXECUTABLE, ExecutableDeterminationVisitor.STATE.NEGATED_EXECUTABLE,
                                        "INDEXED_FIELD == 'a' || (!(INDEX_ONLY_FIELD == 'b') && !(EVENT_FIELD == 'c'))"),
                        // top level or against the global index is non-executable due to the negation
                        // against the field index is partial because the negation requires that all components of the AND are executable and the EVENT_FIELD is
                        // not
                        Arguments.of("OrNegatedAnd", "INDEXED_FIELD == 'a' || !(INDEX_ONLY_FIELD == 'b' && EVENT_FIELD == 'c')",
                                        ExecutableDeterminationVisitor.STATE.PARTIAL, ExecutableDeterminationVisitor.STATE.PARTIAL,
                                        "INDEXED_FIELD == 'a' || !(INDEX_ONLY_FIELD == 'b') || !(EVENT_FIELD == 'c')",
                                        ExecutableDeterminationVisitor.STATE.PARTIAL, ExecutableDeterminationVisitor.STATE.PARTIAL,
                                        "INDEXED_FIELD == 'a' || !(INDEX_ONLY_FIELD == 'b') || !(EVENT_FIELD == 'c')"),
                        // Global index cannot be executed due to partial result (no negations)
                        // Field index is executable becuase INDEX_ONLY_FIELD == 'b' is EXECUTABLE and the negation attached to the OR makes that sufficient for
                        // exclusion
                        Arguments.of("OrNegatedOr", "INDEXED_FIELD == 'a' || !(INDEX_ONLY_FIELD == 'b' || EVENT_FIELD == 'c')",
                                        ExecutableDeterminationVisitor.STATE.PARTIAL, ExecutableDeterminationVisitor.STATE.PARTIAL,
                                        "INDEXED_FIELD == 'a' || (!(INDEX_ONLY_FIELD == 'b') && !(EVENT_FIELD == 'c'))",
                                        ExecutableDeterminationVisitor.STATE.NEGATED_EXECUTABLE, ExecutableDeterminationVisitor.STATE.NEGATED_EXECUTABLE,
                                        "INDEXED_FIELD == 'a' || (!(INDEX_ONLY_FIELD == 'b') && !(EVENT_FIELD == 'c'))"),
                        // Both global and field can be run
                        Arguments.of("AndOr", "INDEXED_FIELD == 'a' && (INDEXED_FIELD == 'b' || TF_FIELD == 'c')",
                                        ExecutableDeterminationVisitor.STATE.EXECUTABLE, ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                                        "INDEXED_FIELD == 'a' && (INDEXED_FIELD == 'b' || TF_FIELD == 'c')", ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                                        ExecutableDeterminationVisitor.STATE.EXECUTABLE, "INDEXED_FIELD == 'a' && (INDEXED_FIELD == 'b' || TF_FIELD == 'c')"),
                        // Both global and field index must add a delay to become executable
                        Arguments.of("AndOrDelayed", "INDEXED_FIELD == 'a' && (INDEXED_FIELD == 'b' || EVENT_FIELD == 'c')",
                                        ExecutableDeterminationVisitor.STATE.PARTIAL, ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                                        "INDEXED_FIELD == 'a' && ((_Delayed_ = true) && (INDEXED_FIELD == 'b' || EVENT_FIELD == 'c'))",
                                        ExecutableDeterminationVisitor.STATE.PARTIAL, ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                                        "INDEXED_FIELD == 'a' && ((_Delayed_ = true) && (INDEXED_FIELD == 'b' || EVENT_FIELD == 'c'))"),
                        // Both global and field index are executable
                        Arguments.of("AndOrNested", "INDEXED_FIELD == 'a' && (INDEXED_FIELD == 'b' || (TF_FIELD == 'd' && EVENT_FIELD == 'c'))",
                                        ExecutableDeterminationVisitor.STATE.EXECUTABLE, ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                                        "INDEXED_FIELD == 'a' && (INDEXED_FIELD == 'b' || (TF_FIELD == 'd' && EVENT_FIELD == 'c'))",
                                        ExecutableDeterminationVisitor.STATE.EXECUTABLE, ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                                        "INDEXED_FIELD == 'a' && (INDEXED_FIELD == 'b' || (TF_FIELD == 'd' && EVENT_FIELD == 'c'))"),
                        // Both global and field index result in a delayed node to become executable
                        Arguments.of("AndOrNestedDelayed",
                                        "INDEXED_FIELD == 'a' && (INDEXED_FIELD == 'b' || !(INDEXED_FIELD == 'd' && EVENT_FIELD == 'c'))",
                                        ExecutableDeterminationVisitor.STATE.PARTIAL,
                                        ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                                        "INDEXED_FIELD == 'a' && ((_Delayed_ = true) && (INDEXED_FIELD == 'b' || !(INDEXED_FIELD == 'd') || !(EVENT_FIELD == 'c')))",
                                        ExecutableDeterminationVisitor.STATE.PARTIAL, ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                                        "INDEXED_FIELD == 'a' && ((_Delayed_ = true) && (INDEXED_FIELD == 'b' || !(INDEXED_FIELD == 'd') || !(EVENT_FIELD == 'c')))"),
                        // after adding delays these are both executable
                        Arguments.of("AndOrNestedDelayedError",
                                        "INDEXED_FIELD == 'a' && (INDEXED_FIELD == 'b' || !(TF_FIELD == 'd' && EVENT_FIELD == 'c'))",
                                        ExecutableDeterminationVisitor.STATE.PARTIAL,
                                        ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                                        "INDEXED_FIELD == 'a' && ((_Delayed_ = true) && (INDEXED_FIELD == 'b' || !(TF_FIELD == 'd') || !(EVENT_FIELD == 'c')))",
                                        ExecutableDeterminationVisitor.STATE.PARTIAL, ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                                        "INDEXED_FIELD == 'a' && ((_Delayed_ = true) && (INDEXED_FIELD == 'b' || !(TF_FIELD == 'd') || !(EVENT_FIELD == 'c')))"),
                        // Global index results in a delayed node because it will not evaluate the negation
                        // field index is executable with no delays because the negation of the OR means that if any term is found it can be removed as a
                        // candidate
                        // When INDEXED_FIELD is found the document can be filtered, but otherwise the document would be filtered during jexl evaluation
                        Arguments.of("AndOrNestedOrDelayed",
                                        "INDEXED_FIELD == 'a' && (INDEXED_FIELD == 'b' || !(INDEXED_FIELD == 'd' || EVENT_FIELD == 'c'))",
                                        ExecutableDeterminationVisitor.STATE.PARTIAL,
                                        ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                                        "INDEXED_FIELD == 'a' && ((_Delayed_ = true) && (INDEXED_FIELD == 'b' || (!(INDEXED_FIELD == 'd') && !(EVENT_FIELD == 'c'))))",
                                        ExecutableDeterminationVisitor.STATE.EXECUTABLE, ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                                        "INDEXED_FIELD == 'a' && (INDEXED_FIELD == 'b' || (!(INDEXED_FIELD == 'd') && !(EVENT_FIELD == 'c')))"),
                        // Global can be executed after adding a delay
                        // Field index will allow this to be executed with no delay because the context of the negation, see above
                        Arguments.of("AndOrNestedOrDelayedError",
                                        "INDEXED_FIELD == 'a' && (INDEXED_FIELD == 'b' || !(INDEX_ONLY_FIELD == 'd' || EVENT_FIELD == 'c'))",
                                        ExecutableDeterminationVisitor.STATE.PARTIAL,
                                        ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                                        "INDEXED_FIELD == 'a' && ((_Delayed_ = true) && (INDEXED_FIELD == 'b' || (!(INDEX_ONLY_FIELD == 'd') && !(EVENT_FIELD == 'c'))))",
                                        ExecutableDeterminationVisitor.STATE.EXECUTABLE, ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                                        "INDEXED_FIELD == 'a' && (INDEXED_FIELD == 'b' || (!(INDEX_ONLY_FIELD == 'd') && !(EVENT_FIELD == 'c')))"));
        
        // @formatter:on
    }
    
    // internal
    private HashSet<String> indexedFields;
    private HashSet<String> indexOnlyFields;
    private HashSet<String> nonEventFields;
    
    @BeforeEach
    public void setup() throws TableNotFoundException {
        config = createMock(ShardQueryConfiguration.class);
        helper = createMock(MetadataHelper.class);
        
        EasyMock.expect(config.getDatatypeFilter()).andReturn(null).anyTimes();
        
        indexedFields = new HashSet<>();
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
    
    @ParameterizedTest(name = "{index}: ({0}")
    @MethodSource("data")
    public void testGlobalIndex(String queryName, String baseQuery, ExecutableDeterminationVisitor.STATE expectedPreGlobalState,
                    ExecutableDeterminationVisitor.STATE expectedPostGlobalState, String expectedGlobalIndexPushdown,
                    ExecutableDeterminationVisitor.STATE expectedPreFieldState, ExecutableDeterminationVisitor.STATE expectedPostFieldState,
                    String expectedFieldIndexPushdown) throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery(baseQuery);
        
        replayAll();
        
        // global index
        Assertions.assertEquals(expectedPreGlobalState, ExecutableDeterminationVisitor.getState(query, config, helper, false));
        JexlNode result = PushdownUnexecutableNodesVisitor.pushdownPredicates(query, false, config, indexedFields, indexOnlyFields, nonEventFields, helper);
        Assertions.assertEquals(expectedGlobalIndexPushdown, JexlStringBuildingVisitor.buildQuery(result));
        Assertions.assertEquals(expectedPostGlobalState, ExecutableDeterminationVisitor.getState(result, config, helper, false));
        
        verifyAll();
    }
    
    @ParameterizedTest(name = "{index}: ({0}")
    @MethodSource("data")
    public void testFieldIndex(String queryName, String baseQuery, ExecutableDeterminationVisitor.STATE expectedPreGlobalState,
                    ExecutableDeterminationVisitor.STATE expectedPostGlobalState, String expectedGlobalIndexPushdown,
                    ExecutableDeterminationVisitor.STATE expectedPreFieldState, ExecutableDeterminationVisitor.STATE expectedPostFieldState,
                    String expectedFieldIndexPushdown) throws ParseException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery(baseQuery);
        
        replayAll();
        
        // field index
        Assertions.assertEquals(expectedPreFieldState, ExecutableDeterminationVisitor.getState(query, config, helper, true));
        JexlNode result = PushdownUnexecutableNodesVisitor.pushdownPredicates(query, true, config, indexedFields, indexOnlyFields, nonEventFields, helper);
        Assertions.assertEquals(expectedFieldIndexPushdown, JexlStringBuildingVisitor.buildQuery(result));
        Assertions.assertEquals(expectedPostFieldState, ExecutableDeterminationVisitor.getState(result, config, helper, true));
        
        verifyAll();
    }
}
