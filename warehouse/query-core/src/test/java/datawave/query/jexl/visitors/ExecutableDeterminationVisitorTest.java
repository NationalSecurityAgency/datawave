package datawave.query.jexl.visitors;

import com.google.common.collect.Sets;
import datawave.query.Constants;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.visitors.ExecutableDeterminationVisitor.STATE;
import datawave.query.util.MetadataHelper;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl2.parser.ASTAdditiveNode;
import org.apache.commons.jexl2.parser.ASTAdditiveOperator;
import org.apache.commons.jexl2.parser.ASTAmbiguous;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTArrayAccess;
import org.apache.commons.jexl2.parser.ASTArrayLiteral;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTBitwiseAndNode;
import org.apache.commons.jexl2.parser.ASTBitwiseComplNode;
import org.apache.commons.jexl2.parser.ASTBitwiseOrNode;
import org.apache.commons.jexl2.parser.ASTBitwiseXorNode;
import org.apache.commons.jexl2.parser.ASTBlock;
import org.apache.commons.jexl2.parser.ASTConstructorNode;
import org.apache.commons.jexl2.parser.ASTDivNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTEmptyFunction;
import org.apache.commons.jexl2.parser.ASTFalseNode;
import org.apache.commons.jexl2.parser.ASTFloatLiteral;
import org.apache.commons.jexl2.parser.ASTForeachStatement;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTIfStatement;
import org.apache.commons.jexl2.parser.ASTIntegerLiteral;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTMapEntry;
import org.apache.commons.jexl2.parser.ASTMapLiteral;
import org.apache.commons.jexl2.parser.ASTMethodNode;
import org.apache.commons.jexl2.parser.ASTModNode;
import org.apache.commons.jexl2.parser.ASTMulNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.ASTNullLiteral;
import org.apache.commons.jexl2.parser.ASTNumberLiteral;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.ASTReturnStatement;
import org.apache.commons.jexl2.parser.ASTSizeFunction;
import org.apache.commons.jexl2.parser.ASTSizeMethod;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.ASTTernaryNode;
import org.apache.commons.jexl2.parser.ASTTrueNode;
import org.apache.commons.jexl2.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl2.parser.ASTVar;
import org.apache.commons.jexl2.parser.ASTWhileStatement;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.commons.jexl2.parser.SimpleNode;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ExecutableDeterminationVisitorTest {
    
    private ShardQueryConfiguration shardQueryConfiguration;
    
    private MetadataHelper metadataHelper;
    
    private ExecutableDeterminationVisitor visitor;
    
    private List<String> output;
    
    @Before
    public void setUp() throws Exception {
        initVisitor(true, false);
    }
    
    private void initVisitor(final boolean forFieldIndex, final boolean debugStates) {
        shardQueryConfiguration = mock(ShardQueryConfiguration.class);
        metadataHelper = mock(MetadataHelper.class);
        output = new ArrayList<>();
        // Instantiate the test class with debug output enabled.
        visitor = new ExecutableDeterminationVisitor(shardQueryConfiguration, metadataHelper, forFieldIndex, output, debugStates);
    }
    
    @Test
    public void visitASTEQNode_givenIsNoFieldOnly_returnsIgnorableState() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(Constants.NO_FIELD + " == 'b'");
        ASTEQNode node = (ASTEQNode) script.jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataEQNode[_NOFIELD_] -> IGNORABLE");
    }
    
    @Test
    public void visitASTEQNode_givenIsAnyField_returnsExecutableState() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(Constants.ANY_FIELD + " == 'b'");
        ASTEQNode node = (ASTEQNode) script.jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.EXECUTABLE, state);
        assertOutputLine(0, "dataEQNode[_ANYFIELD_] -> EXECUTABLE");
    }
    
    @Test
    public void visitASTEQNode_givenIsUnindexed_returnsNonExecutableState() throws ParseException {
        expect(shardQueryConfiguration.getIndexedFields()).andReturn(Sets.newHashSet("B")).anyTimes();
        replay(shardQueryConfiguration);
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A == 'b'");
        ASTEQNode node = (ASTEQNode) script.jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
        assertOutputLine(0, "dataEQNode[A] -> NON_EXECUTABLE");
    }
    
    @Test
    public void visitASTEQNode_givenIsIndexedAndNonExecutableWithIndexOnly_returnsErrorState() throws ParseException, TableNotFoundException {
        expect(shardQueryConfiguration.getIndexedFields()).andReturn(Sets.newHashSet("A")).anyTimes();
        expect(shardQueryConfiguration.getDatatypeFilter()).andReturn(Sets.newHashSet()).anyTimes();
        expect(metadataHelper.getIndexOnlyFields(anyObject())).andReturn(Sets.newHashSet("A")).anyTimes();
        replay(shardQueryConfiguration, metadataHelper);
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A == null");
        ASTEQNode node = (ASTEQNode) script.jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.ERROR, state);
        assertOutputLine(0, "data      Identifier -> IGNORABLE");
        assertOutputLine(1, "data    Reference[IGNORABLE] -> IGNORABLE");
        assertOutputLine(2, "data  NullLiteral -> NON_EXECUTABLE");
        assertOutputLine(3, "dataEQNode[A] -> ERROR");
    }
    
    @Test
    public void visitASTEQNode_givenIsIndexedAndNonExecutableAndIsNotIndexOnly_returnsNonExecutableState() throws ParseException, TableNotFoundException {
        expect(shardQueryConfiguration.getIndexedFields()).andReturn(Sets.newHashSet("A")).anyTimes();
        expect(shardQueryConfiguration.getDatatypeFilter()).andReturn(Sets.newHashSet()).anyTimes();
        expect(metadataHelper.getIndexOnlyFields(anyObject())).andReturn(Sets.newHashSet("B")).anyTimes();
        replay(shardQueryConfiguration, metadataHelper);
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A == null");
        ASTEQNode node = (ASTEQNode) script.jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
        assertOutputLine(0, "data      Identifier -> IGNORABLE");
        assertOutputLine(1, "data    Reference[IGNORABLE] -> IGNORABLE");
        assertOutputLine(2, "data  NullLiteral -> NON_EXECUTABLE");
        assertOutputLine(3, "dataEQNode[A] -> NON_EXECUTABLE");
    }
    
    @Test
    public void visitASTEQNode_givenIsIndexedAndExecutable_returnsExecutableState() throws ParseException {
        expect(shardQueryConfiguration.getIndexedFields()).andReturn(Sets.newHashSet("A")).anyTimes();
        replay(shardQueryConfiguration);
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A == 'b'");
        ASTEQNode node = (ASTEQNode) script.jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.EXECUTABLE, state);
        assertOutputLine(0, "data      Identifier -> IGNORABLE");
        assertOutputLine(1, "data    Reference[IGNORABLE] -> IGNORABLE");
        assertOutputLine(2, "data      StringLiteral -> IGNORABLE");
        assertOutputLine(3, "data    Reference[IGNORABLE] -> IGNORABLE");
        assertOutputLine(4, "dataEQNode[A] -> EXECUTABLE");
    }
    
    @Test
    public void visitASTNENode_givenIsNoFieldOnly_returnsIgnorableState() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(Constants.NO_FIELD + " != 'b'");
        ASTNENode node = (ASTNENode) script.jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataNENode(_NOFIELD_) -> IGNORABLE");
    }
    
    @Test
    public void visitASTNENode_givenIsAnyField_returnsNonExecutableState() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(Constants.ANY_FIELD + " != 'b'");
        ASTNENode node = (ASTNENode) script.jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
        assertOutputLine(0, "dataNENode(_ANYFIELD_) -> NON_EXECUTABLE");
    }
    
    @Test
    public void visitASTNENode_givenIsUnindexed_returnsNonExecutableState() throws ParseException {
        expect(shardQueryConfiguration.getIndexedFields()).andReturn(Sets.newHashSet("B")).anyTimes();
        replay(shardQueryConfiguration);
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A != 'b'");
        ASTNENode node = (ASTNENode) script.jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
        assertOutputLine(0, "dataNENode(A) -> NON_EXECUTABLE");
    }
    
    @Test
    public void visitASTNENode_givenIndexedAndForFieldIndexIsTrue_returnsExecutableState() throws ParseException {
        expect(shardQueryConfiguration.getIndexedFields()).andReturn(Sets.newHashSet("A")).anyTimes();
        replay(shardQueryConfiguration);
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A != 'b'");
        ASTNENode node = (ASTNENode) script.jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.EXECUTABLE, state);
        assertOutputLine(0, "dataNENode(A) -> EXECUTABLE");
    }
    
    @Test
    public void visitASTNENode_givenIsIndexedAndForFieldIndexIsFalse_returnsNonExecutableState() throws ParseException {
        initVisitor(false, false);
        
        expect(shardQueryConfiguration.getIndexedFields()).andReturn(Sets.newHashSet("B")).anyTimes();
        replay(shardQueryConfiguration);
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A != 'b'");
        ASTNENode node = (ASTNENode) script.jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
        assertOutputLine(0, "dataNENode(A) -> NON_EXECUTABLE");
    }
    
    @Test
    public void visitASTReference_givenInstanceOfExceededTermThresholdMarkerJexlNode_returnsNonExecutableState() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("((ExceededTermThresholdMarkerJexlNode = true) && A == 'b')");
        ASTReference node = (ASTReference) script.jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
    }
    
    @Test
    public void visitASTReference_givenInstanceOfExceededValueThresholdMarkerJexlNode_returnsExecutableState() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("((ExceededValueThresholdMarkerJexlNode = true) && A == 'b')");
        ASTReference node = (ASTReference) script.jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.EXECUTABLE, state);
    }
    
    @Test
    public void visitASTReference_givenInstanceOfASTDelayedPredicateAndIsNonEvent_returnsErrorState() throws ParseException, TableNotFoundException {
        expect(metadataHelper.getNonEventFields(anyObject())).andReturn(Sets.newHashSet("A")).anyTimes();
        replay(metadataHelper);
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("((ASTDelayedPredicate = true) && A == 'b')");
        ASTReference node = (ASTReference) script.jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.ERROR, state);
    }
    
    @Test
    public void visitASTReference_givenInstanceOfASTDelayedPredicateAndIsNotNonEvent_returnsNonExecutableState() throws ParseException, TableNotFoundException {
        expect(metadataHelper.getNonEventFields(anyObject())).andReturn(Sets.newHashSet("B")).anyTimes();
        replay(metadataHelper);
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("((ASTDelayedPredicate = true) && A == 'b')");
        ASTReference node = (ASTReference) script.jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
    }
    
    @Test
    public void visitASTReference_givenInstanceOfIndexHoleMarkerJexlNode_returnsNonExecutableState() throws ParseException, TableNotFoundException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("((IndexHoleMarkerJexlNode = true) && A == 'b')");
        ASTReference node = (ASTReference) script.jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
    }
    
    @Test
    public void visitASTReference_givenNoTypeOrChildren_returnsPartialState() {
        ASTReference node = new ASTReference(ParserTreeConstants.JJTREFERENCE);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.PARTIAL, state);
        assertOutputLine(0, "data  Reference[] -> PARTIAL");
    }
    
    @Test
    public void visitASTReference_givenNoTypeAndSingleIgnorableState_returnsIgnorableState() {
        JexlNode child = createMockChild(STATE.IGNORABLE);
        expect(child.jjtAccept(anyObject(QueryPropertyMarkerVisitor.class), anyObject())).andReturn(null).anyTimes();
        replay(child);
        ASTReference node = new ASTReference(ParserTreeConstants.JJTREFERENCE);
        addMockChildren(node, child);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "data  Reference[IGNORABLE] -> IGNORABLE");
    }
    
    @Test
    public void visitASTReference_givenNoTypeSingleNonExecutableState_returnsNonExecutableState() {
        JexlNode child = createMockChild(STATE.NON_EXECUTABLE);
        expect(child.jjtAccept(anyObject(QueryPropertyMarkerVisitor.class), anyObject())).andReturn(null).anyTimes();
        replay(child);
        ASTReference parent = new ASTReference(ParserTreeConstants.JJTREFERENCE);
        addMockChildren(parent, child);
        
        STATE state = (STATE) visitor.visit(parent, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
        assertOutputLine(0, "data  Reference[NON_EXECUTABLE] -> NON_EXECUTABLE");
    }
    
    @Test
    public void visitASTReference_givenNoTypeAndNonExecutableAndIgnorable_returnsNonExecutableState() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.NON_EXECUTABLE);
        expect(child1.jjtAccept(anyObject(QueryPropertyMarkerVisitor.class), anyObject())).andReturn(null).anyTimes();
        expect(child2.jjtAccept(anyObject(QueryPropertyMarkerVisitor.class), anyObject())).andReturn(null).anyTimes();
        replay(child1, child2);
        ASTReference parent = new ASTReference(ParserTreeConstants.JJTREFERENCE);
        addMockChildren(parent, child1, child2);
        
        STATE state = (STATE) visitor.visit(parent, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
        assertOutputLine(0, "data  Reference[NON_EXECUTABLE, IGNORABLE] -> NON_EXECUTABLE");
    }
    
    @Test
    public void visitASTReference_givenNoTypeAndExecutableAndIgnorable_returnsExecutableState() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.EXECUTABLE);
        expect(child1.jjtAccept(anyObject(QueryPropertyMarkerVisitor.class), anyObject())).andReturn(null).anyTimes();
        expect(child2.jjtAccept(anyObject(QueryPropertyMarkerVisitor.class), anyObject())).andReturn(null).anyTimes();
        replay(child1, child2);
        ASTReference parent = new ASTReference(ParserTreeConstants.JJTREFERENCE);
        addMockChildren(parent, child1, child2);
        
        STATE state = (STATE) visitor.visit(parent, "data");
        
        assertEquals(STATE.EXECUTABLE, state);
        assertOutputLine(0, "data  Reference[EXECUTABLE, IGNORABLE] -> EXECUTABLE");
    }
    
    @Test
    public void visitASTReference_givenNoTypeAndThreeStatesWhereOneIsIgnorableAndOneIsError_returnsErrorState() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.NON_EXECUTABLE);
        JexlNode child3 = createMockChild(STATE.ERROR);
        expect(child1.jjtAccept(anyObject(QueryPropertyMarkerVisitor.class), anyObject())).andReturn(null).anyTimes();
        expect(child2.jjtAccept(anyObject(QueryPropertyMarkerVisitor.class), anyObject())).andReturn(null).anyTimes();
        expect(child3.jjtAccept(anyObject(QueryPropertyMarkerVisitor.class), anyObject())).andReturn(null).anyTimes();
        replay(child1, child2, child3);
        ASTReference parent = new ASTReference(ParserTreeConstants.JJTREFERENCE);
        addMockChildren(parent, child1, child2, child3);
        
        STATE state = (STATE) visitor.visit(parent, "data");
        
        assertEquals(STATE.ERROR, state);
        assertOutputLine(0, "data  Reference[ERROR, NON_EXECUTABLE, IGNORABLE] -> ERROR");
    }
    
    @Test
    public void visitASTReference_givenNoTypeAndThreeStatesWhereOneIsIgnorableAndOneIsPartial_returnsPartialState() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.NON_EXECUTABLE);
        JexlNode child3 = createMockChild(STATE.PARTIAL);
        expect(child1.jjtAccept(anyObject(QueryPropertyMarkerVisitor.class), anyObject())).andReturn(null).anyTimes();
        expect(child2.jjtAccept(anyObject(QueryPropertyMarkerVisitor.class), anyObject())).andReturn(null).anyTimes();
        expect(child3.jjtAccept(anyObject(QueryPropertyMarkerVisitor.class), anyObject())).andReturn(null).anyTimes();
        replay(child1, child2, child3);
        ASTReference parent = new ASTReference(ParserTreeConstants.JJTREFERENCE);
        addMockChildren(parent, child1, child2, child3);
        
        STATE state = (STATE) visitor.visit(parent, "data");
        
        assertEquals(STATE.PARTIAL, state);
        assertOutputLine(0, "data  Reference[PARTIAL, NON_EXECUTABLE, IGNORABLE] -> PARTIAL");
    }
    
    @Test
    public void visitASTERNode_givenNoFieldOnlyIdentifier_returnsIgnorableState() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(Constants.NO_FIELD + " =~ 'b'");
        ASTERNode node = (ASTERNode) script.jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataERNode(_NOFIELD_) -> IGNORABLE");
    }
    
    @Test
    public void visitASTERNode_givenAnyFieldOnlyIdentifier_returnsExecutableState() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(Constants.ANY_FIELD + " =~ 'b'");
        ASTERNode node = (ASTERNode) script.jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.EXECUTABLE, state);
        assertOutputLine(0, "dataERNode(_ANYFIELD_) -> EXECUTABLE");
    }
    
    @Test
    public void visitASTERNode_givenUnindexedIdentifier_returnsNonExecutableState() throws ParseException {
        expect(shardQueryConfiguration.getIndexedFields()).andReturn(Sets.newHashSet("B")).anyTimes();
        replay(shardQueryConfiguration);
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A =~ 'b'");
        ASTERNode node = (ASTERNode) script.jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
        assertOutputLine(0, "dataERNode(A) -> NON_EXECUTABLE");
    }
    
    @Test
    public void visitASTERNode_givenIndexedIdentifier_returnsExecutableState() throws ParseException {
        expect(shardQueryConfiguration.getIndexedFields()).andReturn(Sets.newHashSet("A")).anyTimes();
        replay(shardQueryConfiguration);
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A =~ 'b'");
        ASTERNode node = (ASTERNode) script.jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.EXECUTABLE, state);
        assertOutputLine(0, "dataERNode(A) -> EXECUTABLE");
    }
    
    @Test
    public void visitASTNRNode_givenNoFieldOnlyIdentifier_returnsIgnorableState() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(Constants.NO_FIELD + " !~ 'b'");
        ASTNRNode node = (ASTNRNode) script.jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataNRNode(_NOFIELD_) -> IGNORABLE");
    }
    
    @Test
    public void visitASTNRNode_givenIsNonEvent_returnsErrorState() throws ParseException, TableNotFoundException {
        expect(metadataHelper.getNonEventFields(anyObject())).andReturn(Sets.newHashSet("A"));
        replay(metadataHelper);
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A !~ 'b'");
        ASTNRNode node = (ASTNRNode) script.jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.ERROR, state);
        assertOutputLine(0, "dataNRNode(A) -> ERROR");
    }
    
    @Test
    public void visitASTNRNode_givenIsNotNonEvent_returnsNonExecutableState() throws ParseException, TableNotFoundException {
        expect(metadataHelper.getNonEventFields(anyObject())).andReturn(Sets.newHashSet("B"));
        replay(metadataHelper);
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A !~ 'b'");
        ASTNRNode node = (ASTNRNode) script.jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
        assertOutputLine(0, "dataNRNode(A) -> NON_EXECUTABLE");
    }
    
    @Test
    public void visitASTGENode_givenNoFieldOnlyIdentifier_returnsIgnorableState() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(Constants.NO_FIELD + " >= 'b'");
        ASTGENode node = (ASTGENode) script.jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataGENode(_NOFIELD_) -> IGNORABLE");
    }
    
    @Test
    public void visitASTGENode_givenIsWithinBoundedRange_returnsExecutableState() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A >= 'b' && A < 'a'");
        ASTGENode node = (ASTGENode) script.jjtGetChild(0).jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.EXECUTABLE, state);
        assertOutputLine(0, "dataGENode(A) -> EXECUTABLE");
    }
    
    @Test
    public void visitASTGENode_givenIsNonEvent_returnsErrorState() throws ParseException, TableNotFoundException {
        expect(metadataHelper.getNonEventFields(anyObject())).andReturn(Sets.newHashSet("A"));
        replay(metadataHelper);
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A >= 'b'");
        ASTGENode node = (ASTGENode) script.jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.ERROR, state);
        assertOutputLine(0, "dataGENode(A) -> ERROR");
    }
    
    @Test
    public void visitASTGENode_givenIsNotNonEvent_returnsNonExecutableState() throws ParseException, TableNotFoundException {
        expect(metadataHelper.getNonEventFields(anyObject())).andReturn(Sets.newHashSet("B"));
        replay(metadataHelper);
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A >= 'b'");
        ASTGENode node = (ASTGENode) script.jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
        assertOutputLine(0, "dataGENode(A) -> NON_EXECUTABLE");
    }
    
    @Test
    public void visitASTGTNode_givenNoFieldOnlyIdentifier_returnsIgnorableState() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(Constants.NO_FIELD + " > 'b'");
        ASTGTNode node = (ASTGTNode) script.jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataGTNode(_NOFIELD_) -> IGNORABLE");
    }
    
    @Test
    public void visitASTGTNode_givenIsWithinBoundedRange_returnsExecutableState() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A > 'b' && A < 'a'");
        ASTGTNode node = (ASTGTNode) script.jjtGetChild(0).jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.EXECUTABLE, state);
        assertOutputLine(0, "dataGTNode(A) -> EXECUTABLE");
    }
    
    @Test
    public void visitASTGTNode_givenIsNonEvent_returnsErrorState() throws ParseException, TableNotFoundException {
        expect(metadataHelper.getNonEventFields(anyObject())).andReturn(Sets.newHashSet("A"));
        replay(metadataHelper);
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A > 'b'");
        ASTGTNode node = (ASTGTNode) script.jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.ERROR, state);
        assertOutputLine(0, "dataGTNode(A) -> ERROR");
    }
    
    @Test
    public void visitASTGTNode_givenIsNotNonEvent_returnsNonExecutableState() throws ParseException, TableNotFoundException {
        expect(metadataHelper.getNonEventFields(anyObject())).andReturn(Sets.newHashSet("B"));
        replay(metadataHelper);
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A > 'b'");
        ASTGTNode node = (ASTGTNode) script.jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
        assertOutputLine(0, "dataGTNode(A) -> NON_EXECUTABLE");
    }
    
    @Test
    public void visitASTLENode_givenNoFieldOnlyIdentifier_returnsIgnorableState() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(Constants.NO_FIELD + " <= 'b'");
        ASTLENode node = (ASTLENode) script.jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataLENode(_NOFIELD_) -> IGNORABLE");
    }
    
    @Test
    public void visitASTLENode_givenIsWithinBoundedRange_returnsExecutableState() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A <= 'b' && A > 'a'");
        ASTLENode node = (ASTLENode) script.jjtGetChild(0).jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.EXECUTABLE, state);
        assertOutputLine(0, "dataLENode(A) -> EXECUTABLE");
    }
    
    @Test
    public void visitASTLENode_givenIsNonEvent_returnsErrorState() throws ParseException, TableNotFoundException {
        expect(metadataHelper.getNonEventFields(anyObject())).andReturn(Sets.newHashSet("A"));
        replay(metadataHelper);
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A <= 'b'");
        ASTLENode node = (ASTLENode) script.jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.ERROR, state);
        assertOutputLine(0, "dataLENode(A) -> ERROR");
    }
    
    @Test
    public void visitASTLENode_givenIsNotNonEvent_returnsNonExecutableState() throws ParseException, TableNotFoundException {
        expect(metadataHelper.getNonEventFields(anyObject())).andReturn(Sets.newHashSet("B"));
        replay(metadataHelper);
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A <= 'b'");
        ASTLENode node = (ASTLENode) script.jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
        assertOutputLine(0, "dataLENode(A) -> NON_EXECUTABLE");
    }
    
    @Test
    public void visitASTLTNode_givenNoFieldOnlyIdentifier_returnsIgnorableState() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(Constants.NO_FIELD + " < 'b'");
        ASTLTNode node = (ASTLTNode) script.jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataLTNode(_NOFIELD_) -> IGNORABLE");
    }
    
    @Test
    public void visitASTLTNode_givenIsWithinBoundedRange_returnsExecutableState() throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A < 'b' && A > 'a'");
        ASTLTNode node = (ASTLTNode) script.jjtGetChild(0).jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.EXECUTABLE, state);
        assertOutputLine(0, "dataLTNode(A) -> EXECUTABLE");
    }
    
    @Test
    public void visitASTLTNode_givenIsNonEvent_returnsErrorState() throws ParseException, TableNotFoundException {
        expect(metadataHelper.getNonEventFields(anyObject())).andReturn(Sets.newHashSet("A"));
        replay(metadataHelper);
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A < 'b'");
        ASTLTNode node = (ASTLTNode) script.jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.ERROR, state);
        assertOutputLine(0, "dataLTNode(A) -> ERROR");
    }
    
    @Test
    public void visitASTLTNode_givenIsNotNonEvent_returnsNonExecutableState() throws ParseException, TableNotFoundException {
        expect(metadataHelper.getNonEventFields(anyObject())).andReturn(Sets.newHashSet("B"));
        replay(metadataHelper);
        
        ASTJexlScript script = JexlASTHelper.parseJexlQuery("A < 'b'");
        ASTLTNode node = (ASTLTNode) script.jjtGetChild(0);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
        assertOutputLine(0, "dataLTNode(A) -> NON_EXECUTABLE");
    }
    
    @Test
    public void visitASTFunctionNode_returnsNonExecutableState() {
        ASTFunctionNode node = new ASTFunctionNode(ParserTreeConstants.JJTFUNCTIONNODE);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
        assertOutputLine(0, "data{} -> NON_EXECUTABLE");
    }
    
    @Test
    public void visitASTNotNode_givenForFieldIndexIsFalse_returnsNonExecutableState() {
        initVisitor(false, false);
        ASTNotNode node = new ASTNotNode(ParserTreeConstants.JJTNOTNODE);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
    }
    
    @Test
    public void visitASTNotNode_givenNoChildren_returnsPartialState() {
        ASTNotNode node = new ASTNotNode(ParserTreeConstants.JJTNOTNODE);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.PARTIAL, state);
        assertOutputLine(0, "data  NotNode[] -> PARTIAL");
    }
    
    @Test
    public void visitASTNotNode_givenSingleIgnorableState_returnsIgnorableState() {
        JexlNode child = createMockChild(STATE.IGNORABLE);
        replay(child);
        ASTNotNode parent = new ASTNotNode(ParserTreeConstants.JJTNOTNODE);
        addMockChildren(parent, child);
        
        STATE state = (STATE) visitor.visit(parent, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "data  NotNode[IGNORABLE] -> IGNORABLE");
    }
    
    @Test
    public void visitASTNotNode_givenSingleNonExecutableState_returnsNonExecutableState() {
        JexlNode child = createMockChild(STATE.NON_EXECUTABLE);
        replay(child);
        ASTNotNode parent = new ASTNotNode(ParserTreeConstants.JJTNOTNODE);
        addMockChildren(parent, child);
        
        STATE state = (STATE) visitor.visit(parent, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
        assertOutputLine(0, "data  NotNode[NON_EXECUTABLE] -> NON_EXECUTABLE");
    }
    
    @Test
    public void visitASTNotNode_givenNonExecutableAndIgnorable_returnsNonExecutableState() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.NON_EXECUTABLE);
        replay(child1, child2);
        ASTNotNode parent = new ASTNotNode(ParserTreeConstants.JJTNOTNODE);
        addMockChildren(parent, child1, child2);
        
        STATE state = (STATE) visitor.visit(parent, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
        assertOutputLine(0, "data  NotNode[NON_EXECUTABLE, IGNORABLE] -> NON_EXECUTABLE");
    }
    
    @Test
    public void visitASTNotNode_givenExecutableAndIgnorable_returnsExecutableState() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.EXECUTABLE);
        replay(child1, child2);
        ASTNotNode parent = new ASTNotNode(ParserTreeConstants.JJTNOTNODE);
        addMockChildren(parent, child1, child2);
        
        STATE state = (STATE) visitor.visit(parent, "data");
        
        assertEquals(STATE.EXECUTABLE, state);
        assertOutputLine(0, "data  NotNode[EXECUTABLE, IGNORABLE] -> EXECUTABLE");
    }
    
    @Test
    public void visitASTNotNode_givenThreeStatesWhereOneIsIgnorableAndOneIsError_returnsErrorState() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.NON_EXECUTABLE);
        JexlNode child3 = createMockChild(STATE.ERROR);
        replay(child1, child2, child3);
        ASTNotNode parent = new ASTNotNode(ParserTreeConstants.JJTNOTNODE);
        addMockChildren(parent, child1, child2, child3);
        
        STATE state = (STATE) visitor.visit(parent, "data");
        
        assertEquals(STATE.ERROR, state);
        assertOutputLine(0, "data  NotNode[ERROR, NON_EXECUTABLE, IGNORABLE] -> ERROR");
    }
    
    @Test
    public void visitASTNotNode_givenThreeStatesWhereOneIsIgnorableAndOneIsPartial_returnsPartialState() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.NON_EXECUTABLE);
        JexlNode child3 = createMockChild(STATE.PARTIAL);
        replay(child1, child2, child3);
        ASTNotNode parent = new ASTNotNode(ParserTreeConstants.JJTNOTNODE);
        addMockChildren(parent, child1, child2, child3);
        
        STATE state = (STATE) visitor.visit(parent, "data");
        
        assertEquals(STATE.PARTIAL, state);
        assertOutputLine(0, "data  NotNode[PARTIAL, NON_EXECUTABLE, IGNORABLE] -> PARTIAL");
    }
    
    @Test
    public void visitASTJexlScript_givenNoChildren_returnsPartialState() {
        ASTJexlScript node = new ASTJexlScript(ParserTreeConstants.JJTJEXLSCRIPT);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.PARTIAL, state);
        assertOutputLine(0, "data  JexlScript[] -> PARTIAL");
    }
    
    @Test
    public void visitASTJexlScript_givenSingleIgnorableState_returnsIgnorableState() {
        JexlNode child = createMockChild(STATE.IGNORABLE);
        replay(child);
        ASTJexlScript parent = new ASTJexlScript(ParserTreeConstants.JJTJEXLSCRIPT);
        addMockChildren(parent, child);
        
        STATE state = (STATE) visitor.visit(parent, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "data  JexlScript[IGNORABLE] -> IGNORABLE");
    }
    
    @Test
    public void visitASTJexlScript_givenSingleNonExecutableState_returnsNonExecutableState() {
        JexlNode child = createMockChild(STATE.NON_EXECUTABLE);
        replay(child);
        ASTJexlScript parent = new ASTJexlScript(ParserTreeConstants.JJTJEXLSCRIPT);
        addMockChildren(parent, child);
        
        STATE state = (STATE) visitor.visit(parent, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
        assertOutputLine(0, "data  JexlScript[NON_EXECUTABLE] -> NON_EXECUTABLE");
    }
    
    @Test
    public void visitASTJexlScript_givenNonExecutableAndIgnorable_returnsNonExecutableState() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.NON_EXECUTABLE);
        replay(child1, child2);
        ASTJexlScript parent = new ASTJexlScript(ParserTreeConstants.JJTJEXLSCRIPT);
        addMockChildren(parent, child1, child2);
        
        STATE state = (STATE) visitor.visit(parent, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
        assertOutputLine(0, "data  JexlScript[NON_EXECUTABLE, IGNORABLE] -> NON_EXECUTABLE");
    }
    
    @Test
    public void visitASTJexlScript_givenExecutableAndIgnorable_returnsExecutableState() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.EXECUTABLE);
        replay(child1, child2);
        ASTJexlScript parent = new ASTJexlScript(ParserTreeConstants.JJTJEXLSCRIPT);
        addMockChildren(parent, child1, child2);
        
        STATE state = (STATE) visitor.visit(parent, "data");
        
        assertEquals(STATE.EXECUTABLE, state);
        assertOutputLine(0, "data  JexlScript[EXECUTABLE, IGNORABLE] -> EXECUTABLE");
    }
    
    @Test
    public void visitASTJexlScript_givenThreeStatesWhereOneIsIgnorableAndOneIsError_returnsErrorState() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.NON_EXECUTABLE);
        JexlNode child3 = createMockChild(STATE.ERROR);
        replay(child1, child2, child3);
        ASTJexlScript parent = new ASTJexlScript(ParserTreeConstants.JJTJEXLSCRIPT);
        addMockChildren(parent, child1, child2, child3);
        
        STATE state = (STATE) visitor.visit(parent, "data");
        
        assertEquals(STATE.ERROR, state);
        assertOutputLine(0, "data  JexlScript[ERROR, NON_EXECUTABLE, IGNORABLE] -> ERROR");
    }
    
    @Test
    public void visitASTJexlScript_givenThreeStatesWhereOneIsIgnorableAndOneIsPartial_returnsPartialState() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.NON_EXECUTABLE);
        JexlNode child3 = createMockChild(STATE.PARTIAL);
        replay(child1, child2, child3);
        ASTJexlScript parent = new ASTJexlScript(ParserTreeConstants.JJTJEXLSCRIPT);
        addMockChildren(parent, child1, child2, child3);
        
        STATE state = (STATE) visitor.visit(parent, "data");
        
        assertEquals(STATE.PARTIAL, state);
        assertOutputLine(0, "data  JexlScript[PARTIAL, NON_EXECUTABLE, IGNORABLE] -> PARTIAL");
    }
    
    @Test
    public void visitASTReferenceExpression_givenNoChildren_returnsPartial() {
        ASTReferenceExpression node = new ASTReferenceExpression(ParserTreeConstants.JJTREFERENCEEXPRESSION);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.PARTIAL, state);
        assertOutputLine(0, "data  ReferenceExpression[] -> PARTIAL");
    }
    
    @Test
    public void visitASTReferenceExpression_givenSingleIgnorableState_returnsIgnorableState() {
        JexlNode child = createMockChild(STATE.IGNORABLE);
        replay(child);
        ASTReferenceExpression parent = new ASTReferenceExpression(ParserTreeConstants.JJTREFERENCEEXPRESSION);
        addMockChildren(parent, child);
        
        STATE state = (STATE) visitor.visit(parent, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "data  ReferenceExpression[IGNORABLE] -> IGNORABLE");
    }
    
    @Test
    public void visitASTReferenceExpression_givenSingleNonExecutableState_returnsNonExecutableState() {
        JexlNode child = createMockChild(STATE.NON_EXECUTABLE);
        replay(child);
        ASTReferenceExpression parent = new ASTReferenceExpression(ParserTreeConstants.JJTREFERENCEEXPRESSION);
        addMockChildren(parent, child);
        
        STATE state = (STATE) visitor.visit(parent, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
        assertOutputLine(0, "data  ReferenceExpression[NON_EXECUTABLE] -> NON_EXECUTABLE");
    }
    
    @Test
    public void visitASTReferenceExpression_givenNonExecutableAndIgnorable_returnsNonExecutableState() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.NON_EXECUTABLE);
        replay(child1, child2);
        ASTReferenceExpression parent = new ASTReferenceExpression(ParserTreeConstants.JJTREFERENCEEXPRESSION);
        addMockChildren(parent, child1, child2);
        
        STATE state = (STATE) visitor.visit(parent, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
        assertOutputLine(0, "data  ReferenceExpression[NON_EXECUTABLE, IGNORABLE] -> NON_EXECUTABLE");
    }
    
    @Test
    public void visitASTReferenceExpression_givenExecutableAndIgnorable_returnsExecutableState() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.EXECUTABLE);
        replay(child1, child2);
        ASTReferenceExpression parent = new ASTReferenceExpression(ParserTreeConstants.JJTREFERENCEEXPRESSION);
        addMockChildren(parent, child1, child2);
        
        STATE state = (STATE) visitor.visit(parent, "data");
        
        assertEquals(STATE.EXECUTABLE, state);
        assertOutputLine(0, "data  ReferenceExpression[EXECUTABLE, IGNORABLE] -> EXECUTABLE");
    }
    
    @Test
    public void visitASTReferenceExpression_givenThreeStatesWhereOneIsIgnorableAndOneIsError_returnsErrorState() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.NON_EXECUTABLE);
        JexlNode child3 = createMockChild(STATE.ERROR);
        replay(child1, child2, child3);
        ASTReferenceExpression parent = new ASTReferenceExpression(ParserTreeConstants.JJTREFERENCEEXPRESSION);
        addMockChildren(parent, child1, child2, child3);
        
        STATE state = (STATE) visitor.visit(parent, "data");
        
        assertEquals(STATE.ERROR, state);
        assertOutputLine(0, "data  ReferenceExpression[ERROR, NON_EXECUTABLE, IGNORABLE] -> ERROR");
    }
    
    @Test
    public void visitASTReferenceExpression_givenThreeStatesWhereOneIsIgnorableAndOneIsPartial_returnsPartialState() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.NON_EXECUTABLE);
        JexlNode child3 = createMockChild(STATE.PARTIAL);
        replay(child1, child2, child3);
        ASTReferenceExpression parent = new ASTReferenceExpression(ParserTreeConstants.JJTREFERENCEEXPRESSION);
        addMockChildren(parent, child1, child2, child3);
        
        STATE state = (STATE) visitor.visit(parent, "data");
        
        assertEquals(STATE.PARTIAL, state);
        assertOutputLine(0, "data  ReferenceExpression[PARTIAL, NON_EXECUTABLE, IGNORABLE] -> PARTIAL");
    }
    
    @Test
    public void visitASTOrNode_givenNoChildren_returnsPartial() {
        ASTOrNode node = new ASTOrNode(ParserTreeConstants.JJTORNODE);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.PARTIAL, state);
        assertOutputLine(0, "data  OrNode[] -> PARTIAL");
    }
    
    @Test
    public void visitASTOrNode_givenSingleIgnorableState_returnsIgnorableState() {
        JexlNode child = createMockChild(STATE.IGNORABLE);
        replay(child);
        ASTOrNode parent = new ASTOrNode(ParserTreeConstants.JJTORNODE);
        addMockChildren(parent, child);
        
        STATE state = (STATE) visitor.visit(parent, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "data  OrNode[IGNORABLE] -> IGNORABLE");
    }
    
    @Test
    public void visitASTOrNode_givenSingleNonExecutableState_returnsNonExecutableState() {
        JexlNode child = createMockChild(STATE.NON_EXECUTABLE);
        replay(child);
        ASTOrNode parent = new ASTOrNode(ParserTreeConstants.JJTORNODE);
        addMockChildren(parent, child);
        
        STATE state = (STATE) visitor.visit(parent, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
        assertOutputLine(0, "data  OrNode[NON_EXECUTABLE] -> NON_EXECUTABLE");
    }
    
    @Test
    public void visitASTOrNode_givenNonExecutableAndIgnorable_returnsNonExecutableState() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.NON_EXECUTABLE);
        replay(child1, child2);
        ASTOrNode parent = new ASTOrNode(ParserTreeConstants.JJTORNODE);
        addMockChildren(parent, child1, child2);
        
        STATE state = (STATE) visitor.visit(parent, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
        assertOutputLine(0, "data  OrNode[NON_EXECUTABLE, IGNORABLE] -> NON_EXECUTABLE");
    }
    
    @Test
    public void visitASTOrNode_givenExecutableAndIgnorable_returnsExecutableState() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.EXECUTABLE);
        replay(child1, child2);
        ASTOrNode parent = new ASTOrNode(ParserTreeConstants.JJTORNODE);
        addMockChildren(parent, child1, child2);
        
        STATE state = (STATE) visitor.visit(parent, "data");
        
        assertEquals(STATE.EXECUTABLE, state);
        assertOutputLine(0, "data  OrNode[EXECUTABLE, IGNORABLE] -> EXECUTABLE");
    }
    
    @Test
    public void visitASTOrNode_givenThreeStatesWhereOneIsIgnorableAndOneIsError_returnsErrorState() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.NON_EXECUTABLE);
        JexlNode child3 = createMockChild(STATE.ERROR);
        replay(child1, child2, child3);
        ASTOrNode parent = new ASTOrNode(ParserTreeConstants.JJTORNODE);
        addMockChildren(parent, child1, child2, child3);
        
        STATE state = (STATE) visitor.visit(parent, "data");
        
        assertEquals(STATE.ERROR, state);
        assertOutputLine(0, "data  OrNode[ERROR, NON_EXECUTABLE, IGNORABLE] -> ERROR");
    }
    
    @Test
    public void visitASTOrNode_givenThreeStatesWhereOneIsIgnorableAndOneIsPartial_returnsPartialState() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.NON_EXECUTABLE);
        JexlNode child3 = createMockChild(STATE.PARTIAL);
        replay(child1, child2, child3);
        ASTOrNode parent = new ASTOrNode(ParserTreeConstants.JJTORNODE);
        addMockChildren(parent, child1, child2, child3);
        
        STATE state = (STATE) visitor.visit(parent, "data");
        
        assertEquals(STATE.PARTIAL, state);
        assertOutputLine(0, "data  OrNode[PARTIAL, NON_EXECUTABLE, IGNORABLE] -> PARTIAL");
    }
    
    @Test
    public void visitASTAndNode_givenNoChildren_returnsNull() {
        ASTAndNode parent = new ASTAndNode(ParserTreeConstants.JJTANDNODE);
        
        STATE state = (STATE) visitor.visit(parent, "data");
        
        assertNull(state);
    }
    
    @Test
    public void visitASTAndNode_givenSingleIgnorableState_returnsIgnorableState() {
        JexlNode child = createMockChild(STATE.IGNORABLE);
        replay(child);
        ASTAndNode parent = new ASTAndNode(ParserTreeConstants.JJTANDNODE);
        addMockChildren(parent, child);
        
        STATE state = (STATE) visitor.visit(parent, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "data  AndNode[IGNORABLE] -> IGNORABLE");
    }
    
    @Test
    public void visitASTAndNode_givenSingleNonExecutableState_returnsNonExecutableState() {
        JexlNode child = createMockChild(STATE.NON_EXECUTABLE);
        replay(child);
        ASTAndNode parent = new ASTAndNode(ParserTreeConstants.JJTANDNODE);
        addMockChildren(parent, child);
        
        STATE state = (STATE) visitor.visit(parent, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
        assertOutputLine(0, "data  AndNode[NON_EXECUTABLE] -> NON_EXECUTABLE");
    }
    
    @Test
    public void visitASTAndNode_givenNonExecutableAndIgnorable_returnsNonExecutableState() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.NON_EXECUTABLE);
        replay(child1, child2);
        ASTAndNode parent = new ASTAndNode(ParserTreeConstants.JJTANDNODE);
        addMockChildren(parent, child1, child2);
        
        STATE state = (STATE) visitor.visit(parent, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
        assertOutputLine(0, "data  AndNode[NON_EXECUTABLE, IGNORABLE] -> NON_EXECUTABLE");
    }
    
    @Test
    public void visitASTAndNode_givenThreeStatesWhereOneIsIgnorableAndOneIsError_returnsErrorState() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.NON_EXECUTABLE);
        JexlNode child3 = createMockChild(STATE.ERROR);
        replay(child1, child2, child3);
        ASTAndNode parent = new ASTAndNode(ParserTreeConstants.JJTANDNODE);
        addMockChildren(parent, child1, child2, child3);
        
        STATE state = (STATE) visitor.visit(parent, "data");
        
        assertEquals(STATE.ERROR, state);
        assertOutputLine(0, "data  AndNode[ERROR, NON_EXECUTABLE, IGNORABLE] -> ERROR");
    }
    
    @Test
    public void visitASTAndNode_givenThreeStatesWhereOneIsIgnorableAndOneIsPartial_returnsPartialState() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.NON_EXECUTABLE);
        JexlNode child3 = createMockChild(STATE.PARTIAL);
        replay(child1, child2, child3);
        ASTAndNode parent = new ASTAndNode(ParserTreeConstants.JJTANDNODE);
        addMockChildren(parent, child1, child2, child3);
        
        STATE state = (STATE) visitor.visit(parent, "data");
        
        assertEquals(STATE.PARTIAL, state);
        assertOutputLine(0, "data  AndNode[PARTIAL, NON_EXECUTABLE, IGNORABLE] -> PARTIAL");
    }
    
    @Test
    public void visitASTAndNode_givenThreeStatesWhereOneIsIgnorableAndOneIsNonExecutable_returnsExecutableState() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.EXECUTABLE);
        JexlNode child3 = createMockChild(STATE.NON_EXECUTABLE);
        replay(child1, child2, child3);
        ASTAndNode parent = new ASTAndNode(ParserTreeConstants.JJTANDNODE);
        addMockChildren(parent, child1, child2, child3);
        
        STATE state = (STATE) visitor.visit(parent, "data");
        
        assertEquals(STATE.EXECUTABLE, state);
        assertOutputLine(0, "data  AndNode[EXECUTABLE, NON_EXECUTABLE, IGNORABLE] -> EXECUTABLE");
    }
    
    @Test
    public void visitSimpleNode_returnsIgnorableState() {
        SimpleNode node = new SimpleNode(ParserTreeConstants.JJTSTRINGLITERAL);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataStringLiteral -> IGNORABLE");
    }
    
    @Test
    public void visitASTBlock_returnsIgnorableState() {
        ASTBlock node = new ASTBlock(ParserTreeConstants.JJTBLOCK);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataBlock -> IGNORABLE");
    }
    
    @Test
    public void visitASTAmbiguous_returnsIgnorableState() {
        ASTAmbiguous node = new ASTAmbiguous(ParserTreeConstants.JJTAMBIGUOUS);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataAmbiguous -> IGNORABLE");
    }
    
    @Test
    public void visitASTIfStatement_returnsIgnorableState() {
        ASTIfStatement node = new ASTIfStatement(ParserTreeConstants.JJTIFSTATEMENT);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataIfStatement -> IGNORABLE");
    }
    
    @Test
    public void visitASTWhileStatement_returnsIgnorableState() {
        ASTWhileStatement node = new ASTWhileStatement(ParserTreeConstants.JJTWHILESTATEMENT);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataWhileStatement -> IGNORABLE");
    }
    
    @Test
    public void visitASTForeachStatement_returnsIgnorableState() {
        ASTForeachStatement node = new ASTForeachStatement(ParserTreeConstants.JJTFOREACHSTATEMENT);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataForeachStatement -> IGNORABLE");
    }
    
    @Test
    public void visitASTAssignment_returnsIgnorableState() {
        ASTAssignment node = new ASTAssignment(ParserTreeConstants.JJTASSIGNMENT);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataAssignment -> IGNORABLE");
    }
    
    @Test
    public void visitASTTernaryNode_returnsIgnorableState() {
        ASTTernaryNode node = new ASTTernaryNode(ParserTreeConstants.JJTTERNARYNODE);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataTernaryNode -> IGNORABLE");
    }
    
    @Test
    public void visitASTBitwiseOrNode_returnsIgnorableState() {
        ASTBitwiseOrNode node = new ASTBitwiseOrNode(ParserTreeConstants.JJTBITWISEORNODE);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataBitwiseOrNode -> IGNORABLE");
    }
    
    @Test
    public void visitASTBitwiseXorNode_returnsIgnorableState() {
        ASTBitwiseXorNode node = new ASTBitwiseXorNode(ParserTreeConstants.JJTBITWISEXORNODE);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataBitwiseXorNode -> IGNORABLE");
    }
    
    @Test
    public void visitASTBitwiseAndNode_returnsIgnorableState() {
        ASTBitwiseAndNode node = new ASTBitwiseAndNode(ParserTreeConstants.JJTBITWISEANDNODE);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataBitwiseAndNode -> IGNORABLE");
    }
    
    @Test
    public void visitASTAdditiveNode_returnsIgnorableState() {
        ASTAdditiveNode node = new ASTAdditiveNode(ParserTreeConstants.JJTADDITIVENODE);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataAdditiveNode -> IGNORABLE");
    }
    
    @Test
    public void visitASTAdditiveOperator_returnsIgnorableState() {
        ASTAdditiveOperator node = new ASTAdditiveOperator(ParserTreeConstants.JJTADDITIVEOPERATOR);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataAdditiveOperator -> IGNORABLE");
    }
    
    @Test
    public void visitASTMulNode_returnsIgnorableState() {
        ASTMulNode node = new ASTMulNode(ParserTreeConstants.JJTMULNODE);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataMulNode -> IGNORABLE");
    }
    
    @Test
    public void visitASTDivNode_returnsIgnorableState() {
        ASTDivNode node = new ASTDivNode(ParserTreeConstants.JJTDIVNODE);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataDivNode -> IGNORABLE");
    }
    
    @Test
    public void visitASTModNode_returnsIgnorableState() {
        ASTModNode node = new ASTModNode(ParserTreeConstants.JJTMODNODE);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataModNode -> IGNORABLE");
    }
    
    @Test
    public void visitASTUnaryMinusNode_returnsIgnorableState() {
        ASTUnaryMinusNode node = new ASTUnaryMinusNode(ParserTreeConstants.JJTUNARYMINUSNODE);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataUnaryMinusNode -> IGNORABLE");
    }
    
    @Test
    public void visitASTBitwiseComplNodeNode_returnsIgnorableState() {
        ASTBitwiseComplNode node = new ASTBitwiseComplNode(ParserTreeConstants.JJTBITWISECOMPLNODE);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataBitwiseComplNode -> IGNORABLE");
    }
    
    @Test
    public void visitASTIdentifier_returnsIgnorableState() {
        ASTIdentifier node = new ASTIdentifier(ParserTreeConstants.JJTIDENTIFIER);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataIdentifier -> IGNORABLE");
    }
    
    @Test
    public void visitASTNullLiteral_returnsNonExecutableState() {
        ASTNullLiteral node = new ASTNullLiteral(ParserTreeConstants.JJTNULLLITERAL);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
        assertOutputLine(0, "dataNullLiteral -> NON_EXECUTABLE");
    }
    
    @Test
    public void visitASTTrueNode_returnsIgnorableState() {
        ASTTrueNode node = new ASTTrueNode(ParserTreeConstants.JJTTRUENODE);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataTrueNode -> IGNORABLE");
    }
    
    @Test
    public void visitASTFalseNode_returnsIgnorableState() {
        ASTFalseNode node = new ASTFalseNode(ParserTreeConstants.JJTFALSENODE);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataFalseNode -> IGNORABLE");
    }
    
    @Test
    public void visitASTIntegerLiteral_returnsIgnorableState() throws InvocationTargetException, NoSuchMethodException, InstantiationException,
                    IllegalAccessException {
        Class<?>[] paramTypes = {Integer.TYPE};
        Object[] params = {ParserTreeConstants.JJTNUMBERLITERAL};
        ASTIntegerLiteral node = instantiateViaReflection(ASTIntegerLiteral.class, paramTypes, params);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataNumberLiteral -> IGNORABLE");
    }
    
    @Test
    public void visitASTFloatLiteral_returnsIgnorableState() {
        ASTFloatLiteral node = new ASTFloatLiteral(ParserTreeConstants.JJTNUMBERLITERAL);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataNumberLiteral -> IGNORABLE");
    }
    
    @Test
    public void visitASTStringLiteral_returnsIgnorableState() {
        ASTStringLiteral node = new ASTStringLiteral(ParserTreeConstants.JJTSTRINGLITERAL);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataStringLiteral -> IGNORABLE");
    }
    
    @Test
    public void visitASTArrayLiteral_returnsIgnorableState() throws InvocationTargetException, NoSuchMethodException, InstantiationException,
                    IllegalAccessException {
        Class<?>[] paramTypes = {Integer.TYPE};
        Object[] params = {ParserTreeConstants.JJTARRAYLITERAL};
        ASTArrayLiteral node = instantiateViaReflection(ASTArrayLiteral.class, paramTypes, params);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataArrayLiteral -> IGNORABLE");
    }
    
    @Test
    public void visitASTMapLiteral_returnsIgnorableState() throws InvocationTargetException, NoSuchMethodException, InstantiationException,
                    IllegalAccessException {
        Class<?>[] paramTypes = {Integer.TYPE};
        Object[] params = {ParserTreeConstants.JJTMAPLITERAL};
        ASTMapLiteral node = instantiateViaReflection(ASTMapLiteral.class, paramTypes, params);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataMapLiteral -> IGNORABLE");
    }
    
    @Test
    public void visitASTMapEntry_returnsIgnorableState() {
        ASTMapEntry node = new ASTMapEntry(ParserTreeConstants.JJTMAPENTRY);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataMapEntry -> IGNORABLE");
    }
    
    @Test
    public void visitASTEmptyFunction_returnsIgnorableState() {
        ASTEmptyFunction node = new ASTEmptyFunction(ParserTreeConstants.JJTEMPTYFUNCTION);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataEmptyFunction -> IGNORABLE");
    }
    
    @Test
    public void visitASTSizeFunction_returnsIgnorableState() {
        ASTSizeFunction node = new ASTSizeFunction(ParserTreeConstants.JJTSIZEFUNCTION);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataSizeFunction -> IGNORABLE");
    }
    
    @Test
    public void visitASTMethodNode_returnsNonExecutableState() {
        ASTMethodNode node = new ASTMethodNode(ParserTreeConstants.JJTMETHODNODE);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
        assertOutputLine(0, "dataMethodNode -> NON_EXECUTABLE");
    }
    
    @Test
    public void visitASTSizeMethodNode_returnsNonExecutableState() {
        ASTSizeMethod node = new ASTSizeMethod(ParserTreeConstants.JJTSIZEMETHOD);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
        assertOutputLine(0, "dataSizeMethod -> NON_EXECUTABLE");
    }
    
    @Test
    public void visitASTConstructorNode_returnsIgnorableState() {
        ASTConstructorNode node = new ASTConstructorNode(ParserTreeConstants.JJTCONSTRUCTORNODE);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataConstructorNode -> IGNORABLE");
    }
    
    @Test
    public void visitASTArrayAccess_returnsIgnorableState() {
        ASTArrayAccess node = new ASTArrayAccess(ParserTreeConstants.JJTARRAYACCESS);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataArrayAccess -> IGNORABLE");
    }
    
    @Test
    public void visitASTReturnStatement_returnsIgnorableState() {
        ASTReturnStatement node = new ASTReturnStatement(ParserTreeConstants.JJTRETURNSTATEMENT);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataReturnStatement -> IGNORABLE");
    }
    
    @Test
    public void visitASTVar_returnsIgnorableState() {
        ASTVar node = new ASTVar(ParserTreeConstants.JJTVAR);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataVar -> IGNORABLE");
    }
    
    @Test
    public void visitASTNumberLiteral_returnsIgnorableState() {
        ASTNumberLiteral node = new ASTNumberLiteral(ParserTreeConstants.JJTNUMBERLITERAL);
        
        STATE state = (STATE) visitor.visit(node, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataNumberLiteral -> IGNORABLE");
    }
    
    @Test
    public void allOrSome_givenNoChildren_returnsNull() {
        JexlNode parent = createMockParent();
        replay(parent);
        
        STATE state = visitor.allOrSome(parent, "data");
        
        assertNull(state);
    }
    
    @Test
    public void allOrSome_givenSingleIgnorableState_returnsIgnorableState() {
        JexlNode child = createMockChild(STATE.IGNORABLE);
        JexlNode parent = createMockParent(child);
        replay(child, parent);
        
        STATE state = visitor.allOrSome(parent, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataEasyMock for class org.apache.commons.jexl2.parser.JexlNode[IGNORABLE] -> IGNORABLE");
    }
    
    @Test
    public void allOrSome_givenSingleNonExecutableState_returnsNonExecutableState() {
        JexlNode child = createMockChild(STATE.NON_EXECUTABLE);
        JexlNode parent = createMockParent(child);
        replay(child, parent);
        
        STATE state = visitor.allOrSome(parent, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
        assertOutputLine(0, "dataEasyMock for class org.apache.commons.jexl2.parser.JexlNode[NON_EXECUTABLE] -> NON_EXECUTABLE");
    }
    
    @Test
    public void allOrSome_givenNonExecutableAndIgnorable_returnsNonExecutableState() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.NON_EXECUTABLE);
        JexlNode parent = createMockParent(child1, child2);
        replay(child1, child2, parent);
        
        STATE state = visitor.allOrSome(parent, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
        assertOutputLine(0, "dataEasyMock for class org.apache.commons.jexl2.parser.JexlNode[NON_EXECUTABLE, IGNORABLE] -> NON_EXECUTABLE");
    }
    
    @Test
    public void allOrSome_givenThreeStatesWhereOneIsIgnorableAndOneIsError_returnsErrorState() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.NON_EXECUTABLE);
        JexlNode child3 = createMockChild(STATE.ERROR);
        JexlNode parent = createMockParent(child1, child2, child3);
        replay(child1, child2, child3, parent);
        
        STATE state = visitor.allOrSome(parent, "data");
        
        assertEquals(STATE.ERROR, state);
        assertOutputLine(0, "dataEasyMock for class org.apache.commons.jexl2.parser.JexlNode[ERROR, NON_EXECUTABLE, IGNORABLE] -> ERROR");
    }
    
    @Test
    public void allOrSome_givenThreeStatesWhereOneIsIgnorableAndOneIsPartial_returnsPartialState() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.NON_EXECUTABLE);
        JexlNode child3 = createMockChild(STATE.PARTIAL);
        JexlNode parent = createMockParent(child1, child2, child3);
        replay(child1, child2, child3, parent);
        
        STATE state = visitor.allOrSome(parent, "data");
        
        assertEquals(STATE.PARTIAL, state);
        assertOutputLine(0, "dataEasyMock for class org.apache.commons.jexl2.parser.JexlNode[PARTIAL, NON_EXECUTABLE, IGNORABLE] -> PARTIAL");
    }
    
    @Test
    public void allOrSome_givenThreeStatesWhereOneIsIgnorableAndOneIsNonExecutable_returnsExecutableState() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.EXECUTABLE);
        JexlNode child3 = createMockChild(STATE.NON_EXECUTABLE);
        JexlNode parent = createMockParent(child1, child2, child3);
        replay(child1, child2, child3, parent);
        
        STATE state = visitor.allOrSome(parent, "data");
        
        assertEquals(STATE.EXECUTABLE, state);
        assertOutputLine(0, "dataEasyMock for class org.apache.commons.jexl2.parser.JexlNode[EXECUTABLE, NON_EXECUTABLE, IGNORABLE] -> EXECUTABLE");
    }
    
    @Test
    public void unlessAnyNonExecutable_givenNoChildren_returnsExecutable() {
        JexlNode parent = createMockParent();
        replay(parent);
        
        STATE state = visitor.unlessAnyNonExecutable(parent, "data");
        
        assertEquals(STATE.EXECUTABLE, state);
    }
    
    @Test
    public void unlessAnyNonExecutable_givenStatesWhereOneIsNonExecutable_returnsNonExecutable() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.EXECUTABLE);
        JexlNode child3 = createMockChild(STATE.PARTIAL);
        JexlNode child4 = createMockChild(STATE.ERROR);
        JexlNode child5 = createMockChild(STATE.NON_EXECUTABLE);
        JexlNode parent = createMockParent(child1, child2, child3, child4, child5);
        replay(parent, child1, child2, child3, child4, child5);
        
        STATE state = visitor.unlessAnyNonExecutable(parent, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
    }
    
    @Test
    public void unlessAnyNonExecutable_givenStatesWhereNoneAreNonExecutable_returnsNonExecutable() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.EXECUTABLE);
        JexlNode child3 = createMockChild(STATE.PARTIAL);
        JexlNode child4 = createMockChild(STATE.ERROR);
        JexlNode parent = createMockParent(child1, child2, child3, child4);
        replay(parent, child1, child2, child3, child4);
        
        STATE state = visitor.unlessAnyNonExecutable(parent, "data");
        
        assertEquals(STATE.EXECUTABLE, state);
    }
    
    @Test
    public void allOrNone_givenNoChildren_returnsPartial() {
        JexlNode parent = createMockParent();
        replay(parent);
        
        STATE state = visitor.allOrNone(parent, "data");
        
        assertEquals(STATE.PARTIAL, state);
        assertOutputLine(0, "dataEasyMock for class org.apache.commons.jexl2.parser.JexlNode[] -> PARTIAL");
    }
    
    @Test
    public void allOrNone_givenSingleIgnorableState_returnsIgnorableState() {
        JexlNode child = createMockChild(STATE.IGNORABLE);
        JexlNode parent = createMockParent(child);
        replay(child, parent);
        
        STATE state = visitor.allOrNone(parent, "data");
        
        assertEquals(STATE.IGNORABLE, state);
        assertOutputLine(0, "dataEasyMock for class org.apache.commons.jexl2.parser.JexlNode[IGNORABLE] -> IGNORABLE");
    }
    
    @Test
    public void allOrNone_givenSingleNonExecutableState_returnsNonExecutableState() {
        JexlNode child = createMockChild(STATE.NON_EXECUTABLE);
        JexlNode parent = createMockParent(child);
        replay(child, parent);
        
        STATE state = visitor.allOrNone(parent, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
        assertOutputLine(0, "dataEasyMock for class org.apache.commons.jexl2.parser.JexlNode[NON_EXECUTABLE] -> NON_EXECUTABLE");
    }
    
    @Test
    public void allOrNone_givenNonExecutableAndIgnorable_returnsNonExecutableState() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.NON_EXECUTABLE);
        JexlNode parent = createMockParent(child1, child2);
        replay(child1, child2, parent);
        
        STATE state = visitor.allOrNone(parent, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
        assertOutputLine(0, "dataEasyMock for class org.apache.commons.jexl2.parser.JexlNode[NON_EXECUTABLE, IGNORABLE] -> NON_EXECUTABLE");
    }
    
    @Test
    public void allOrNone_givenExecutableAndIgnorable_returnsExecutableState() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.EXECUTABLE);
        JexlNode parent = createMockParent(child1, child2);
        replay(child1, child2, parent);
        
        STATE state = visitor.allOrNone(parent, "data");
        
        assertEquals(STATE.EXECUTABLE, state);
        assertOutputLine(0, "dataEasyMock for class org.apache.commons.jexl2.parser.JexlNode[EXECUTABLE, IGNORABLE] -> EXECUTABLE");
    }
    
    @Test
    public void allOrNone_givenThreeStatesWhereOneIsIgnorableAndOneIsError_returnsErrorState() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.NON_EXECUTABLE);
        JexlNode child3 = createMockChild(STATE.ERROR);
        JexlNode parent = createMockParent(child1, child2, child3);
        replay(child1, child2, child3, parent);
        
        STATE state = visitor.allOrNone(parent, "data");
        
        assertEquals(STATE.ERROR, state);
        assertOutputLine(0, "dataEasyMock for class org.apache.commons.jexl2.parser.JexlNode[ERROR, NON_EXECUTABLE, IGNORABLE] -> ERROR");
    }
    
    @Test
    public void allOrNone_givenThreeStatesWhereOneIsIgnorableAndOneIsPartial_returnsPartialState() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.NON_EXECUTABLE);
        JexlNode child3 = createMockChild(STATE.PARTIAL);
        JexlNode parent = createMockParent(child1, child2, child3);
        replay(child1, child2, child3, parent);
        
        STATE state = visitor.allOrNone(parent, "data");
        
        assertEquals(STATE.PARTIAL, state);
        assertOutputLine(0, "dataEasyMock for class org.apache.commons.jexl2.parser.JexlNode[PARTIAL, NON_EXECUTABLE, IGNORABLE] -> PARTIAL");
    }
    
    @Test
    public void isNoFieldOnly_givenNoIdentifier_returnsFalse() {
        ASTStringLiteral node = new ASTStringLiteral(ParserTreeConstants.JJTSTRINGLITERAL);
        assertFalse(ExecutableDeterminationVisitor.isNoFieldOnly(node));
    }
    
    @Test
    public void isNoFieldOnly_givenNonNoFieldIdentifier_returnsFalse() {
        ASTFunctionNode node = new ASTFunctionNode(ParserTreeConstants.JJTFUNCTIONNODE);
        addIdentifier(node, Constants.ANY_FIELD);
        assertFalse(ExecutableDeterminationVisitor.isNoFieldOnly(node));
    }
    
    @Test
    public void isNoFieldOnly_givenNoFieldIdentifier_returnsTrue() {
        ASTFunctionNode node = new ASTFunctionNode(ParserTreeConstants.JJTFUNCTIONNODE);
        addIdentifier(node, Constants.NO_FIELD);
        assertTrue(ExecutableDeterminationVisitor.isNoFieldOnly(node));
    }
    
    @Test
    public void executableUnlessItIsnt_givenNoChildren_returnsExecutableState() {
        JexlNode parent = createMockParent();
        replay(parent);
        
        STATE state = visitor.executableUnlessItIsnt(parent, "data");
        
        assertEquals(STATE.EXECUTABLE, state);
        assertOutputLine(0, "dataEasyMock for class org.apache.commons.jexl2.parser.JexlNode[] -> EXECUTABLE");
    }
    
    @Test
    public void executableUnlessItIsnt_givenSingleIgnorableState_returnsExecutableState() {
        JexlNode child = createMockChild(STATE.IGNORABLE);
        JexlNode parent = createMockParent(child);
        replay(child, parent);
        
        STATE state = visitor.executableUnlessItIsnt(parent, "data");
        
        assertEquals(STATE.EXECUTABLE, state);
        assertOutputLine(0, "dataEasyMock for class org.apache.commons.jexl2.parser.JexlNode[IGNORABLE] -> EXECUTABLE");
    }
    
    @Test
    public void executableUnlessItIsnt_givenSingleNonExecutableState_returnsNonExecutableState() {
        JexlNode child = createMockChild(STATE.NON_EXECUTABLE);
        JexlNode parent = createMockParent(child);
        replay(child, parent);
        
        STATE state = visitor.executableUnlessItIsnt(parent, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
        assertOutputLine(0, "dataEasyMock for class org.apache.commons.jexl2.parser.JexlNode[NON_EXECUTABLE] -> NON_EXECUTABLE");
    }
    
    @Test
    public void executableUnlessItIsnt_givenNonExecutableAndIgnorable_returnsNonExecutableState() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.NON_EXECUTABLE);
        JexlNode parent = createMockParent(child1, child2);
        replay(child1, child2, parent);
        
        STATE state = visitor.executableUnlessItIsnt(parent, "data");
        
        assertEquals(STATE.NON_EXECUTABLE, state);
        assertOutputLine(0, "dataEasyMock for class org.apache.commons.jexl2.parser.JexlNode[NON_EXECUTABLE, IGNORABLE] -> NON_EXECUTABLE");
    }
    
    @Test
    public void executableUnlessItIsnt_givenThreeStatesWhereOneIsIgnorableAndOneIsError_returnsErrorState() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.NON_EXECUTABLE);
        JexlNode child3 = createMockChild(STATE.ERROR);
        JexlNode parent = createMockParent(child1, child2, child3);
        replay(child1, child2, child3, parent);
        
        STATE state = visitor.executableUnlessItIsnt(parent, "data");
        
        assertEquals(STATE.ERROR, state);
        assertOutputLine(0, "dataEasyMock for class org.apache.commons.jexl2.parser.JexlNode[ERROR, NON_EXECUTABLE, IGNORABLE] -> ERROR");
    }
    
    @Test
    public void executableUnlessItIsnt_givenThreeStatesWhereOneIsIgnorableAndOneIsPartial_returnsPartialState() {
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.EXECUTABLE);
        JexlNode child3 = createMockChild(STATE.PARTIAL);
        JexlNode parent = createMockParent(child1, child2, child3);
        replay(child1, child2, child3, parent);
        
        STATE state = visitor.executableUnlessItIsnt(parent, "data");
        
        assertEquals(STATE.PARTIAL, state);
        assertOutputLine(0, "dataEasyMock for class org.apache.commons.jexl2.parser.JexlNode[PARTIAL, EXECUTABLE, IGNORABLE] -> PARTIAL");
    }
    
    @Test
    public void givenDebugStatesIsTrue_thenDebugsAllIndividualRetrievedStates() {
        initVisitor(true, true);
        
        JexlNode child1 = createMockChild(STATE.IGNORABLE);
        JexlNode child2 = createMockChild(STATE.EXECUTABLE);
        JexlNode child3 = createMockChild(STATE.PARTIAL);
        JexlNode child4 = createMockChild(STATE.IGNORABLE);
        JexlNode child5 = createMockChild(STATE.EXECUTABLE);
        JexlNode child6 = createMockChild(STATE.PARTIAL);
        JexlNode parent = createMockParent(child1, child2, child3, child4, child5, child6);
        replay(child1, child2, child3, child4, child5, child6, parent);
        
        STATE state = visitor.executableUnlessItIsnt(parent, "data");
        
        assertEquals(STATE.PARTIAL, state);
        assertOutputLine(0, "  dataEasyMock for class org.apache.commons.jexl2.parser.JexlNode -> IGNORABLE");
        assertOutputLine(1, "  dataEasyMock for class org.apache.commons.jexl2.parser.JexlNode -> EXECUTABLE");
        assertOutputLine(2, "  dataEasyMock for class org.apache.commons.jexl2.parser.JexlNode -> PARTIAL");
        assertOutputLine(3, "  dataEasyMock for class org.apache.commons.jexl2.parser.JexlNode -> IGNORABLE");
        assertOutputLine(4, "  dataEasyMock for class org.apache.commons.jexl2.parser.JexlNode -> EXECUTABLE");
        assertOutputLine(5, "  dataEasyMock for class org.apache.commons.jexl2.parser.JexlNode -> PARTIAL");
        assertOutputLine(6, "dataEasyMock for class org.apache.commons.jexl2.parser.JexlNode[PARTIAL, EXECUTABLE, IGNORABLE] -> PARTIAL");
    }
    
    private void assertOutputLine(final int index, final String message) {
        assertTrue("expected output to contain line " + index, output.size() > index);
        assertEquals("expected output for line " + index, message, output.get(index));
    }
    
    private void addIdentifier(final JexlNode node, final String fieldName) {
        ASTIdentifier identifier = JexlNodeFactory.buildIdentifier(fieldName);
        ASTStringLiteral literal = buildLiteral();
        int offset = node.jjtGetNumChildren();
        addReferencedChild(node, identifier, offset);
        offset++;
        addReferencedChild(node, literal, offset);
    }
    
    private ASTStringLiteral buildLiteral() {
        ASTStringLiteral literal = new ASTStringLiteral(ParserTreeConstants.JJTSTRINGLITERAL);
        literal.image = "data";
        return literal;
    }
    
    private void addReferencedChild(final JexlNode parent, final JexlNode child, int offset) {
        ASTReference reference = new ASTReference(ParserTreeConstants.JJTREFERENCE);
        addChild(reference, child, 0);
        addChild(parent, reference, offset);
    }
    
    private void addChild(final JexlNode parent, final JexlNode child, int offset) {
        parent.jjtAddChild(child, offset);
        child.jjtSetParent(parent);
    }
    
    private <T> T instantiateViaReflection(final Class<T> clazz, final Class<?>[] paramTypes, final Object[] params) throws NoSuchMethodException,
                    IllegalAccessException, InvocationTargetException, InstantiationException {
        Constructor<T> constructor = clazz.getDeclaredConstructor(paramTypes);
        constructor.setAccessible(true);
        return constructor.newInstance(params);
    }
    
    private JexlNode createMockParent(final JexlNode... children) {
        JexlNode node = mock(JexlNode.class);
        expect(node.jjtGetNumChildren()).andReturn(children.length).anyTimes();
        for (int i = 0; i < children.length; i++) {
            expect(node.jjtGetChild(i)).andReturn(children[i]).anyTimes();
        }
        return node;
    }
    
    private JexlNode createMockChild(final STATE state) {
        JexlNode node = mock(JexlNode.class);
        expect(node.jjtAccept(eq(visitor), anyObject())).andReturn(state).anyTimes();
        return node;
    }
    
    private void addMockChildren(final JexlNode parent, final JexlNode... children) {
        for (int i = 0; i < children.length; i++) {
            parent.jjtAddChild(children[i], i);
        }
    }
}
