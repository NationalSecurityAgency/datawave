package datawave.query.jexl.visitors;

import datawave.query.Constants;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.LiteralRange;
import datawave.query.jexl.nodes.ExceededOrThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededTermThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededValueThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.IndexHoleMarkerJexlNode;
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
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTDivNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTEmptyFunction;
import org.apache.commons.jexl2.parser.ASTEvaluationOnly;
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
import org.apache.commons.jexl2.parser.SimpleNode;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Determine if a node can be processed against the global index and/or the field index
 * <p>
 * A node can be processed against the field index if it is not a ER/NR/LE/LT/GE/GT node (unless surrounded by an ivarator expression).
 * <p>
 * A node can be processed against the global index if it is not a ER,NR,LE,LT,GE,GT,NOT, or NE (unless surrounded by an ivarator expression).
 * <p>
 * In general an OR can be processed if it is completely composed of expressions that can be processed. An AND can be processed it at least one of its children
 * can be processed.
 */
public class ExecutableDeterminationVisitor extends BaseVisitor {
    
    /**
     * Represents the executability status of an expression against the index.
     */
    public enum STATE {
        
        /**
         * The expression is executable against the index.
         */
        EXECUTABLE(2, 3, 3),
        
        /**
         * The expression has an OR or cannot be completely satisfied by the index.
         */
        PARTIAL(1, 1, 2),
        
        /**
         * The expression cannot be executed against the index.
         */
        NON_EXECUTABLE(3, 2, 1),
        
        /**
         * The expression does not matter for determining executability against the global index.
         */
        IGNORABLE(4, 4, 4),
        
        /**
         * The expression is index-only, but cannot be run against the index (negation, delayed prefix).
         */
        ERROR(0, 0, 0);
        
        /**
         * The number of unique {@link STATE} values.
         */
        private static final int size = STATE.values().length;
        
        /**
         * The priority order of the {@link STATE} values for {@link #allOrSome(JexlNode, Object)}.
         */
        private final int allOrSomePriority;
        
        /**
         * The priority order of the {@link STATE values for {@link #allOrNone(JexlNode, Object)}.
         */
        private final int allOrNonePriority;
        
        /**
         * The priority order of the {@link STATE values for {@link #executableUnlessItIsnt(JexlNode, Object)}.
         */
        private final int executableUnlessItIsntPriority;
        
        STATE(final int allOrSomePriority, final int allOrNonePriority, final int executableUnlessItIsntPriority) {
            this.allOrSomePriority = allOrSomePriority;
            this.allOrNonePriority = allOrNonePriority;
            this.executableUnlessItIsntPriority = executableUnlessItIsntPriority;
        }
        
        int getAllOrSomePriority() {
            return allOrSomePriority;
        }
        
        int getAllOrNonePriority() {
            return allOrNonePriority;
        }
        
        int getExecutableUnlessItIsntPriority() {
            return executableUnlessItIsntPriority;
        }
    }
    
    private static class StringListOutput {
        
        private final List<String> outputLines;
        
        public StringListOutput(List<String> debugOutput) {
            this.outputLines = debugOutput;
        }
        
        public void writeLine(String line) {
            outputLines.add(line);
        }
    }
    
    private static final Logger log = Logger.getLogger(ExecutableDeterminationVisitor.class);
    private static final Comparator<STATE> allOrSomeComparator = Comparator.comparing(STATE::getAllOrSomePriority);
    private static final Comparator<STATE> allOrNoneComparator = Comparator.comparing(STATE::getAllOrNonePriority);
    private static final Comparator<STATE> executableUnlessItIsntComparator = Comparator.comparing(STATE::getExecutableUnlessItIsntPriority);
    private static final String PREFIX = "  ";
    
    protected MetadataHelper helper;
    protected boolean forFieldIndex;
    protected Set<String> indexedFields = null;
    protected Set<String> indexOnlyFields = null;
    protected Set<String> nonEventFields = null;
    protected ShardQueryConfiguration config;
    
    private StringListOutput output = null;
    
    public ExecutableDeterminationVisitor(ShardQueryConfiguration conf, MetadataHelper metadata, boolean forFieldIndex) {
        this(conf, metadata, forFieldIndex, null);
    }
    
    public ExecutableDeterminationVisitor(ShardQueryConfiguration conf, MetadataHelper metadata, boolean forFieldIndex, List<String> debugOutput) {
        this.helper = metadata;
        this.config = conf;
        this.forFieldIndex = forFieldIndex;
        if (debugOutput != null) {
            this.output = new StringListOutput(debugOutput);
        }
    }
    
    public static STATE getState(JexlNode node, ShardQueryConfiguration config, MetadataHelper helper) {
        return getState(node, config, helper, false);
    }
    
    public static STATE getState(JexlNode node, ShardQueryConfiguration config, MetadataHelper helper, boolean forFieldIndex) {
        return getState(node, config, helper, forFieldIndex, null);
    }
    
    public static STATE getState(JexlNode node, ShardQueryConfiguration config, MetadataHelper helper, List<String> debugOutput) {
        return getState(node, config, helper, false, debugOutput);
    }
    
    public static STATE getState(JexlNode node, ShardQueryConfiguration config, MetadataHelper helper, boolean forFieldIndex, List<String> debugOutput) {
        ExecutableDeterminationVisitor visitor = new ExecutableDeterminationVisitor(config, helper, forFieldIndex, debugOutput);
        return (STATE) node.jjtAccept(visitor, "");
    }
    
    public static STATE getState(JexlNode node, ShardQueryConfiguration config, Set<String> indexedFields, Set<String> indexOnlyFields,
                    Set<String> nonEventFields, boolean forFieldIndex, List<String> debugOutput, MetadataHelper metadataHelper) {
        ExecutableDeterminationVisitor visitor = new ExecutableDeterminationVisitor(config, metadataHelper, forFieldIndex, debugOutput)
                        .setNonEventFields(nonEventFields).setIndexOnlyFields(indexOnlyFields).setIndexedFields(indexedFields);
        return (STATE) node.jjtAccept(visitor, "");
    }
    
    public static boolean isExecutable(JexlNode node, ShardQueryConfiguration config, MetadataHelper helper) {
        return isExecutable(node, config, helper, false);
    }
    
    public static boolean isExecutable(JexlNode node, ShardQueryConfiguration config, MetadataHelper helper, boolean forFieldIndex) {
        return isExecutable(node, config, helper, forFieldIndex, null);
    }
    
    public static boolean isExecutable(JexlNode node, ShardQueryConfiguration config, MetadataHelper helper, List<String> debugOutput) {
        return isExecutable(node, config, helper, false, debugOutput);
    }
    
    public static boolean isExecutable(JexlNode node, ShardQueryConfiguration config, Set<String> indexedFields, Set<String> indexOnlyFields,
                    Set<String> nonEventFields, List<String> debugOutput, MetadataHelper metadataHelper) {
        return isExecutable(node, config, indexedFields, indexOnlyFields, nonEventFields, false, debugOutput, metadataHelper);
    }
    
    public static boolean isExecutable(JexlNode node, ShardQueryConfiguration config, MetadataHelper helper, boolean forFieldIndex, List<String> debugOutput) {
        return getState(node, config, helper, forFieldIndex, debugOutput) == STATE.EXECUTABLE;
    }
    
    public static boolean isExecutable(JexlNode node, ShardQueryConfiguration config, Set<String> indexedFields, Set<String> indexOnlyFields,
                    Set<String> nonEventFields, boolean forFieldIndex, List<String> debugOutput, MetadataHelper metadataHelper) {
        return getState(node, config, indexedFields, indexOnlyFields, nonEventFields, forFieldIndex, debugOutput, metadataHelper) == STATE.EXECUTABLE;
    }
    
    /**
     * Returns whether or not if the specified node has a single identifier equal to {@value Constants#NO_FIELD}.
     *
     * @param node
     *            the node
     * @return true if the node only has the identifier {@value Constants#NO_FIELD}, or false otherwise.
     */
    public static boolean isNoFieldOnly(JexlNode node) {
        try {
            return Constants.NO_FIELD.equals(JexlASTHelper.getIdentifier(node));
        } catch (NoSuchElementException e) {
            // Thrown when the node does not have exactly one identifier.
        }
        return false;
    }
    
    public ExecutableDeterminationVisitor setNonEventFields(Set<String> nonEventFields) {
        this.nonEventFields = nonEventFields;
        return this;
    }
    
    public ExecutableDeterminationVisitor setIndexOnlyFields(Set<String> indexOnlyFields) {
        this.indexOnlyFields = indexOnlyFields;
        return this;
    }
    
    public ExecutableDeterminationVisitor setIndexedFields(Set<String> indexedFields) {
        this.indexedFields = indexedFields;
        return this;
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        STATE state;
        if (isNoFieldOnly(node)) {
            state = STATE.IGNORABLE;
        } else if (isUnOrNoFielded(node)) {
            state = STATE.EXECUTABLE;
        } else if (isUnindexed(node)) {
            state = STATE.NON_EXECUTABLE;
        } else {
            state = unlessAnyNonExecutable(node, data + PREFIX);
            // the only non-executable case here would be with a null literal, which only cannot be computed if index only
            if (state == STATE.NON_EXECUTABLE && isIndexOnly(node)) {
                state = STATE.ERROR;
            }
        }
        
        writeIdentifiersToOutput(node, data, state);
        return state;
    }
    
    @Override
    public Object visit(ASTNENode node, Object data) {
        STATE state;
        if (isNoFieldOnly(node)) {
            state = STATE.IGNORABLE;
        } else if (isUnOrNoFielded(node)) {
            state = STATE.NON_EXECUTABLE;
        } else if (isUnindexed(node)) {
            state = STATE.NON_EXECUTABLE;
        } else if (forFieldIndex) {
            state = STATE.EXECUTABLE;
        } else {
            state = STATE.NON_EXECUTABLE;
        }
        
        writeIdentifierToOutput(node, data, state);
        return state;
    }
    
    @Override
    public Object visit(ASTReference node, Object data) {
        STATE state;
        // until we implement an ivarator that can handle an ExceededTermThreshold node, and ensure that the JexlContext gets
        // _ANYFIELD_ values, then we cannot execute these nodes
        if (ExceededTermThresholdMarkerJexlNode.instanceOf(node)) {
            state = STATE.NON_EXECUTABLE;
        }
        // if an ivarator the return true, else check out children
        else if (ExceededValueThresholdMarkerJexlNode.instanceOf(node) || ExceededOrThresholdMarkerJexlNode.instanceOf(node)) {
            state = STATE.EXECUTABLE;
        }
        // if a delayed predicate, then this is not-executable against the index by choice
        else if (ASTDelayedPredicate.instanceOf(node) || ASTEvaluationOnly.instanceOf(node)) {
            if (isNoFieldOnly(node)) {
                state = STATE.IGNORABLE;
            } else if (isNonEvent(node)) {
                state = STATE.ERROR;
            } else {
                state = STATE.NON_EXECUTABLE;
            }
        } else if (IndexHoleMarkerJexlNode.instanceOf(node)) {
            state = STATE.NON_EXECUTABLE;
        } else {
            state = allOrNone(node, data + PREFIX);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        STATE state;
        // if we got here, then we were not wrapped in an ivarator, or in a delayed predicate. So we know it returns 0 results unless unindexed.
        if (isNoFieldOnly(node)) {
            state = STATE.IGNORABLE;
        } else if (isUnOrNoFielded(node)) {
            state = STATE.EXECUTABLE;
        } else if (isUnindexed(node)) {
            state = STATE.NON_EXECUTABLE;
        } else {
            state = STATE.EXECUTABLE;
        }
        
        writeIdentifierToOutput(node, data, state);
        return state;
    }
    
    @Override
    public Object visit(ASTNRNode node, Object data) {
        STATE state;
        // negated regex nodes are in general not-executable against the index
        if (isNoFieldOnly(node)) {
            state = STATE.IGNORABLE;
        } else if (isNonEvent(node)) {
            state = STATE.ERROR;
        } else {
            state = STATE.NON_EXECUTABLE;
        }
        
        writeIdentifierToOutput(node, data, state);
        return state;
    }
    
    @Override
    public Object visit(ASTGENode node, Object data) {
        return visitLtGtNode(node, data);
    }
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        return visitLtGtNode(node, data);
    }
    
    @Override
    public Object visit(ASTLENode node, Object data) {
        return visitLtGtNode(node, data);
    }
    
    @Override
    public Object visit(ASTLTNode node, Object data) {
        return visitLtGtNode(node, data);
    }
    
    /**
     * Returns {@link STATE#NON_EXECUTABLE}. Function nodes are in general non-executable against the index.
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        STATE state = STATE.NON_EXECUTABLE;
        writeOutput(data + JexlASTHelper.getFunctions(node).toString() + " -> " + state);
        return state;
    }
    
    /**
     * If the this visitor is for a field index, returns {@link STATE#EXECUTABLE} if and only if all the children of the specified node are executable. If not
     * for a field index, returns {@link STATE#NON_EXECUTABLE}.
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTNotNode node, Object data) {
        return forFieldIndex ? allOrNone(node, data + PREFIX) : STATE.NON_EXECUTABLE;
    }
    
    /**
     * Returns {@link STATE#EXECUTABLE} if and only if all the children of the specified node are executable.
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTJexlScript node, Object data) {
        return allOrNone(node, data + PREFIX);
    }
    
    /**
     * Returns {@link STATE#EXECUTABLE} if and only if all the children of the specified node are executable.
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        return allOrNone(node, data + PREFIX);
    }
    
    /**
     * Returns {@link STATE#EXECUTABLE} if and only if all the children of the specified node are executable.
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTOrNode node, Object data) {
        return allOrNone(node, data + PREFIX);
    }
    
    /**
     * Returns {@link STATE#EXECUTABLE} if and only if at least one child of the specified node is executable, and none of the other children are partially
     * executable (e.g. a child has a {@link STATE#ERROR} or {@link STATE#PARTIAL} state).
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTAndNode node, Object data) {
        // at least one child must be executable for an AND expression to be executable, and none of the other nodes should be partially executable
        // all children must be executable for an OR expression to be executable
        return allOrSome(node, data + PREFIX);
    }
    
    /**
     * Returns {@link STATE#IGNORABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(SimpleNode node, Object data) {
        STATE state = STATE.IGNORABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#IGNORABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTBlock node, Object data) {
        STATE state = STATE.IGNORABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#IGNORABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTAmbiguous node, Object data) {
        STATE state = STATE.IGNORABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#IGNORABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTIfStatement node, Object data) {
        STATE state = STATE.IGNORABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#IGNORABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTWhileStatement node, Object data) {
        STATE state = STATE.IGNORABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#IGNORABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTForeachStatement node, Object data) {
        STATE state = STATE.IGNORABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#IGNORABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTAssignment node, Object data) {
        STATE state = STATE.IGNORABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#IGNORABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTTernaryNode node, Object data) {
        STATE state = STATE.IGNORABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#IGNORABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTBitwiseOrNode node, Object data) {
        STATE state = STATE.IGNORABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#IGNORABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTBitwiseXorNode node, Object data) {
        STATE state = STATE.IGNORABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#IGNORABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTBitwiseAndNode node, Object data) {
        STATE state = STATE.IGNORABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#IGNORABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTAdditiveNode node, Object data) {
        STATE state = STATE.IGNORABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#IGNORABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTAdditiveOperator node, Object data) {
        STATE state = STATE.IGNORABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#IGNORABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTMulNode node, Object data) {
        STATE state = STATE.IGNORABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#IGNORABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTDivNode node, Object data) {
        STATE state = STATE.IGNORABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#IGNORABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTModNode node, Object data) {
        STATE state = STATE.IGNORABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#IGNORABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTUnaryMinusNode node, Object data) {
        STATE state = STATE.IGNORABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#IGNORABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTBitwiseComplNode node, Object data) {
        STATE state = STATE.IGNORABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#IGNORABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTIdentifier node, Object data) {
        STATE state = STATE.IGNORABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#NON_EXECUTABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTNullLiteral node, Object data) {
        STATE state = STATE.NON_EXECUTABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#IGNORABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTTrueNode node, Object data) {
        STATE state = STATE.IGNORABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#IGNORABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTFalseNode node, Object data) {
        STATE state = STATE.IGNORABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#IGNORABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    // TODO - mark deprecated
    @Override
    public Object visit(ASTIntegerLiteral node, Object data) {
        STATE state = STATE.IGNORABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#IGNORABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    // TODO - mark deprecated
    @Override
    public Object visit(ASTFloatLiteral node, Object data) {
        STATE state = STATE.IGNORABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#IGNORABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTStringLiteral node, Object data) {
        STATE state = STATE.IGNORABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#IGNORABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTArrayLiteral node, Object data) {
        STATE state = STATE.IGNORABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#IGNORABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTMapLiteral node, Object data) {
        STATE state = STATE.IGNORABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#IGNORABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTMapEntry node, Object data) {
        STATE state = STATE.IGNORABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#IGNORABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTEmptyFunction node, Object data) {
        STATE state = STATE.IGNORABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#IGNORABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTSizeFunction node, Object data) {
        STATE state = STATE.IGNORABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#NON_EXECUTABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTMethodNode node, Object data) {
        STATE state = STATE.NON_EXECUTABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#NON_EXECUTABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTSizeMethod node, Object data) {
        STATE state = STATE.NON_EXECUTABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#IGNORABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTConstructorNode node, Object data) {
        STATE state = STATE.IGNORABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#IGNORABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTArrayAccess node, Object data) {
        STATE state = STATE.IGNORABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#IGNORABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTReturnStatement node, Object data) {
        STATE state = STATE.IGNORABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#IGNORABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTVar node, Object data) {
        STATE state = STATE.IGNORABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#IGNORABLE}
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    @Override
    public Object visit(ASTNumberLiteral node, Object data) {
        STATE state = STATE.IGNORABLE;
        writeNodeToOutput(node, data, state);
        return state;
    }
    
    /**
     * All children must be must be executable for the specified node to be executable. Used for expressions, scripts, and or nodes. Returns a state based on
     * the states retrieved for each child of the specified node based on the following cases, in priority order:
     * <ol>
     * <li>
     * If no states are found, {@link STATE#PARTIAL} is returned.</li>
     * <li>
     * If one unique state is found, that state is returned.</li>
     * <li>
     * If two unique states are found, and one is {@link STATE#IGNORABLE}, the other state is returned.</li>
     * <li>
     * If three or more unique states are found and one is {@link STATE#ERROR}, then {@link STATE#ERROR} is returned.</li>
     * <li>
     * If three or more unique states are found and one is {@link STATE#PARTIAL}, then {@link STATE#PARTIAL} is returned.</li>
     * <li>
     * If only {@link STATE#EXECUTABLE}, {@link STATE#NON_EXECUTABLE}, and {@link STATE#IGNORABLE} are found, {@link STATE#PARTIAL} is returned.</li>
     * </ol>
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    protected STATE allOrNone(JexlNode node, Object data) {
        // Find the states of each child, sorting them in all-or-none priority.
        final SortedSet<STATE> states = new TreeSet<>(allOrNoneComparator);
        readStatesFromChildren(node, data + PREFIX, states);
        
        if (log.isTraceEnabled()) {
            log.trace("node:" + PrintingVisitor.formattedQueryString(node));
            log.trace("states are:" + states);
        }
        
        // If no states were found, or both EXECUTABLE and NON_EXECUTABLE were found, return PARTIAL.
        final STATE state;
        if (states.size() == 0 || (states.size() > 2 && states.first() == STATE.EXECUTABLE)) {
            state = STATE.PARTIAL;
        } else {
            // Otherwise return the first priority state.
            state = states.first();
        }
        
        writeOutput(data + node.toString() + states + " -> " + state);
        return state;
    }
    
    /**
     * Returns {@link STATE#NON_EXECUTABLE} if any of the children of the specified node results in a {@link STATE#NON_EXECUTABLE} state. Otherwise returns
     * {@link STATE#EXECUTABLE}.
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    protected STATE unlessAnyNonExecutable(JexlNode node, Object data) {
        final Set<STATE> states = new HashSet<>();
        readStatesFromChildren(node, data, states);
        
        if (log.isTraceEnabled()) {
            log.trace("node:" + PrintingVisitor.formattedQueryString(node));
            log.trace("states are:" + states);
        }
        
        return states.contains(STATE.NON_EXECUTABLE) ? STATE.NON_EXECUTABLE : STATE.EXECUTABLE;
    }
    
    /**
     * At least one child must be executable for an AND expression to be executable, and none of the other nodes should be partially executable. Returns a state
     * based on the states retrieved for each child of the specified node based on the following cases, in priority order:
     * <ol>
     * <li>
     * If no states are found, null is returned.</li>
     * <li>
     * If one unique state is found, that state is returned.</li>
     * <li>
     * If two unique states are found, and one is {@link STATE#IGNORABLE}, the other state is returned.</li>
     * <li>
     * If three or more unique states are found and one is {@link STATE#ERROR}, then {@link STATE#ERROR} is returned.</li>
     * <li>
     * If three or more unique states are found and one is {@link STATE#PARTIAL}, then {@link STATE#PARTIAL} is returned.</li>
     * <li>
     * If three or more unique states are found non are {@link STATE#ERROR} or {@link STATE#PARTIAL}, then {@link STATE#EXECUTABLE} is returned.</li>
     * </ol>
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    protected STATE allOrSome(JexlNode node, Object data) {
        if (node.jjtGetNumChildren() == 0) {
            return null;
        }
        
        // Find the states of each child, sorting them in all-or-some priority.
        final SortedSet<STATE> states = new TreeSet<>(allOrSomeComparator);
        readStatesFromChildren(node, data + PREFIX, states);
        
        // The correct state will always be the first priority state.
        final STATE state = states.first();
        writeStatesToOutput(node, data, states, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#EXECUTABLE} only if all children are executable or ignorable. Introduced to catch NULL literals in a disjunction. The state will be
     * determined based on the states retrieved for each child of the specified node based on the following cases, in priority order:
     * <ol>
     * <li>
     * If no states are found, {@link STATE#EXECUTABLE} is returned.</li>
     * <li>
     * If one unique state is found, that state is returned.</li>
     * <li>
     * If two unique states are found, and one is {@link STATE#IGNORABLE}, the other state is returned.</li>
     * <li>
     * If three or more unique states are found and one is {@link STATE#ERROR}, then {@link STATE#ERROR} is returned.</li>
     * <li>
     * If three or more unique states are found and one is {@link STATE#NON_EXECUTABLE}, then {@link STATE#NON_EXECUTABLE} is returned.</li>
     * <li>
     * If three or more unique states are found and one is {@link STATE#PARTIAL}, then {@link STATE#PARTIAL} is returned. </>li>
     * </ol>
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    protected STATE executableUnlessItIsnt(JexlNode node, Object data) {
        // Find the states of each child, sorting them in executable-unless-it-isn't priority.
        SortedSet<STATE> states = new TreeSet<>(executableUnlessItIsntComparator);
        readStatesFromChildren(node, data, states);
        
        STATE state;
        // Return EXECUTABLE when there are no states or only an IGNORABLE state.
        if (states.isEmpty() || states.first() == STATE.IGNORABLE) {
            state = STATE.EXECUTABLE;
        } else {
            // Otherwise, the correct state will always be the first priority state.
            state = states.first();
        }
        writeStatesToOutput(node, data, states, state);
        return state;
    }
    
    /**
     * Returns {@link STATE#EXECUTABLE} if the specified node is part of a bounded range (e.g. {@literal (A > 'b' && A < 'c')}. The state will be determined
     * based on the states retrieved for each child of the specified node based on the following cases, in priority order:
     * <ol>
     * <li>
     * If the only identifier is {@value Constants#NO_FIELD}, returns {@link STATE#IGNORABLE}.</li>
     * <li>
     * If the node is within a bounded range, returns {@link STATE#EXECUTABLE}.</li>
     * <li>
     * If the node contains a non-event identifier, returns {@link STATE#ERROR}.</li>
     * <li>
     * Otherwise, returns {@link STATE#NON_EXECUTABLE}.</li>
     * </ol>
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the state
     */
    private STATE visitLtGtNode(JexlNode node, Object data) {
        STATE state;
        // if we got here, then (iff in a bounded, indexed range) we were not wrapped in an ivarator, or in a delayed predicate. So we know it returns 0
        // results.
        if (isNoFieldOnly(node)) {
            state = STATE.IGNORABLE;
        } else if (isWithinBoundedRange(node)) {
            state = STATE.EXECUTABLE;
        } else if (isNonEvent(node)) {
            state = STATE.ERROR;
        } else {
            state = STATE.NON_EXECUTABLE;
        }
        writeIdentifierToOutput(node, data, state);
        return state;
    }
    
    /**
     * Returns whether or not any identifier in the node is {@value Constants#NO_FIELD} or {@value Constants#ANY_FIELD}.
     *
     * @param node
     *            the node
     * @return true if any identifier is {@value Constants#NO_FIELD} or {@value Constants#ANY_FIELD}, or false otherwise.
     */
    private boolean isUnOrNoFielded(JexlNode node) {
        return JexlASTHelper.getIdentifiers(node).stream().anyMatch(this::isAnyOrNoFielded);
    }
    
    /**
     * Returns whether or not any identifier in the specified node is not an indexed field.
     *
     * @param node
     *            the node
     * @return true if any identifier is un-indexed, or false otherwise
     */
    private boolean isUnindexed(JexlNode node) {
        List<ASTIdentifier> identifiers = JexlASTHelper.getIdentifiers(node);
        for (ASTIdentifier identifier : identifiers) {
            if (!(isAnyOrNoFielded(identifier))) {
                if (this.indexedFields == null) {
                    if (config.getIndexedFields() != null && !config.getIndexedFields().isEmpty()) {
                        this.indexedFields = config.getIndexedFields();
                    } else {
                        try {
                            this.indexedFields = this.helper.getIndexedFields(config.getDatatypeFilter());
                        } catch (Exception ex) {
                            log.error("Could not determine whether a field is indexed", ex);
                            throw new RuntimeException("got exception when using MetadataHelper to get indexed fields ", ex);
                        }
                    }
                }
                if (!this.indexedFields.contains(JexlASTHelper.deconstructIdentifier(identifier))) {
                    return true;
                }
            }
        }
        return false;
    }
    
    /**
     * Returns whether or not the specified nodes has any index-only identifiers.
     *
     * @param node
     *            the node
     * @return true if the node has any any index-only identifiers, or false otherwise.
     * @throws RuntimeException
     *             if the index-only fields need to be retrieved from the metadata helper and an exception occurs
     */
    private boolean isIndexOnly(JexlNode node) {
        // Initialize the index-only fields if necessary.
        if (this.indexOnlyFields == null) {
            try {
                this.indexOnlyFields = helper.getIndexOnlyFields(config.getDatatypeFilter());
            } catch (TableNotFoundException e) {
                log.error("Could not determine whether field is index only", e);
                throw new RuntimeException("got exception when using MetadataHelper to get index only fields", e);
            }
        }
        
        return JexlASTHelper.getIdentifiers(node).stream().map(JexlASTHelper::deconstructIdentifier).anyMatch(this.indexOnlyFields::contains);
    }
    
    /**
     * Returns whether or not the specified nodes has any non-event identifiers.
     *
     * @param node
     *            the node
     * @return true if the node has any any non-event identifiers, or false otherwise.
     * @throws RuntimeException
     *             if the non-event fields need to be retrieved from the metadata helper and an exception occurs
     */
    private boolean isNonEvent(JexlNode node) {
        // Initialize the non-event fields if necessary.
        if (this.nonEventFields == null) {
            try {
                this.nonEventFields = helper.getNonEventFields(config.getDatatypeFilter());
            } catch (TableNotFoundException e) {
                log.error("Could not determine whether field is non event", e);
                throw new RuntimeException("got exception when using MetadataHelper to get non event fields", e);
            }
        }
        
        return JexlASTHelper.getIdentifiers(node).stream().map(JexlASTHelper::deconstructIdentifier).filter(ident -> !ident.equals(Constants.NO_FIELD))
                        .anyMatch(this.nonEventFields::contains);
    }
    
    /**
     * Returns whether or not if the specified node is part of an AND for a bounded range, e.g. {@literal (A < 12 && A > 4)}.
     *
     * @param node
     *            the node
     * @return true if the node is within a bounded range or false otherwise.
     */
    private boolean isWithinBoundedRange(JexlNode node) {
        if (node.jjtGetParent() instanceof ASTAndNode) {
            List<JexlNode> otherNodes = new ArrayList<>();
            Map<LiteralRange<?>,List<JexlNode>> ranges = JexlASTHelper.getBoundedRangesIndexAgnostic(node.jjtGetParent(), otherNodes, false);
            return ranges.size() == 1 && otherNodes.isEmpty();
        }
        return false;
    }
    
    /**
     * Populates the specified set with the states returned by each child of the specified node when the child accepts this visitor.
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @param states
     *            the set of states
     */
    private void readStatesFromChildren(final JexlNode node, final Object data, final Set<STATE> states) {
        final int numChildren = node.jjtGetNumChildren();
        for (int i = 0; i < numChildren; i++) {
            states.add((STATE) (node.jjtGetChild(i).jjtAccept(this, data)));
            // Break early if all possibly unique states have already been added.
            if (states.size() == STATE.size) {
                break;
            }
        }
    }
    
    /**
     * Returns true if the identifiers image is equal to {@link Constants#ANY_FIELD} or {@link Constants#NO_FIELD}, or false otherwise.
     */
    private boolean isAnyOrNoFielded(final ASTIdentifier identifier) {
        return identifier.image.equals(Constants.ANY_FIELD) || identifier.image.equals(Constants.NO_FIELD);
    }
    
    private void writeNodeToOutput(final SimpleNode node, final Object data, final STATE state) {
        writeStateResultToOutput(data + node.toString(), state);
    }
    
    private void writeIdentifierToOutput(final JexlNode node, final Object data, final STATE state) {
        writeStateResultToOutput(data + node.toString() + "(" + JexlASTHelper.getIdentifier(node) + ")", state);
    }
    
    private void writeIdentifiersToOutput(final JexlNode node, final Object data, final STATE state) {
        String identifiers = JexlASTHelper.getIdentifiers(node).stream().map(identifier -> identifier.image).map(JexlASTHelper::deconstructIdentifier)
                        .collect(Collectors.joining(","));
        writeStateResultToOutput(data + node.toString() + "[" + identifiers + "]", state);
    }
    
    private void writeStatesToOutput(final JexlNode node, final Object data, final Collection<STATE> states, final STATE state) {
        writeStateResultToOutput(data + node.toString() + states, state);
    }
    
    private void writeStateResultToOutput(final String conditions, final STATE state) {
        writeOutput(conditions + " -> " + state);
    }
    
    /**
     * Adds the specified message as debug output if the output is not null.
     *
     * @param message
     *            the message
     */
    private void writeOutput(final String message) {
        if (output != null) {
            output.writeLine(message);
        }
    }
}
