package datawave.query.jexl.visitors;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.IndexHoleMarkerJexlNode;
import datawave.query.Constants;
import datawave.query.jexl.LiteralRange;
import datawave.query.jexl.nodes.ExceededOrThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededTermThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededValueThresholdMarkerJexlNode;
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
import org.apache.commons.jexl2.parser.ASTEvaluationOnly;
import org.apache.commons.jexl2.parser.ASTConstructorNode;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
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
import org.apache.commons.jexl2.parser.SimpleNode;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Determine if a node can be processed against the global index and/or the field index
 * 
 * A node can be processed against the field index if it is not a ER/NR/LE/LT/GE/GT node (unless surrounded by an ivarator expression).
 * 
 * A node can be processed against the global index if it is not a ER,NR,LE,LT,GE,GT,NOT, or NE (unless surrounded by an ivarator expression).
 * 
 * In general an OR can be processed if it is completely composed of expressions that can be processed. An AND can be processed it at least one of its children
 * can be processed.
 * 
 */
public class ExecutableDeterminationVisitor extends BaseVisitor {
    
    /**
     * EXECUTABLE means that the expression is executable against the index PARTIAL means that we have an OR that cannot be completely satisfied by the index
     * NON_EXECUTABLE means that the expression cannot be executed against the index IGNORABLE means that it does not matter the executable state of the
     * underlying expression ERROR means that we have an expression that is index only but yet cannot be run against the index (negation, delayed prefix)
     */
    public enum STATE {
        EXECUTABLE, PARTIAL, NON_EXECUTABLE, IGNORABLE, ERROR
    }
    
    private static final Logger log = Logger.getLogger(ExecutableDeterminationVisitor.class);
    
    private interface Output {
        void writeLine(String line);
    }
    
    private static class StringListOutput implements Output {
        private final List<String> outputLines;
        
        public StringListOutput(List<String> debugOutput) {
            this.outputLines = debugOutput;
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see PrintingVisitor.Output#writeLine(java.lang.String)
         */
        @Override
        public void writeLine(String line) {
            outputLines.add(line);
        }
    }
    
    private static StringListOutput newStringListOutput(List<String> debugOutput) {
        return new StringListOutput(debugOutput);
        
    }
    
    private static final String PREFIX = "  ";
    
    private StringListOutput output = null;
    
    protected MetadataHelper helper;
    protected boolean forFieldIndex;
    protected Set<String> indexedFields = null;
    protected Set<String> indexOnlyFields = null;
    protected Set<String> nonEventFields = null;
    protected ShardQueryConfiguration config;
    
    public ExecutableDeterminationVisitor(ShardQueryConfiguration conf, MetadataHelper metadata, boolean forFieldIndex) {
        this(conf, metadata, forFieldIndex, null);
    }
    
    public ExecutableDeterminationVisitor(ShardQueryConfiguration conf, MetadataHelper metadata, boolean forFieldIndex, List<String> debugOutput) {
        this.helper = metadata;
        this.config = conf;
        this.forFieldIndex = forFieldIndex;
        if (debugOutput != null) {
            output = newStringListOutput(debugOutput);
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
        STATE state = getState(node, config, helper, forFieldIndex, debugOutput);
        return state == STATE.EXECUTABLE;
    }
    
    public static boolean isExecutable(JexlNode node, ShardQueryConfiguration config, Set<String> indexedFields, Set<String> indexOnlyFields,
                    Set<String> nonEventFields, boolean forFieldIndex, List<String> debugOutput, MetadataHelper metadataHelper) {
        STATE state = getState(node, config, indexedFields, indexOnlyFields, nonEventFields, forFieldIndex, debugOutput, metadataHelper);
        return state == STATE.EXECUTABLE;
    }
    
    /**
     * allOrNone means that all contained children must be executable for this node to be executable. Used for expressions, scripts, and or nodes.
     */
    protected STATE allOrNone(JexlNode node, Object data) {
        STATE state;
        boolean containsIgnorable = false;
        // all children must be executable for a script to be executable
        Set<STATE> states = new HashSet<>();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            states.add((STATE) (node.jjtGetChild(i).jjtAccept(this, data)));
        }
        if (log.isTraceEnabled()) {
            log.trace("node:" + PrintingVisitor.formattedQueryString(node));
            log.trace("states are:" + states);
        }
        // if only one state, then that is the state
        if (states.size() == 1) {
            state = states.iterator().next();
        } else {
            // else remove the ignorable state
            containsIgnorable = states.remove(STATE.IGNORABLE);
            // if now only one state, then that is the state
            if (states.size() == 1) {
                state = states.iterator().next();
            }
            // if we contain an error state, the error
            else if (states.contains(STATE.ERROR)) {
                state = STATE.ERROR;
            } else {
                // otherwise we have a PARTIAL state
                state = STATE.PARTIAL;
            }
        }
        if (output != null) {
            output.writeLine(data + node.toString() + '[' + states + (containsIgnorable ? ",IGNORABLE" : "") + "] -> " + state);
        }
        return state;
    }
    
    protected STATE unlessAnyNonExecutable(JexlNode node, Object data) {
        STATE state;
        boolean containsIgnorable = false;
        // all children must be executable for a script to be executable
        Set<STATE> states = new HashSet<>();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            states.add((STATE) (node.jjtGetChild(i).jjtAccept(this, data)));
        }
        if (log.isTraceEnabled()) {
            log.trace("node:" + PrintingVisitor.formattedQueryString(node));
            log.trace("states are:" + states);
        }
        if (states.contains(STATE.NON_EXECUTABLE)) {
            return STATE.NON_EXECUTABLE;
        } else {
            return STATE.EXECUTABLE;
        }
    }
    
    /**
     * allOrSome means that some of the nodes must be executable for this to be executable. Any partial state results in a partial state however. Used for and
     * nodes.
     */
    protected STATE allOrSome(JexlNode node, Object data) {
        STATE state;
        boolean containsIgnorable = false;
        Set<STATE> states = new HashSet<>();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            states.add((STATE) (node.jjtGetChild(i).jjtAccept(this, data + PREFIX)));
        }
        // if only one state, then that is the state
        if (states.size() == 1) {
            state = states.iterator().next();
        } else {
            // else remove the ignorable state
            containsIgnorable = states.remove(STATE.IGNORABLE);
            // if now only one state, then that is the state
            if (states.size() == 1) {
                state = states.iterator().next();
            }
            // if we contain an error state, the error
            else if (states.contains(STATE.ERROR)) {
                state = STATE.ERROR;
            }
            // if we have a partial state, then partial
            else if (states.contains(STATE.PARTIAL)) {
                state = STATE.PARTIAL;
            }
            // otherwise we have an executable state
            else {
                return STATE.EXECUTABLE;
            }
        }
        if (output != null) {
            output.writeLine(data + node.toString() + '[' + states + (containsIgnorable ? ",IGNORABLE" : "") + "] -> " + state);
        }
        return state;
    }
    
    /**
     * executableUnlessItIsnt means that none of the nodes may be non-executable or error for this to be executable. Introduced to catch NULL literals in a
     * disjuction
     */
    protected STATE executableUnlessItIsnt(JexlNode node, Object data) {
        STATE state;
        boolean containsIgnorable = false;
        Set<STATE> states = new HashSet<>();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            states.add((STATE) (node.jjtGetChild(i).jjtAccept(this, data + PREFIX)));
        }
        // else remove the ignorable state
        containsIgnorable = states.remove(STATE.IGNORABLE);
        // if now only one state, then that is the state
        if (states.size() == 1) {
            state = states.iterator().next();
        }
        // if we contain an error state, the error
        else if (states.contains(STATE.ERROR)) {
            state = STATE.ERROR;
        } else if (states.contains(STATE.NON_EXECUTABLE)) {
            state = STATE.NON_EXECUTABLE;
        }
        // if we have a partial state, then partial
        else if (states.contains(STATE.PARTIAL)) {
            state = STATE.PARTIAL;
        }
        // otherwise we have an executable state
        else {
            return STATE.EXECUTABLE;
        }
        if (output != null) {
            output.writeLine(data + node.toString() + '[' + states + (containsIgnorable ? ",IGNORABLE" : "") + "] -> " + state);
        }
        return state;
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
        if (output != null) {
            output.writeLine(data + node.toString() + '(' + JexlASTHelper.getIdentifier(node) + ") -> " + state);
        }
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
        if (output != null) {
            output.writeLine(data + node.toString() + '(' + JexlASTHelper.getIdentifier(node) + ") -> " + state);
        }
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
        if (output != null) {
            output.writeLine(data + node.toString() + '(' + JexlASTHelper.getIdentifier(node) + ") -> " + state);
        }
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
        if (output != null) {
            output.writeLine(data + node.toString() + '(' + JexlASTHelper.getIdentifier(node) + ") -> " + state);
        }
        return state;
    }
    
    public static boolean isNoFieldOnly(JexlNode node) {
        try {
            return Constants.NO_FIELD.equals(JexlASTHelper.getIdentifier(node));
        } catch (NoSuchElementException e) {
            // no-op
        }
        
        return false;
    }
    
    private boolean isUnOrNoFielded(JexlNode node) {
        List<ASTIdentifier> identifiers = JexlASTHelper.getIdentifiers(node);
        for (ASTIdentifier identifier : identifiers) {
            if (identifier.image.equals(Constants.ANY_FIELD) || identifier.image.equals(Constants.NO_FIELD)) {
                return true;
            }
        }
        return false;
    }
    
    private boolean isUnindexed(JexlNode node) {
        List<ASTIdentifier> identifiers = JexlASTHelper.getIdentifiers(node);
        for (ASTIdentifier identifier : identifiers) {
            if (!(identifier.image.equals(Constants.ANY_FIELD) || identifier.image.equals(Constants.NO_FIELD))) {
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
                if (this.indexedFields.contains(JexlASTHelper.deconstructIdentifier(identifier)) == false) {
                    return true;
                }
            }
        }
        return false;
    }
    
    private boolean isIndexOnly(JexlNode node) {
        if (this.indexOnlyFields == null) {
            try {
                this.indexOnlyFields = helper.getIndexOnlyFields(config.getDatatypeFilter());
            } catch (TableNotFoundException e) {
                log.error("Could not determine whether field is index only", e);
                throw new RuntimeException("got exception when using MetadataHelper to get index only fields", e);
            }
        }
        List<ASTIdentifier> identifiers = JexlASTHelper.getIdentifiers(node);
        for (ASTIdentifier identifier : identifiers) {
            if (this.indexOnlyFields.contains(JexlASTHelper.deconstructIdentifier(identifier))) {
                return true;
            }
        }
        return false;
    }
    
    private boolean isNonEvent(JexlNode node) {
        if (this.nonEventFields == null) {
            try {
                this.nonEventFields = helper.getNonEventFields(config.getDatatypeFilter());
            } catch (TableNotFoundException e) {
                log.error("Could not determine whether field is non event", e);
                throw new RuntimeException("got exception when using MetadataHelper to get non event fields", e);
            }
        }
        List<ASTIdentifier> identifiers = JexlASTHelper.getIdentifiers(node);
        for (ASTIdentifier identifier : identifiers) {
            String deconstructed = JexlASTHelper.deconstructIdentifier(identifier);
            if (deconstructed.equals(Constants.NO_FIELD)) {
                // no field should not be factored in here
                continue;
            }
            if (this.nonEventFields.contains(deconstructed)) {
                return true;
            }
        }
        return false;
    }
    
    private boolean isWithinBoundedRange(JexlNode node) {
        if (node.jjtGetParent() instanceof ASTAndNode) {
            List<JexlNode> otherNodes = new ArrayList<>();
            Map<LiteralRange<?>,List<JexlNode>> ranges = JexlASTHelper.getBoundedRangesIndexAgnostic((ASTAndNode) (node.jjtGetParent()), otherNodes, false);
            if (ranges.size() == 1 && otherNodes.isEmpty()) {
                return true;
            }
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
        if (output != null) {
            output.writeLine(data + node.toString() + '(' + JexlASTHelper.getIdentifier(node) + ") -> " + state);
        }
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
    
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        STATE state;
        // functions nodes are in general not-executable against the index
        state = STATE.NON_EXECUTABLE;
        if (output != null) {
            output.writeLine(data + JexlASTHelper.getFunctions(node).toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTNotNode node, Object data) {
        STATE state;
        if (forFieldIndex) {
            // return the state of the underlying expression
            state = allOrNone(node, data + PREFIX);
        } else {
            state = STATE.NON_EXECUTABLE;
        }
        return state;
    }
    
    @Override
    public Object visit(ASTJexlScript node, Object data) {
        STATE state;
        state = allOrNone(node, data + PREFIX);
        return state;
    }
    
    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        STATE state;
        state = allOrNone(node, data + PREFIX);
        return state;
    }
    
    @Override
    public Object visit(ASTOrNode node, Object data) {
        STATE state;
        state = allOrNone(node, data + PREFIX);
        return state;
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        STATE state;
        // at least one child must be executable for an AND expression to be executable, and none of the other nodes should be partially executable
        // all children must be executable for an OR expression to be executable
        state = allOrSome(node, data + PREFIX);
        return state;
    }
    
    @Override
    public Object visit(SimpleNode node, Object data) {
        STATE state = STATE.IGNORABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTBlock node, Object data) {
        STATE state = STATE.IGNORABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTAmbiguous node, Object data) {
        STATE state = STATE.IGNORABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTIfStatement node, Object data) {
        STATE state = STATE.IGNORABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTWhileStatement node, Object data) {
        STATE state = STATE.IGNORABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTForeachStatement node, Object data) {
        STATE state = STATE.IGNORABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTAssignment node, Object data) {
        STATE state = STATE.IGNORABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTTernaryNode node, Object data) {
        STATE state = STATE.IGNORABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTBitwiseOrNode node, Object data) {
        STATE state = STATE.IGNORABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTBitwiseXorNode node, Object data) {
        STATE state = STATE.IGNORABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTBitwiseAndNode node, Object data) {
        STATE state = STATE.IGNORABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTAdditiveNode node, Object data) {
        STATE state = STATE.IGNORABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTAdditiveOperator node, Object data) {
        STATE state = STATE.IGNORABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTMulNode node, Object data) {
        STATE state = STATE.IGNORABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTDivNode node, Object data) {
        STATE state = STATE.IGNORABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTModNode node, Object data) {
        STATE state = STATE.IGNORABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTUnaryMinusNode node, Object data) {
        STATE state = STATE.IGNORABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTBitwiseComplNode node, Object data) {
        STATE state = STATE.IGNORABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTIdentifier node, Object data) {
        STATE state = STATE.IGNORABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTNullLiteral node, Object data) {
        STATE state = STATE.NON_EXECUTABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTTrueNode node, Object data) {
        STATE state = STATE.IGNORABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTFalseNode node, Object data) {
        STATE state = STATE.IGNORABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTIntegerLiteral node, Object data) {
        STATE state = STATE.IGNORABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTFloatLiteral node, Object data) {
        STATE state = STATE.IGNORABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTStringLiteral node, Object data) {
        STATE state = STATE.IGNORABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTArrayLiteral node, Object data) {
        STATE state = STATE.IGNORABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTMapLiteral node, Object data) {
        STATE state = STATE.IGNORABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTMapEntry node, Object data) {
        STATE state = STATE.IGNORABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTEmptyFunction node, Object data) {
        STATE state = STATE.IGNORABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTSizeFunction node, Object data) {
        STATE state = STATE.IGNORABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTMethodNode node, Object data) {
        STATE state = STATE.NON_EXECUTABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTSizeMethod node, Object data) {
        STATE state = STATE.NON_EXECUTABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTConstructorNode node, Object data) {
        STATE state = STATE.IGNORABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTArrayAccess node, Object data) {
        STATE state = STATE.IGNORABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTReturnStatement node, Object data) {
        STATE state = STATE.IGNORABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTVar node, Object data) {
        STATE state = STATE.IGNORABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
    @Override
    public Object visit(ASTNumberLiteral node, Object data) {
        STATE state = STATE.IGNORABLE;
        if (output != null) {
            output.writeLine(data + node.toString() + " -> " + state);
        }
        return state;
    }
    
}
