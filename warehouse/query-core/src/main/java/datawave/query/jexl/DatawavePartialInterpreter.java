package datawave.query.jexl;

import datawave.query.attributes.ValueTuple;
import datawave.query.collections.FunctionalSet;
import datawave.query.jexl.functions.ContentFunctions;
import datawave.query.jexl.functions.FunctionJexlNodeVisitor;
import datawave.query.jexl.functions.JexlFunctionArgumentDescriptorFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.jexl.nodes.BoundedRange;
import datawave.query.jexl.nodes.ExceededOrThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.BaseVisitor;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.JexlException;
import org.apache.commons.jexl2.parser.ASTAdditiveNode;
import org.apache.commons.jexl2.parser.ASTAdditiveOperator;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTIfStatement;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTMethodNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTSizeMethod;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.log4j.Logger;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Set;

/**
 * <h1>Partial Document Evaluation</h1>
 * <p>
 * The DatawavePartialInterpreter supports partial document evaluation. Any query term whose field is also a member of the 'incompleteFields' set will be
 * treated as 'true' even though the evaluation state is actually 'unknown'. It is expected that a full document evaluation will happen later.
 * <p>
 * If an incomplete field contributes to the final evaluation result the document is marked via the {@link PartialInterpreterCallback} so the
 * {@link datawave.query.function.ws.EvaluationFunction} knows to run a full evaluation. Conversely, if a document fully evaluates to true without an incomplete
 * field, the document is marked so the {@link datawave.query.function.ws.EvaluationFunction} does not run a second, unnecessary evaluation.
 * <p>
 * <h2>Evaluation State</h2>
 * The current evaluation state is tracked via a {@link State} object. This object records if an 'incomplete field' contributed to a positive evaluation state,
 * a functional set of 'hits', and possibly a numeric value.
 * <p>
 * <h2>Evaluation Logic (Intersections)</h2>
 * <ul>
 * <li>(TRUE and TRUE) evaluates to TRUE and is a FULL evaluation</li>
 * <li>(TRUE and FALSE) evaluates to FALSE and is a FULL evaluation</li>
 * <li>(FALSE and FALSE) evaluates to FALSE and is a FULL evaluation</li>
 * <li>(TRUE and UNKNOWN) evaluates to TRUE and is a PARTIAL evaluation</li>
 * <li>(FALSE and UNKNOWN) evaluates to FALSE and is a FULL evaluation</li>
 * <li>(UNKNOWN and UNKNOWN) evaluates to TRUE and is a PARTIAL evaluation</li>
 * </ul>
 * <h2>Evaluation Logic (Unions)</h2>
 * <ul>
 * <li>(TRUE or TRUE) evaluates to TRUE and is a FULL evaluation</li>
 * <li>(TRUE or FALSE) evaluates to TRUE and is a FULL evaluation</li>
 * <li>(FALSE or FALSE) evaluates to FALSE and is a FULL evaluation</li>
 * <li>(TRUE or UNKNOWN) evaluates to TRUE and is a FULL evaluation</li>
 * <li>(FALSE or UNKNOWN) evaluates to TRUE and is a PARTIAL evaluation</li>
 * <li>(UNKNOWN or UNKNOWN) evaluates to TRUE and is a PARTIAL evaluation</li>
 * </ul>
 * <h2>Negation Logic</h2>
 * <ul>
 * <li>!(TRUE) evaluates to FALSE and is a FULL evaluation</li>
 * <li>!(FALSE) evaluates to TRUE and is a FULL evaluation</li>
 * <li>!(UNKNOWN) evaluates to TRUE and is a PARTIAL evaluation</li>
 * </ul>
 */
public class DatawavePartialInterpreter extends DatawaveInterpreter {
    
    private static final Logger log = Logger.getLogger(DatawavePartialInterpreter.class);
    private final Set<String> incompleteFields;
    private final PartialInterpreterCallback callback;
    
    public DatawavePartialInterpreter(JexlEngine jexl, JexlContext aContext, boolean strictFlag, boolean silentFlag, Set<String> incompleteFields,
                    PartialInterpreterCallback callback) {
        super(jexl, aContext, strictFlag, silentFlag);
        this.incompleteFields = incompleteFields;
        this.callback = callback;
        this.callback.reset();
    }
    
    /**
     * Sets the callback state if an incomplete field contributed to a positive evaluation
     *
     * @param node
     *            the JexlNode
     * @return true if the query matched the document
     */
    @Override
    public Object interpret(JexlNode node) {
        Object result = super.interpret(node);
        if (result instanceof State) {
            State state = (State) result;
            if (state.isMatched() && state.isIncomplete()) {
                callback.setUsed(true); // only set the callback if the documented matched
            }
        } else {
            throw new IllegalStateException("result of interpret was not STATE, it was " + result.getClass().getSimpleName());
        }
        return result;
    }
    
    /**
     * Nodes may be defeated for the purposes of evaluation prior to visiting.
     * <p>
     * If a node relies wholly upon incomplete fields a visit is not required.
     *
     * @param node
     *            a JexlNode
     * @return a State object if an incomplete field exists, or null if no incomplete fields are present
     */
    private State getPreOrderState(JexlNode node) {
        
        String nodeString = JexlStringBuildingVisitor.buildQueryWithoutParse(node);
        Object result = resultMap.get(nodeString);
        if (result instanceof State) {
            return (State) result;
        }
        
        boolean isPresent;
        boolean isIncomplete;
        boolean incompleteAndPresent = false;
        boolean incompleteAndAbsent = false;
        boolean completeAndPresent = false;
        
        Set<String> fields = JexlASTHelper.getIdentifierNames(node);
        for (String field : fields) {
            field = JexlASTHelper.deconstructIdentifier(field);
            isPresent = isFieldPresent(field);
            isIncomplete = isFieldIncomplete(field);
            
            if (isIncomplete) {
                if (isPresent) {
                    incompleteAndPresent = true;
                } else {
                    incompleteAndAbsent = true;
                }
            } else if (isPresent) {
                completeAndPresent = true;
            }
        }
        
        State state = null;
        boolean hasMethod = new HasMethodVisitor().hasMethod(node);
        
        if (hasMethod && incompleteAndPresent) {
            
            // odd case like (FIELD_A || FIELD_B).getValuesForGroups(grouping:getGroupsForMatchesInGroup(...)...
            state = new State(true, true);
            
        } else if (!completeAndPresent) {
            
            if (incompleteAndPresent) {
                state = new State(true, true);
            } else if (incompleteAndAbsent) {
                state = new State(false, true);
            }
        }
        
        if (completeAndPresent && incompleteAndPresent) {
            // Case like (FIELD == INCOMPLETE)
            state = new State(true, true);
        }
        
        if (state != null) {
            resultMap.put(nodeString, state.copy());
        }
        return state;
    }
    
    /**
     * No fields were incomplete
     *
     * @param node
     *            a JexlNode
     * @param result
     *            the result of a delegate call to the {@link DatawaveInterpreter}
     * @return a State object
     */
    private State getPostOrderState(JexlNode node, Object result) {
        
        State state;
        boolean matched = isMatched(result) || result instanceof ValueTuple;
        
        if (matched) {
            state = new State(true, false, result);
        } else {
            state = new State(false, false, result);
        }
        
        String nodeString = JexlStringBuildingVisitor.buildQueryWithoutParse(node);
        resultMap.put(nodeString, state.copy());
        return state;
    }
    
    /**
     * Returns true if the provided JexlNode contains an identifier that is also present in the set of incomplete fields
     *
     * @param node
     *            the JexlNode
     * @return true any fields are considered incomplete
     */
    private boolean isFieldIncomplete(JexlNode node) {
        for (String field : JexlASTHelper.getIdentifierNames(node)) {
            field = JexlASTHelper.deconstructIdentifier(field);
            if (isFieldIncomplete(field)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Returns true if the field is in the set of incomplete fields
     * 
     * @param field
     *            a field
     * @return true if the field is incomplete
     */
    private boolean isFieldIncomplete(String field) {
        return incompleteFields.contains(field);
    }
    
    /**
     * Determines if the provided JexlNode contains a field that exists in the context
     *
     * @param node
     *            a JexlNode
     * @return true if any field is present in the context
     */
    private boolean isFieldPresent(JexlNode node) {
        for (String field : JexlASTHelper.getIdentifierNames(node)) {
            field = JexlASTHelper.deconstructIdentifier(field);
            if (isFieldPresent(field)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Determines if a field exists in the context
     *
     * @param field
     *            a field
     * @return true if the field can never be evaluated
     */
    private boolean isFieldPresent(String field) {
        return context.has(field);
    }
    
    /**
     * Determines if the result of a script execution evaluates to true
     *
     * @param scriptExecuteResult
     *            the result of visiting a query node
     * @return true if the object matched
     */
    public static boolean isMatched(Object scriptExecuteResult) {
        if (scriptExecuteResult instanceof State) {
            State state = (State) scriptExecuteResult;
            return state.isMatched();
        } else {
            return DatawaveInterpreter.isMatched(scriptExecuteResult);
        }
    }
    
    /**
     *
     * @param node
     *            an ASTFunctionNode
     * @param data
     *            some data
     * @return a result
     */
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        
        State state = getPreOrderState(node);
        if (state != null) {
            return state;
        }
        
        // get the namespace
        FunctionJexlNodeVisitor visitor = new FunctionJexlNodeVisitor();
        visitor.visit(node, null);
        
        // content functions should always have the field as the first argument
        if (visitor.namespace().equals(ContentFunctions.CONTENT_FUNCTION_NAMESPACE)) {
            JexlNode first = visitor.args().get(0);
            if (first instanceof ASTIdentifier) {
                String field = JexlASTHelper.deconstructIdentifier((ASTIdentifier) first);
                if (isFieldIncomplete(field) && isFieldPresent(field)) {
                    return new State(true, true);
                }
            }
        } else {
            // fall back to descriptor for non-content functions
            JexlArgumentDescriptor descriptor = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(node);
            Set<String> fields = descriptor.fields(null, null);
            
            for (String field : fields) {
                if (isFieldIncomplete(field) && isFieldPresent(field)) {
                    return new State(true, true);
                }
            }
        }
        
        // if no determination could be made about an UNKNOWN status, fallback to a normal visit
        Object result = super.visit(node, data);
        
        // functions with siblings have the collection returned, otherwise collections are cast to a boolean
        boolean hasSiblings = hasSiblings(node);
        boolean matched = true;
        
        // cast result to boolean if no sibling methods found
        if (!hasSiblings && result instanceof Collection) {
            Collection collection = (Collection) result;
            if (collection.isEmpty()) {
                matched = false;
            }
        }
        
        if (result instanceof Boolean && !((Boolean) result)) {
            matched = false;
        }
        
        if (matched) {
            return new State(true, false, result);
        } else {
            return new State(false, false);
        }
    }
    
    /**
     * Arithmetic expressions like <code>1 + 1 + 1 == 3</code> means we can't simply call super.visit(ASTEQNode)
     * <p>
     *
     *
     * @param node
     *            an ASTEQNode
     * @param data
     *            an object
     * @return the resulting State object
     */
    @Override
    public Object visit(ASTEQNode node, Object data) {
        State state = getPreOrderState(node);
        if (state != null) {
            return state;
        }
        
        Object result;
        Object left = node.jjtGetChild(0).jjtAccept(this, data);
        Object right = node.jjtGetChild(1).jjtAccept(this, data);
        try {
            // we could have an expression like '1+1+1 == 3'
            if (left instanceof State) {
                state = (State) left;
                // left = state.narrowToNumber();
                left = state.getValue();
            }
            
            result = arithmetic.equals(left, right) ? Boolean.TRUE : Boolean.FALSE;
        } catch (ArithmeticException xrt) {
            throw new JexlException(node, "== error", xrt);
        }
        
        return getPostOrderState(node, result);
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        State state = getPreOrderState(node);
        if (state != null) {
            return state;
        }
        
        Object result = super.visit(node, data);
        return getPostOrderState(node, result);
    }
    
    @Override
    public Object visit(ASTOrNode node, Object data) {
        Deque<JexlNode> children = new ArrayDeque<>();
        Deque<JexlNode> stack = new ArrayDeque<>();
        stack.push(node);
        
        boolean allIdentifiers = true;
        
        // iterative depth-first traversal of tree to avoid stack
        // overflow when traversing large or'd lists
        JexlNode current;
        JexlNode child;
        while (!stack.isEmpty()) {
            current = stack.pop();
            
            if (current instanceof ASTOrNode) {
                for (int i = current.jjtGetNumChildren() - 1; i >= 0; i--) {
                    child = JexlASTHelper.dereference(current.jjtGetChild(i));
                    stack.push(child);
                }
            } else {
                children.push(current);
                if (allIdentifiers && !(current instanceof ASTIdentifier)) {
                    allIdentifiers = false;
                }
            }
        }
        
        // If all ASTIdentifiers, then traverse the whole queue. Otherwise, we can attempt to short circuit.
        State result = null;
        if (allIdentifiers) {
            // Likely within a function and must visit every child in the stack.
            // Failure to do so will short circuit value aggregation leading to incorrect function evaluation.
            while (!children.isEmpty()) {
                // Child nodes were put onto the stack left to right. PollLast to evaluate left to right.
                result = (State) interpretOr(children.pollLast().jjtAccept(this, data), result);
            }
        } else {
            
            // We are likely within a normal union and can short circuit
            while (!children.isEmpty()) {
                // Child nodes were put onto the stack left to right. PollLast to evaluate left to right.
                result = (State) interpretOr(children.pollLast().jjtAccept(this, data), result);
                
                if (result.isMatched() && !result.isIncomplete()) {
                    break;
                }
            }
            return result;
        }
        return result;
    }
    
    /**
     * Interpret the result of a union via pairwise element evaluation
     *
     * @param left
     *            the current element
     * @param right
     *            the current evaluation state of this union
     * @return the updated evaluation state of this union
     */
    @Override
    public Object interpretOr(Object left, Object right) {
        State leftState = null;
        State rightState = null;
        
        if (left != null) {
            if (left instanceof State) {
                leftState = (State) left;
            } else if (left instanceof FunctionalSet) {
                leftState = new State(true, false, left);
            }
        }
        
        if (right != null) {
            if (right instanceof State) {
                rightState = (State) right;
            } else if (right instanceof FunctionalSet) {
                rightState = new State(true, false, right);
            }
        }
        
        if (leftState != null && rightState != null) {
            leftState.mergeOr(rightState);
            return leftState;
        } else if (leftState != null) {
            return leftState;
        } else if (rightState != null) {
            return rightState;
        } else {
            // if we got here then we are likely dealing with a function that takes a union of null fields as arguments
            // for example, filter:notNull(NULL1 || NULL2).
            return new State(false);
        }
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        // we could have arrived here after the node was dereferenced
        if (QueryPropertyMarker.findInstance(node).isType(ExceededOrThresholdMarkerJexlNode.class)) {
            Object o = visitExceededOrThresholdMarker(node);
            return getState(node, o);
        }
        
        // check for the special case of a range (conjunction of a G/GE and a L/LE node) and reinterpret as a function
        Object evaluation = evaluateRange(node);
        if (evaluation != null) {
            return getState(node, evaluation);
        }
        
        // values for intersection
        boolean anyFromUnknown = false;
        FunctionalSet functionalSet = new FunctionalSet();
        for (JexlNode child : JexlNodes.children(node)) {
            
            Object o = child.jjtAccept(this, data);
            if (o instanceof State) {
                State state = (State) o;
                if (!state.isMatched()) {
                    return state;
                } else {
                    if (state.isIncomplete()) {
                        anyFromUnknown = true;
                    }
                    if (state.isFunctionalSet()) {
                        functionalSet.addAll(state.getFunctionalSet());
                    }
                }
            } else {
                throw new IllegalStateException("expected STATE but was " + o.getClass().getSimpleName());
            }
        }
        
        if (!functionalSet.isEmpty()) {
            return new State(true, anyFromUnknown, functionalSet);
        } else {
            return new State(true, anyFromUnknown);
        }
    }
    
    @Override
    public Object visit(ASTAssignment node, Object data) {
        Object match = super.visit(node, data);
        return getState(node, match);
    }
    
    @Override
    public Object visit(ASTGENode node, Object data) {
        
        State state = getPreOrderState(node);
        if (state != null) {
            return state;
        }
        
        Object left = node.jjtGetChild(0).jjtAccept(this, data);
        Object right = node.jjtGetChild(1).jjtAccept(this, data);
        try {
            if (left instanceof State) {
                state = (State) left;
                if (state.isIncomplete()) {
                    return state;
                } else {
                    left = state.narrowToNumber();
                }
            }
            if (right instanceof State) {
                state = (State) right;
                if (state.isIncomplete()) {
                    return state;
                } else {
                    right = state.narrowToNumber();
                }
            }
            
            // neither side had an incomplete field if we reached this point
            if (arithmetic.greaterThanOrEqual(left, right)) {
                return new State(true);
            } else {
                return new State(false);
            }
        } catch (ArithmeticException xrt) {
            throw new JexlException(node, ">= error", xrt);
        }
    }
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        
        State state = getPreOrderState(node);
        if (state != null) {
            return state;
        }
        
        Object left = node.jjtGetChild(0).jjtAccept(this, data);
        Object right = node.jjtGetChild(1).jjtAccept(this, data);
        try {
            if (left instanceof State) {
                state = (State) left;
                if (state.isIncomplete()) {
                    return state;
                } else {
                    left = state.narrowToNumber();
                }
            }
            if (right instanceof State) {
                state = (State) right;
                if (state.isIncomplete()) {
                    return state;
                } else {
                    right = state.narrowToNumber();
                }
            }
            
            // neither side had an incomplete field if we reached this point
            if (arithmetic.greaterThan(left, right)) {
                return new State(true);
            } else {
                return new State(false);
            }
        } catch (ArithmeticException xrt) {
            throw new JexlException(node, "> error", xrt);
        }
    }
    
    @Override
    public Object visit(ASTIfStatement node, Object data) {
        Object o = super.visit(node, data);
        return getState(node, o);
    }
    
    @Override
    public Object visit(ASTLENode node, Object data) {
        
        State state = getPreOrderState(node);
        if (state != null) {
            return state;
        }
        
        Object left = node.jjtGetChild(0).jjtAccept(this, data);
        Object right = node.jjtGetChild(1).jjtAccept(this, data);
        try {
            if (left instanceof State) {
                state = (State) left;
                if (state.isIncomplete()) {
                    return state;
                } else {
                    left = state.narrowToNumber();
                }
            }
            if (right instanceof State) {
                state = (State) right;
                if (state.isIncomplete()) {
                    return state;
                } else {
                    right = state.narrowToNumber();
                }
            }
            
            // neither side had an incomplete field if we reached this point
            if (arithmetic.lessThanOrEqual(left, right)) {
                return new State(true);
            } else {
                return new State(false);
            }
        } catch (ArithmeticException xrt) {
            throw new JexlException(node, "<= error", xrt);
        }
    }
    
    @Override
    public Object visit(ASTLTNode node, Object data) {
        
        State state = getPreOrderState(node);
        if (state != null) {
            return state;
        }
        
        Object left = node.jjtGetChild(0).jjtAccept(this, data);
        Object right = node.jjtGetChild(1).jjtAccept(this, data);
        try {
            if (left instanceof State) {
                state = (State) left;
                if (state.isIncomplete()) {
                    return state;
                } else {
                    left = state.getValue();
                }
            }
            if (right instanceof State) {
                state = (State) right;
                if (state.isIncomplete()) {
                    return state;
                } else {
                    right = state.getValue();
                }
            }
            
            // neither side had an incomplete field if we reached this point
            if (arithmetic.lessThan(left, right)) {
                return new State(true);
            } else {
                return new State(false);
            }
        } catch (ArithmeticException xrt) {
            throw new JexlException(node, "< error", xrt);
        }
    }
    
    @Override
    public Object visit(ASTNENode node, Object data) {
        Object match = super.visit(node, data);
        return getState(node, match);
    }
    
    @Override
    public Object visit(ASTNRNode node, Object data) {
        Object match = super.visit(node, data);
        return getState(node, match);
    }
    
    @Override
    public Object visit(ASTNotNode node, Object data) {
        Object o = node.jjtGetChild(0).jjtAccept(this, data);
        
        if (!(o instanceof State)) {
            o = getState(node, o);
        }
        
        State state = (State) o;
        if (state.isIncomplete() && state.isMatched()) {
            // persist "matched from unknown" as-is
            return state;
        } else {
            // otherwise, flip the sign and return
            state.setMatched(!state.isMatched());
            return state;
        }
    }
    
    @Override
    public Object visit(ASTReference node, Object data) {
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(node);
        Object match;
        if (instance.isType(ExceededOrThresholdMarkerJexlNode.class)) {
            Object o = visitExceededOrThresholdMarker(node);
            match = getState(node, o);
        } else if (instance.isAnyTypeExcept(BoundedRange.class)) {
            JexlNode source = instance.getSource();
            match = visitSource(source, data);
        } else {
            match = super.visit(node, data);
        }
        return match;
    }
    
    /**
     * Call the correct super.visit() method
     *
     * @param node
     *            a JexlNode
     * @param data
     *            the data
     * @return the result of calling super.visit()
     */
    private Object visitSource(JexlNode node, Object data) {
        if (node instanceof ASTEQNode) {
            return visit((ASTEQNode) node, data);
        } else if (node instanceof ASTERNode) {
            return visit((ASTERNode) node, data);
        } else if (node instanceof ASTAndNode) {
            return visit((ASTAndNode) node, data);
        } else if (node instanceof ASTOrNode) {
            return visit((ASTOrNode) node, data);
        } else if (node instanceof ASTLTNode) {
            return visit((ASTLTNode) node, data);
        } else if (node instanceof ASTLENode) {
            return visit((ASTLENode) node, data);
        } else if (node instanceof ASTGTNode) {
            return visit((ASTGTNode) node, data);
        } else if (node instanceof ASTGENode) {
            return visit((ASTGENode) node, data);
        } else if (node instanceof ASTFunctionNode) {
            return visit((ASTFunctionNode) node, data);
        } else if (node instanceof ASTAssignment) {
            return visit((ASTAssignment) node, data);
        } else if (node instanceof ASTAdditiveNode) {
            return visit((ASTAdditiveNode) node, data);
        } else if (node instanceof ASTNotNode) {
            return visit((ASTNotNode) node, data);
        } else {
            throw new IllegalStateException("Unknown source node was type: " + node.getClass().getSimpleName());
        }
    }
    
    @Override
    public Object visit(ASTSizeMethod node, Object data) {
        if (data instanceof State) {
            State state = (State) data;
            Object value = state.getValue();
            if (value instanceof FunctionalSet) {
                value = ((FunctionalSet) value).size();
            } else if (value instanceof Collection) {
                value = ((Collection) value).size();
            }
            
            return new State(state.isMatched(), state.isIncomplete(), value);
        }
        
        // always returns a number
        Object result = super.visit(node, data);
        return new State(true, false, result);
    }
    
    /**
     * See {@link org.apache.commons.jexl2.Interpreter#visit(ASTAdditiveNode, Object)}
     * 
     * @param node
     *            a JexlNode
     * @param data
     *            a data
     * @return the result of visiting this node
     */
    public Object visit(ASTAdditiveNode node, Object data) {
        Object left = node.jjtGetChild(0).jjtAccept(this, data);
        State leftState;
        if (left instanceof State) {
            leftState = (State) left;
        } else {
            leftState = getState(node.jjtGetChild(0), left);
        }
        
        if (leftState.isIncomplete()) {
            return leftState;
        }
        
        Object result = null;
        for (int c = 2, size = node.jjtGetNumChildren(); c < size; c += 2) {
            Object right = node.jjtGetChild(c).jjtAccept(this, data);
            State rightState = getState(node.jjtGetChild(c), right);
            
            // if the left result object is an instance of a state
            if (left instanceof State) {
                State state = (State) left;
                if (state.isNarrowableToNumber()) {
                    left = state.narrowToNumber();
                } else {
                    throw new IllegalStateException("Could not coalesce State to numeric for additive op");
                }
            }
            
            if (right instanceof State) {
                State state = (State) right;
                
                if (state.isIncomplete()) {
                    throw new IllegalStateException("additive node had a right arg that was incomplete");
                }
                
                if (state.isNarrowableToNumber()) {
                    right = state.narrowToNumber();
                } else {
                    throw new IllegalStateException("Could not coalesce State to numeric for additive op");
                }
            }
            
            try {
                JexlNode op = node.jjtGetChild(c - 1);
                if (op instanceof ASTAdditiveOperator) {
                    String which = op.image;
                    if ("+".equals(which)) {
                        result = arithmetic.add(left, right);
                        left = result;
                        continue;
                    }
                    if ("-".equals(which)) {
                        result = arithmetic.subtract(left, right);
                        left = result;
                        continue;
                    }
                    throw new UnsupportedOperationException("unknown operator " + which);
                }
                throw new IllegalArgumentException("unknown operator " + op);
            } catch (ArithmeticException xrt) {
                JexlNode xnode = findNullOperand(xrt, node, leftState, rightState);
                throw new JexlException(xnode, "+/- error", xrt);
            }
        }
        
        return new State(true, false, result);
    }
    
    public Object visit(ASTMethodNode node, Object data) {
        
        State state = getPreOrderState(node);
        if (state != null) {
            return state;
        }
        
        Object result;
        if (data == null) {
            result = super.visit(node, FunctionalSet.emptySet());
        } else {
            if (data instanceof State) {
                state = (State) data;
                result = super.visit(node, state.getValue());
            } else {
                result = super.visit(node, data);
            }
        }
        
        return getPostOrderState(node, result);
    }
    
    /**
     * Helper method that determines if a functional set contains a ValueTuple for an incomplete field
     * 
     * @param fields
     *            a set of incomplete fields
     * @param set
     *            a FunctionalSet
     * @return true if the FunctionalSet contains a ValueTuple for an incomplete field
     */
    private boolean isFunctionalSetIncomplete(Set<String> fields, FunctionalSet set) {
        ValueTuple vt;
        for (Object o : set) {
            if (o instanceof ValueTuple) {
                vt = (ValueTuple) o;
                if (fields.contains(vt.getFieldName())) {
                    return true;
                }
            }
        }
        return false;
    }
    
    /**
     * Generate a {@link State} from a JexlNode, and its value from a super call to {@link DatawaveInterpreter}
     *
     * @param node
     *            the node
     * @param scriptExecuteResult
     *            the result of script execution
     * @return a State object
     */
    private State getState(JexlNode node, Object scriptExecuteResult) {
        boolean isFieldIncomplete = isFieldIncomplete(node);
        boolean matched = isMatched(scriptExecuteResult) || scriptExecuteResult instanceof ValueTuple || isFieldIncomplete;
        
        if (scriptExecuteResult instanceof ValueTuple) {
            return new State(true, isFieldIncomplete, new FunctionalSet(Collections.singleton(scriptExecuteResult)));
        } else if (scriptExecuteResult instanceof Integer || scriptExecuteResult instanceof Long) {
            // if we got a numeric script result this could be the result of ASTMult or ASTDiv node. In this case we
            // did match and must persist the result as a numeric
            return new State(true, isFieldIncomplete, scriptExecuteResult);
        }
        return new State(matched, isFieldIncomplete);
    }
    
    /**
     * Track some basic metadata about the state of each node in the tree.
     * <ul>
     * <li>the evaluation result (boolean)</li>
     * <li>if the evaluation is from an incomplete field (boolean)</li>
     * <li>a backing object</li>
     * </ul>
     * <p>
     * The backing object may be a {@link FunctionalSet}, {@link Collection}, {@link ValueTuple}, or null.
     */
    public class State {
        
        boolean matched; // evaluation result
        boolean incomplete; // did an incomplete field contribute to the evaluation state?
        Object value; // FunctionalSet, Collection, ValueTuple, Numeric, or null;
        
        public State(boolean matched) {
            this(matched, false, new FunctionalSet());
        }
        
        public State(boolean matched, boolean incomplete) {
            this(matched, incomplete, new FunctionalSet());
        }
        
        public State(boolean matched, boolean incomplete, Object value) {
            this.matched = matched;
            this.incomplete = incomplete;
            if (value instanceof ValueTuple) {
                FunctionalSet set = new FunctionalSet<>();
                set.add((ValueTuple) value);
                value = set;
            }
            this.value = value;
        }
        
        /**
         * Merge this state with another state with union rules
         *
         * @param other
         *            another State
         */
        public void mergeOr(State other) {
            
            // Case 1: both match
            if (this.isMatched() && other.isMatched()) {
                // matched remains the same
                // only persist incomplete flag if both sides are incomplete
                this.incomplete = this.isIncomplete() && other.isIncomplete();
                
                if (this.isIncomplete() && other.isIncomplete()) {
                    // no boolean, just two empty functional sets
                } else if (this.isIncomplete() && !other.isIncomplete()) {
                    this.value = other.getValue();
                } else if (!this.isIncomplete() && other.isIncomplete()) {
                    // no-op
                } else {
                    // merge the values
                    if (isFunctionalSet() && other.isFunctionalSet()) {
                        FunctionalSet set = new FunctionalSet();
                        set.addAll(getFunctionalSet());
                        set.addAll(other.getFunctionalSet());
                        this.value = set;
                    } else {
                        throw new IllegalStateException("expected two FunctionalSets when merging States");
                    }
                }
            }
            // Case 2: left side matches
            else if (this.isMatched() && !other.isMatched()) {
                // matched remains the same
                // incomplete remains the same
                // value remains the same
                // effectively a no-op
            }
            // Case 3: right side matches
            else if (!this.isMatched() && other.isMatched()) {
                this.matched = other.isMatched();
                // use other incomplete state
                this.incomplete = other.isIncomplete();
                this.value = other.getValue();
            }
            // Case 4: neither side matches
            else {
                // no-op?
                int i = 0;
                if (this.isIncomplete() != other.isIncomplete()) {
                    throw new IllegalStateException("merge OR error: incomplete states did not match");
                }
            }
        }
        
        public boolean isMatched() {
            return this.matched;
        }
        
        public void setMatched(boolean matched) {
            this.matched = matched;
        }
        
        public boolean isIncomplete() {
            return this.incomplete;
        }
        
        /**
         *
         * @return the raw backing object without any casting
         */
        public Object getValue() {
            return this.value;
        }
        
        // utility methods for determining value type
        
        public boolean isNumber() {
            return this.value instanceof Number;
        }
        
        public Number getNumber() {
            return (Number) this.value;
        }
        
        public boolean isFunctionalSet() {
            return value instanceof FunctionalSet;
        }
        
        public FunctionalSet getFunctionalSet() {
            return (FunctionalSet) value;
        }
        
        public boolean isValueTuple() {
            return value instanceof ValueTuple || (value instanceof FunctionalSet && ((FunctionalSet) value).iterator().next() instanceof ValueTuple);
        }
        
        public ValueTuple getValueTuple() {
            if (value instanceof ValueTuple) {
                return (ValueTuple) value;
            } else if (value instanceof FunctionalSet) {
                return (ValueTuple) ((FunctionalSet) value).iterator().next();
            } else {
                return null;
            }
        }
        
        public boolean isNarrowableToNumber() {
            return value instanceof Number || value instanceof FunctionalSet || value instanceof Collection;
        }
        
        /**
         * Narrows the value to a number. If the value is a FunctionalSet the size of the set is returned.
         *
         * @return a numeric representation of the value, or null if so such representation is possible
         */
        public Object narrowToNumber() {
            if (isNumber()) {
                return getNumber();
            } else if (isFunctionalSet()) {
                FunctionalSet set = getFunctionalSet();
                if (set.size() == 1) {
                    Object o = set.iterator().next();
                    if (o instanceof ValueTuple) {
                        return o;
                    }
                }
                // fallback to size of functional set
                return set.size();
            } else if (value instanceof Collection<?>) {
                // for example, the result of 'grouping:getGroupsForMatchesInGroup'
                return ((Collection<?>) value).size();
            } else {
                return null;
            }
        }
        
        /**
         * Get a copy of the current state
         * 
         * @return
         */
        public State copy() {
            return new State(this.matched, this.incomplete, this.value);
        }
    }
    
    class HasMethodVisitor extends BaseVisitor {
        
        private boolean methodExists = false;
        
        public HasMethodVisitor() {
            
        }
        
        public boolean hasMethod(JexlNode node) {
            node.jjtAccept(this, null);
            return methodExists;
        }
        
        @Override
        public Object visit(ASTMethodNode node, Object data) {
            methodExists = true;
            return data;
        }
    }
}
