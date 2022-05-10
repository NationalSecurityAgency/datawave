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

import java.math.BigDecimal;
import java.math.BigInteger;
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
        Object matched = super.interpret(node);
        if (matched instanceof State) {
            State state = (State) matched;
            if (state.matched && state.fromUnknown) {
                callback.setUsed(true);
            }
        } else {
            throw new IllegalStateException("result of interpret was not STATE, it was " + matched.getClass().getSimpleName());
        }
        return matched;
    }
    
    /**
     * Returns true if the provided JexlNode contains an identifier that is also present in the set of incomplete fields
     *
     * @param node
     *            the JexlNode
     * @return true if incomplete fieldname found, false otherwise
     */
    private boolean isIncomplete(JexlNode node) {
        for (String field : JexlASTHelper.getIdentifierNames(node)) {
            field = JexlASTHelper.deconstructIdentifier(field);
            // if the field is not in the context, we cannot evaluate it.
            return incompleteFields.contains(field) && context.has(field);
        }
        return false;
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
            return state.matched;
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
        
        // get the namespace
        FunctionJexlNodeVisitor visitor = new FunctionJexlNodeVisitor();
        visitor.visit(node, null);
        
        // content functions should always have the field as the first argument
        if (visitor.namespace().equals(ContentFunctions.CONTENT_FUNCTION_NAMESPACE)) {
            JexlNode first = visitor.args().get(0);
            if (first instanceof ASTIdentifier) {
                String field = JexlASTHelper.deconstructIdentifier((ASTIdentifier) first);
                if (context.has(field) && incompleteFields.contains(field)) {
                    return new State(true, true);
                }
            }
        } else {
            // fall back to descriptor for non-content functions
            JexlArgumentDescriptor descriptor = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(node);
            Set<String> fields = descriptor.fields(null, null);
            
            for (String field : fields) {
                if (context.has(field) && incompleteFields.contains(field)) {
                    return new State(true, true);
                }
            }
        }
        
        // if no determination could be made about an UNKNOWN status, fallback to a normal visit
        Object result = super.visit(node, data);
        if (result instanceof Integer || result instanceof Long) {
            // filter functions like 'getMaxTime' or 'getMinTime'. No incomplete fields exist if we got to this point
            return new State(true, false, new FunctionalSet(), result);
        }
        
        if (result instanceof Collection) {
            boolean isEmpty = ((Collection<?>) result).isEmpty();
            if (hasSiblings(node)) {
                if (isEmpty) {
                    return new State(true, false);
                } else {
                    if (result instanceof FunctionalSet) {
                        return new State(true, false, (FunctionalSet) result);
                    } else {
                        return new State(true, false, new FunctionalSet(), result);
                    }
                }
            } else {
                if (isEmpty) {
                    return new State(true);
                } else {
                    return new State(false);
                }
            }
        }
        return getState(node, result);
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        String nodeString = JexlStringBuildingVisitor.buildQueryWithoutParse(node);
        Object result = resultMap.get(nodeString);
        if (null != result)
            return new State(true); // TODO -- this might be from an unknown field
            
        Object left = node.jjtGetChild(0).jjtAccept(this, data);
        Object right = node.jjtGetChild(1).jjtAccept(this, data);
        try {
            // we could have an expression like '1+1+1 == 3'
            if (left instanceof State) {
                State state = (State) left;
                if (state.hasNumeric()) {
                    left = state.getNumeric();
                }
            }
            
            result = arithmetic.equals(left, right) ? Boolean.TRUE : Boolean.FALSE;
        } catch (ArithmeticException xrt) {
            throw new JexlException(node, "== error", xrt);
        }
        
        resultMap.put(nodeString, result);
        return getState(node, result);
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        Object match = super.visit(node, data);
        return getState(node, match);
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
                
                if (result.matched && !result.fromUnknown) {
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
                leftState = new State(true, false, (FunctionalSet) left);
            }
        }
        
        if (right != null) {
            if (right instanceof State) {
                rightState = (State) right;
            } else if (right instanceof FunctionalSet) {
                rightState = new State(true, false, (FunctionalSet) right);
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
            // for example, filter:notNull(NULL1 || NULL2). return false with an initialized empty functional set
            return new State(true);
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
                if (!state.matched) {
                    return state;
                } else {
                    if (state.fromUnknown) {
                        anyFromUnknown = true;
                    }
                    if (state.set != null && !state.set.isEmpty()) {
                        functionalSet.addAll(state.set);
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
        Object left = node.jjtGetChild(0).jjtAccept(this, data);
        Object right = node.jjtGetChild(1).jjtAccept(this, data);
        try {
            State state = null;
            if (left instanceof State) {
                state = (State) left;
                if (state.fromUnknown) {
                    return state;
                } else if (state.hasNumeric()) {
                    left = state.getNumeric();
                } else if (state.getSet() != null) {
                    left = state.getSet().size();
                }
            }
            if (right instanceof State) {
                state = (State) right;
                if (state.fromUnknown) {
                    return state;
                } else if (state.hasNumeric()) {
                    right = state.getNumeric();
                } else if (state.getSet() != null) {
                    right = state.getSet().size();
                }
            }
            
            if (arithmetic.greaterThanOrEqual(left, right)) {
                return state != null ? state : new State(true);
            } else {
                return new State(false);
            }
        } catch (ArithmeticException xrt) {
            throw new JexlException(node, ">= error", xrt);
        }
    }
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        Object left = node.jjtGetChild(0).jjtAccept(this, data);
        Object right = node.jjtGetChild(1).jjtAccept(this, data);
        try {
            State state = null;
            if (left instanceof State) {
                state = (State) left;
                if (state.fromUnknown) {
                    return state;
                } else if (state.hasNumeric()) {
                    left = state.getNumeric();
                } else if (state.getSet() != null) {
                    left = state.getSet().size();
                }
            }
            if (right instanceof State) {
                state = (State) right;
                if (state.fromUnknown) {
                    return state;
                } else if (state.hasNumeric()) {
                    right = state.getNumeric();
                } else if (state.getSet() != null) {
                    right = state.getSet().size();
                }
            }
            
            if (arithmetic.greaterThan(left, right)) {
                if (state != null) {
                    return state;
                } else {
                    return new State(true);
                }
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
        Object left = node.jjtGetChild(0).jjtAccept(this, data);
        Object right = node.jjtGetChild(1).jjtAccept(this, data);
        try {
            State state = null;
            if (left instanceof State) {
                state = (State) left;
                if (state.fromUnknown) {
                    return state;
                } else if (state.hasNumeric()) {
                    left = state.getNumeric();
                } else if (state.getSet() != null) {
                    left = state.getSet().size();
                }
            }
            if (right instanceof State) {
                state = (State) right;
                if (state.fromUnknown) {
                    return state;
                } else if (state.hasNumeric()) {
                    right = state.getNumeric();
                } else if (state.getSet() != null) {
                    right = state.getSet().size();
                }
            }
            
            if (arithmetic.lessThanOrEqual(left, right)) {
                if (state != null) {
                    return state;
                } else {
                    return new State(true);
                }
            } else {
                return new State(false);
            }
        } catch (ArithmeticException xrt) {
            throw new JexlException(node, "> error", xrt);
        }
    }
    
    @Override
    public Object visit(ASTLTNode node, Object data) {
        Object left = node.jjtGetChild(0).jjtAccept(this, data);
        Object right = node.jjtGetChild(1).jjtAccept(this, data);
        try {
            State state = null;
            if (left instanceof State) {
                state = (State) left;
                if (state.fromUnknown) {
                    return state;
                } else if (state.hasNumeric()) {
                    left = state.getNumeric();
                } else if (state.getSet() != null) {
                    left = state.getSet().size();
                }
            }
            if (right instanceof State) {
                state = (State) right;
                if (state.fromUnknown) {
                    return state;
                } else if (state.hasNumeric()) {
                    right = state.getNumeric();
                } else if (state.getSet() != null) {
                    right = state.getSet().size();
                }
            }
            
            if (arithmetic.lessThan(left, right)) {
                if (state != null) {
                    return state;
                } else {
                    return new State(true);
                }
            } else {
                return new State(false);
            }
        } catch (ArithmeticException xrt) {
            throw new JexlException(node, "> error", xrt);
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
        if (state.fromUnknown && state.matched) {
            // persist "matched from unknown" as-is
            return state;
        } else {
            // otherwise, flip the sign and return
            state.matched = !state.matched;
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
        if (data instanceof FunctionalSet) {
            boolean fromIncomplete = isFunctionalSetIncomplete(incompleteFields, (FunctionalSet) data);
            data = new State(true, fromIncomplete, (FunctionalSet) data);
        }
        
        if (data instanceof State) {
            // need to clear the functional set otherwise functions like filter:getAllMatches(FIELD).size() will
            // pass the functional set to an additive node.
            State state = (State) data;
            state.numeric = state.set.size();
            state.set = new FunctionalSet();
            return state;
        }
        
        return super.visit(node, data);
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
        
        if (leftState.fromUnknown) {
            return leftState;
        }
        
        Object result;
        for (int c = 2, size = node.jjtGetNumChildren(); c < size; c += 2) {
            Object right = node.jjtGetChild(c).jjtAccept(this, data);
            State rightState = getState(node.jjtGetChild(c), right);
            
            // if the left result object is an instance of a state
            if (left instanceof State) {
                State tmp = (State) left;
                switch (tmp.set.size()) {
                    case 0:
                        left = tmp.getNumeric();
                        break;
                    case 1:
                        left = tmp.getSet().iterator().next();
                        break;
                    default:
                        throw new IllegalStateException("Could not coalesce State to numeric for additive op");
                }
            }
            
            if (right instanceof State) {
                State tmp = (State) right;
                switch (tmp.set.size()) {
                    case 0:
                        right = tmp.getNumeric();
                        break;
                    case 1:
                        right = tmp.getSet().iterator().next();
                        break;
                    default:
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
        
        // may call getState() on left or just set the numerical result
        leftState.numeric = left;
        return leftState;
    }
    
    public Object visit(ASTMethodNode node, Object data) {
        if (data instanceof State) {
            State state = (State) data;
            if (state.getSet() != null) {
                return super.visit(node, state.getSet());
            }
        } else if (data instanceof FunctionalSet) {
            if (isFunctionalSetIncomplete(incompleteFields, (FunctionalSet) data)) {
                return new State(true, true, (FunctionalSet) data);
            }
        }
        
        if (data == null) {
            return super.visit(node, FunctionalSet.emptySet());
        } else {
            return super.visit(node, data);
        }
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
        boolean fromUnknown = isIncomplete(node);
        boolean matched = isMatched(scriptExecuteResult) || scriptExecuteResult instanceof ValueTuple || fromUnknown;
        
        if (scriptExecuteResult instanceof ValueTuple) {
            return new State(true, fromUnknown, new FunctionalSet(Collections.singleton(scriptExecuteResult)));
        } else if (scriptExecuteResult instanceof Integer || scriptExecuteResult instanceof Long) {
            // if we got a numeric script result this could be the result of ASTMult or ASTDiv node. In this case we
            // did match and must persist the result as a numeric
            return new State(true, fromUnknown, new FunctionalSet(), scriptExecuteResult);
        }
        return new State(matched, fromUnknown);
    }
    
    /**
     * Used to track matched state on a per-node basis.
     */
    public class State {
        boolean matched;
        boolean fromUnknown;
        FunctionalSet set;
        Object numeric;
        
        public State(boolean matched) {
            this(matched, false, new FunctionalSet(), null);
        }
        
        public State(boolean matched, boolean fromUnknown) {
            this(matched, fromUnknown, new FunctionalSet(), null);
        }
        
        public State(boolean matched, boolean fromUnknown, FunctionalSet set) {
            this(matched, fromUnknown, set, null);
        }
        
        public State(boolean matched, boolean fromUnknown, FunctionalSet set, Object numeric) {
            this.matched = matched;
            this.fromUnknown = fromUnknown;
            this.set = set;
            this.numeric = numeric;
        }
        
        /**
         * Merge this state with another state with union rules
         *
         * @param other
         *            another State
         */
        public void mergeOr(State other) {
            if (this.matched) {
                if (other.matched) {
                    // both match
                    if ((this.fromUnknown && !other.fromUnknown) || (!this.fromUnknown && other.fromUnknown)) {
                        // if both match but only one is from an unknown field
                        this.fromUnknown = false;
                    }
                }
            } else if (other.matched) {
                this.matched = other.matched;
                this.fromUnknown = other.fromUnknown;
            }
            
            if (this.set != null && other.set != null) {
                this.set.addAll(other.set);
            } else if (this.set == null && other.set != null) {
                this.set = other.set;
            }
        }
        
        /**
         * Determine if the numeric field is indeed a numeric
         *
         * @return true if a number otherwise false
         */
        public boolean hasNumeric() {
            return numeric != null
                            && (numeric instanceof Integer || numeric instanceof Long || numeric instanceof Float || numeric instanceof Double
                                            || numeric instanceof BigDecimal || numeric instanceof BigInteger);
        }
        
        public Object getNumeric() {
            return this.numeric;
        }
        
        public FunctionalSet getSet() {
            return this.set;
        }
    }
}
