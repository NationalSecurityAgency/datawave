package datawave.query.jexl;

import com.google.common.collect.Maps;
import datawave.core.iterators.DatawaveFieldIndexListIteratorJexl;
import datawave.query.attributes.ValueTuple;
import datawave.query.collections.FunctionalSet;
import datawave.query.jexl.functions.QueryFunctions;
import datawave.query.jexl.nodes.ExceededOrThresholdMarkerJexlNode;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl2.Interpreter;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.JexlException;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTMethodNode;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.ASTSizeMethod;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.lucene.util.fst.FST;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

/**
 * Extended so that calls to a function node, which can return a collection of 'hits' instead of a Boolean, can be evaluated as true/false based on the size of
 * the hit collection. Also, if the member 'arithmetic' has been set to a HitListArithmetic, then the returned hits will be added to the hitSet of the
 * arithmetic.
 *
 * Also added in the ability to count attributes pulled from the ValueTuples which contribute to the positive evaluation.
 */
public class DatawaveInterpreter extends Interpreter {
    
    protected Map<String,Object> resultMap;
    
    private static final Logger log = Logger.getLogger(DatawaveInterpreter.class);
    
    public DatawaveInterpreter(JexlEngine jexl, JexlContext aContext, boolean strictFlag, boolean silentFlag) {
        super(jexl, aContext, strictFlag, silentFlag);
        resultMap = Maps.newHashMap();
    }
    
    /**
     * This convenience method can be used to interpret the result of the script.execute() result which calls the interpret method below.
     * 
     * @param scriptExecuteResult
     * @return true if we matched, false otherwise.
     */
    public static boolean isMatched(Object scriptExecuteResult) {
        boolean matched = false;
        if (scriptExecuteResult != null && Boolean.class.isAssignableFrom(scriptExecuteResult.getClass())) {
            matched = (Boolean) scriptExecuteResult;
        } else if (scriptExecuteResult != null && Collection.class.isAssignableFrom(scriptExecuteResult.getClass())) {
            // if the function returns a collection of matches, return true/false
            // based on the number of matches
            Collection<?> matches = (Collection<?>) scriptExecuteResult;
            matched = (!matches.isEmpty());
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Unable to process non-Boolean result from JEXL evaluation '" + scriptExecuteResult);
            }
        }
        return matched;
    }
    
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        String nodeString = JexlStringBuildingVisitor.buildQueryWithoutParse(node);
        
        Object result = resultMap.get(nodeString);
        if (null != result) {
            return result;
        }
        
        result = super.visit(node, data);
        
        addHits(result);
        
        // if the function stands alone, then it needs to return ag boolean
        // if the function is paired with a method that is called on its results (like 'size') then the
        // actual results must be returned.
        if (hasSiblings(node)) {
            resultMap.put(nodeString, result);
            return result;
        }
        resultMap.put(nodeString, result instanceof Collection ? !((Collection) result).isEmpty() : result);
        return result instanceof Collection ? !((Collection) result).isEmpty() : result;
    }
    
    /**
     * Triggered when variable can not be resolved.
     * 
     * @param xjexl
     *            the JexlException ("undefined variable " + variable)
     * @return throws JexlException if strict, null otherwise
     */
    @Override
    protected Object unknownVariable(JexlException xjexl) {
        if (strict) {
            throw xjexl;
        }
        // do not warn
        return null;
    }
    
    /**
     * Triggered when method, function or constructor invocation fails.
     * 
     * @param xjexl
     *            the JexlException wrapping the original error
     * @return throws JexlException
     */
    @Override
    protected Object invocationFailed(JexlException xjexl) {
        throw xjexl;
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        String nodeString = JexlStringBuildingVisitor.buildQueryWithoutParse(node);
        
        Object result = resultMap.get(nodeString);
        if (null != result)
            return result;
        result = super.visit(node, data);
        resultMap.put(nodeString, result);
        return result;
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        String nodeString = JexlStringBuildingVisitor.buildQueryWithoutParse(node);
        
        Object result = resultMap.get(nodeString);
        if (null != result)
            return result;
        result = super.visit(node, data);
        resultMap.put(nodeString, result);
        return result;
    }
    
    /**
     * unused because hasSiblings should cover every case with the size method, plus other methods
     * 
     * @param node
     * @return
     */
    private boolean hasSizeMethodSibling(ASTFunctionNode node) {
        JexlNode parent = node.jjtGetParent();
        for (int i = 0; i < parent.jjtGetNumChildren(); i++) {
            JexlNode kid = parent.jjtGetChild(i);
            if (kid instanceof ASTSizeMethod) {
                return true;
            }
        }
        return false;
    }
    
    public Object visit(ASTMethodNode node, Object data) {
        if (data == null) {
            data = new FunctionalSet(); // an empty set
        }
        return super.visit(node, data);
    }
    
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
        
        // If all ASTIdentifiers, then traverse the whole queue. Otherwise we can attempt to short circuit.
        Object result = null;
        if (allIdentifiers) {
            // Likely within a function and must visit every child in the stack.
            // Failure to do so will short circuit value aggregation leading to incorrect function evaluation.
            while (!children.isEmpty()) {
                // Child nodes were put onto the stack left to right. PollLast to evaluate left to right.
                result = interpretOr(children.pollLast().jjtAccept(this, data), result);
            }
        } else {
            // We are likely within a normal union and can short circuit
            while (!arithmetic.toBoolean(result) && !children.isEmpty()) {
                // Child nodes were put onto the stack left to right. PollLast to evaluate left to right.
                result = interpretOr(children.pollLast().jjtAccept(this, data), result);
            }
        }
        
        return result;
    }
    
    public Object interpretOr(Object left, Object right) {
        FunctionalSet leftFunctionalSet = null;
        FunctionalSet rightFunctionalSet = null;
        if (left == null)
            left = FunctionalSet.empty();
        if (!(left instanceof Collection)) {
            try {
                boolean leftValue = arithmetic.toBoolean(left);
                if (leftValue) {
                    return Boolean.TRUE;
                }
            } catch (ArithmeticException xrt) {
                throw new RuntimeException(left.toString() + " boolean coercion error", xrt);
            }
        } else {
            leftFunctionalSet = new FunctionalSet();
            leftFunctionalSet.addAll((Collection) left);
        }
        if (right == null)
            right = FunctionalSet.empty();
        if (!(right instanceof Collection)) {
            try {
                boolean rightValue = arithmetic.toBoolean(right);
                if (rightValue) {
                    return Boolean.TRUE;
                }
            } catch (ArithmeticException xrt) {
                throw new RuntimeException(right.toString() + " boolean coercion error", xrt);
            }
        } else {
            rightFunctionalSet = new FunctionalSet();
            rightFunctionalSet.addAll((Collection) right);
        }
        // when an identifier is expanded by the data model within a Function node, the results of the matches
        // for both (all?) fields must be gathered into a single collection to be returned.
        if (leftFunctionalSet != null && rightFunctionalSet != null) { // add left and right
            FunctionalSet functionalSet = new FunctionalSet(leftFunctionalSet);
            functionalSet.addAll(rightFunctionalSet);
            return functionalSet;
        } else if (leftFunctionalSet != null) {
            return leftFunctionalSet;
        } else if (rightFunctionalSet != null) {
            return rightFunctionalSet;
        } else {
            return getBooleanOr(left, right);
        }
    }
    
    private JexlNode dereference(JexlNode node) {
        while (node.jjtGetNumChildren() == 1 && (node instanceof ASTReferenceExpression || node instanceof ASTReference)) {
            node = node.jjtGetChild(0);
        }
        return node;
    }
    
    /**
     * * This will determine if this ANDNode contains a range, and will invoke the appropriate range function instead of evaluating the LT/LE and GT/GE nodes *
     * independently as that does not work when there are sets of values in the context. * * @param node * @return a collection of hits (or empty set) if we
     * evaluated a range. null otherwise.
     */
    private Collection<?> evaluateRange(ASTAndNode node) {
        Collection<?> evaluation = null;
        
        LiteralRange range = JexlASTHelper.findRange().getRange(node);
        if (range != null) {
            JexlNode left = range.getLowerNode();
            JexlNode right = range.getUpperNode();
            if (left instanceof ASTLENode || left instanceof ASTLTNode) {
                JexlNode temp = left;
                left = right;
                right = temp;
            }
            if ((left instanceof ASTGENode || left instanceof ASTGTNode) && (right instanceof ASTLENode || right instanceof ASTLTNode)) {
                JexlNode leftIdentifier = dereference(left.jjtGetChild(0));
                JexlNode rightIdentifier = dereference(right.jjtGetChild(0));
                if (leftIdentifier instanceof ASTIdentifier && rightIdentifier instanceof ASTIdentifier) {
                    String fieldName = leftIdentifier.image;
                    if (fieldName.equals(rightIdentifier.image)) {
                        Object fieldValue = leftIdentifier.jjtAccept(this, null);
                        Object leftValue = left.jjtGetChild(1).jjtAccept(this, null);
                        boolean leftInclusive = left instanceof ASTGENode;
                        Object rightValue = right.jjtGetChild(1).jjtAccept(this, null);
                        boolean rightInclusive = right instanceof ASTLENode;
                        if (leftValue instanceof Number && rightValue instanceof Number) {
                            if (fieldValue instanceof Collection) {
                                evaluation = QueryFunctions.between((Collection) fieldValue, ((Number) leftValue).floatValue(), leftInclusive,
                                                ((Number) rightValue).floatValue(), rightInclusive);
                            } else {
                                evaluation = QueryFunctions.between(fieldValue, ((Number) leftValue).floatValue(), leftInclusive,
                                                ((Number) rightValue).floatValue(), rightInclusive);
                            }
                        } else {
                            if (fieldValue instanceof Collection) {
                                evaluation = QueryFunctions.between((Collection) fieldValue, String.valueOf(leftValue), leftInclusive,
                                                String.valueOf(rightValue), rightInclusive);
                            } else {
                                evaluation = QueryFunctions.between(fieldValue, String.valueOf(leftValue), leftInclusive, String.valueOf(rightValue),
                                                rightInclusive);
                            }
                        }
                        addHits(fieldValue);
                    }
                }
            }
        }
        return evaluation;
    }
    
    private void addHits(Object fieldValue) {
        if (this.arithmetic instanceof HitListArithmetic && fieldValue != null) {
            HitListArithmetic hitListArithmetic = (HitListArithmetic) arithmetic;
            if (fieldValue instanceof Collection<?>) {
                for (Object o : ((Collection<?>) fieldValue)) {
                    addHits(o);
                }
            } else if (fieldValue instanceof ValueTuple) {
                hitListArithmetic.add((ValueTuple) fieldValue);
            }
        }
    }
    
    public Object visit(ASTAndNode node, Object data) {
        // we could have arrived here after the node was dereferenced
        if (ExceededOrThresholdMarkerJexlNode.instanceOf(node)) {
            return visitExceededOrThresholdMarker(node);
        }
        
        // check for the special case of a range (conjunction of a G/GE and a L/LE node) and reinterpret as a function
        Object evaluation = evaluateRange(node);
        if (evaluation != null) {
            return evaluation;
        }
        
        FunctionalSet leftFunctionalSet = null;
        FunctionalSet rightFunctionalSet = null;
        Object left = node.jjtGetChild(0).jjtAccept(this, data);
        if (left == null)
            left = FunctionalSet.empty();
        if (left instanceof Collection == false) {
            try {
                boolean leftValue = arithmetic.toBoolean(left);
                if (!leftValue) {
                    return Boolean.FALSE;
                }
            } catch (RuntimeException xrt) {
                throw new JexlException(node.jjtGetChild(0), "boolean coercion error", xrt);
            }
        } else {
            if (leftFunctionalSet == null)
                leftFunctionalSet = new FunctionalSet();
            leftFunctionalSet.addAll((Collection) left);
        }
        Object right = node.jjtGetChild(1).jjtAccept(this, data);
        if (right == null)
            right = FunctionalSet.empty();
        if (right instanceof Collection == false) {
            try {
                boolean rightValue = arithmetic.toBoolean(right);
                if (!rightValue) {
                    return Boolean.FALSE;
                }
            } catch (ArithmeticException xrt) {
                throw new JexlException(node.jjtGetChild(1), "boolean coercion error", xrt);
            }
        } else {
            if (rightFunctionalSet == null)
                rightFunctionalSet = new FunctionalSet();
            rightFunctionalSet.addAll((Collection) right);
        }
        // return union of left and right iff they are both non-null & non-empty
        if (leftFunctionalSet != null && rightFunctionalSet != null) {
            if (!leftFunctionalSet.isEmpty() && !rightFunctionalSet.isEmpty()) {
                FunctionalSet functionalSet = new FunctionalSet(leftFunctionalSet);
                functionalSet.addAll(rightFunctionalSet);
                return functionalSet;
            } else {
                return Boolean.FALSE;
            }
        } else {
            return getBooleanAnd(left, right);
        }
    }
    
    /** {@inheritDoc} */
    public Object visit(ASTSizeMethod node, Object data) {
        if (data != null) {
            return super.visit(node, data);
        } else {
            return Integer.valueOf(0);
        }
    }
    
    // this handles the case where one side is a boolean and the other is a collection
    private boolean getBooleanAnd(Object left, Object right) {
        if (left instanceof Collection) {
            left = ((Collection) left).isEmpty() == false;
        }
        if (right instanceof Collection) {
            right = ((Collection) right).isEmpty() == false;
        }
        return arithmetic.toBoolean(left) && arithmetic.toBoolean(right);
    }
    
    private boolean getBooleanOr(Object left, Object right) {
        if (left instanceof Collection) {
            left = ((Collection) left).isEmpty() == false;
        }
        if (right instanceof Collection) {
            right = ((Collection) right).isEmpty() == false;
        }
        return arithmetic.toBoolean(left) || arithmetic.toBoolean(right);
    }
    
    /**
     * a function node that has siblings has a method paired with it, like the size method in includeRegex(foo,bar).size() It must return its collection of
     * results for the other method to use, instead of a boolean indicating that there were results
     * 
     * @param node
     * @return
     */
    private boolean hasSiblings(ASTFunctionNode node) {
        
        JexlNode parent = node.jjtGetParent();
        
        if (parent.jjtGetNumChildren() > 1) {
            return true;
        }
        
        JexlNode grandparent = parent.jjtGetParent();
        
        if (grandparent instanceof ASTMethodNode) {
            return true;
        }
        return false;
    }
    
    @Override
    public Object visit(ASTReference node, Object data) {
        if (ExceededOrThresholdMarkerJexlNode.instanceOf(node)) {
            return visitExceededOrThresholdMarker(node);
        } else {
            return super.visit(node, data);
        }
    }
    
    private Object visitExceededOrThresholdMarker(JexlNode node) {
        String id = ExceededOrThresholdMarkerJexlNode.getId(node);
        String field = ExceededOrThresholdMarkerJexlNode.getField(node);
        
        Set<String> evalValues = null;
        FST evalFst = null;
        SortedSet<Range> evalRanges = null;
        
        // if the context isn't cached, load it now
        if (!getContext().has(id)) {
            try {
                ExceededOrThresholdMarkerJexlNode.ExceededOrParams params = ExceededOrThresholdMarkerJexlNode.getParameters(node);
                if (params != null) {
                    if (params.getRanges() != null && !params.getRanges().isEmpty()) {
                        getContext().set(id, params.getSortedAccumuloRanges());
                    } else if (params.getValues() != null && !params.getValues().isEmpty()) {
                        getContext().set(id, params.getValues());
                    } else if (params.getFstURI() != null) {
                        getContext().set(id, DatawaveFieldIndexListIteratorJexl.FSTManager.get(new Path(new URI(params.getFstURI()))));
                    }
                }
            } catch (IOException | URISyntaxException e) {
                log.warn("Unable to load ExceededOrThreshold Parameters during evaluation", e);
            }
        }
        
        // determine what we're dealing with
        Object contextObj = getContext().get(id);
        if (contextObj instanceof FST) {
            evalFst = (FST) contextObj;
        } else if (contextObj instanceof Set) {
            Iterator iter = ((Set) contextObj).iterator();
            if (iter.hasNext()) {
                Object element = iter.next();
                if (element instanceof Range)
                    evalRanges = (SortedSet<Range>) contextObj;
                else if (element instanceof String)
                    evalValues = (Set<String>) contextObj;
            }
        }
        
        // get all of the values for this field from the context
        Collection<?> contextValues;
        Object fieldValue = getContext().get(field);
        if (!(fieldValue instanceof Collection)) {
            contextValues = Collections.singletonList(fieldValue);
        } else {
            contextValues = (Collection<?>) fieldValue;
        }
        
        Set evaluation = new HashSet<>();
        
        // check for value matches
        if (evalValues != null && !evalValues.isEmpty()) {
            for (Object contextValue : contextValues) {
                for (String evalValue : evalValues) {
                    if (arithmetic.equals(contextValue, evalValue)) {
                        evaluation.add(contextValue);
                        break;
                    }
                }
            }
        }
        
        // check for FST matches
        else if (evalFst != null && arithmetic instanceof DatawaveArithmetic) {
            for (Object contextValue : contextValues) {
                if (((DatawaveArithmetic) arithmetic).fstMatch(evalFst, contextValue)) {
                    evaluation.add(contextValue);
                    break;
                }
            }
        }
        
        // check for range matches
        else if (evalRanges != null && !evalRanges.isEmpty()) {
            for (Object contextValue : contextValues) {
                for (Range evalRange : evalRanges) {
                    if ((evalRange.isStartKeyInclusive() ? arithmetic.greaterThanOrEqual(contextValue, evalRange.getStartKey().getRow().toString())
                                    : arithmetic.greaterThan(contextValue, evalRange.getStartKey().getRow().toString()))
                                    && (evalRange.isEndKeyInclusive() ? arithmetic.lessThanOrEqual(contextValue, evalRange.getEndKey().getRow().toString())
                                                    : arithmetic.lessThan(contextValue, evalRange.getEndKey().getRow().toString()))) {
                        evaluation.add(contextValue);
                        break;
                    }
                }
            }
        }
        
        if (evaluation.isEmpty())
            return Boolean.FALSE;
        
        addHits(evaluation);
        
        return evaluation;
    }
}
