package datawave.query.jexl;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.DROPPED;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_OR;

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

import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.JexlOptions;
import org.apache.commons.jexl3.internal.Engine;
import org.apache.commons.jexl3.internal.Frame;
import org.apache.commons.jexl3.internal.Interpreter;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTMethodNode;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.lucene.util.fst.FST;

import com.google.common.collect.Maps;

import datawave.core.iterators.DatawaveFieldIndexListIteratorJexl;
import datawave.marking.MarkingFunctions;
import datawave.marking.MarkingFunctionsFactory;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.ValueTuple;
import datawave.query.collections.FunctionalSet;
import datawave.query.jexl.functions.ContentFunctionsDescriptor;
import datawave.query.jexl.functions.QueryFunctions;
import datawave.query.jexl.nodes.ExceededOr;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;

/**
 * Extended so that calls to a function node, which can return a collection of 'hits' instead of a Boolean, can be evaluated as true/false based on the size of
 * the hit collection. Also, if the member 'arithmetic' has been set to a HitListArithmetic, then the returned hits will be added to the hitSet of the
 * arithmetic.
 *
 * Also added in the ability to count attributes pulled from the ValueTuples which contribute to the positive evaluation.
 *
 * In general visiting an expression can return true, false, a functional set, or null. The boolean value of an expression is true if true or a non-empty
 * functional set is returned. The boolean value of an expression is false if false or an empty functional set is returned. Return null means that the
 * (sub)expression will not contribute to the boolean value of the overall expression.
 *
 */
public class DatawaveInterpreter extends Interpreter {

    protected Map<String,Object> resultMap;

    private static final Logger log = Logger.getLogger(DatawaveInterpreter.class);

    public DatawaveInterpreter(Engine jexl, JexlOptions opts, JexlContext aContext, Frame eFrame) {
        super(jexl, opts, aContext, eFrame);
        resultMap = Maps.newHashMap();
    }

    /**
     * This convenience method can be used to interpret the result of the script.execute() result which calls the interpret method below.
     *
     * @param scriptExecuteResult
     *            the script result
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

        boolean isPhraseFunc = (nodeString.startsWith("content:phrase") || nodeString.startsWith("content:scoredPhrase"));
        // special handling for phrase functions and HIT_TERMs
        if (isPhraseFunc) {
            addHitsForFunction(result, node);
        }

        // If a content:phrase returned a collection translate that to a true or a false
        if (isPhraseFunc && result instanceof Collection) {
            Collection<String> hitFields = (Collection<String>) result;
            result = hitFields.isEmpty() ? Boolean.FALSE : Boolean.TRUE;
        }

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
     * @param node
     *            the node where the error originated from
     * @param var
     *            the variable name
     * @param undef
     *            whether the variable is undefined or null
     * @return
     */
    @Override
    protected Object unsolvableVariable(JexlNode node, String var, boolean undef) {
        if (options.isStrict()) {
            throw new JexlException.Variable(node, var, undef);
        }
        // do not warn
        return null;
    }

    /**
     * Triggered when method, function or constructor invocation fails.
     *
     * @param node
     *            the node triggering the exception
     * @param methodName
     *            the method/function name
     * @param xany
     *            the cause
     * @return
     */
    @Override
    protected JexlException invocationException(JexlNode node, String methodName, Throwable xany) {
        if (xany instanceof JexlException) {
            throw (JexlException) xany;
        }
        throw super.invocationException(node, methodName, xany);
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

    public Object visit(ASTIdentifier node, Object data) {
        Object result = super.visit(node, data);
        if (result == null && hasMethodSibling(node)) {
            result = new FunctionalSet(); // an empty set
        }
        return result;
    }

    // check to see if this node's next (right-most) sibling is an ASTMethodNode
    public boolean hasMethodSibling(JexlNode node) {
        boolean methodFound = false;
        if (node.jjtGetParent() != null && node.jjtGetParent().jjtGetNumChildren() > 1) {
            JexlNode parent = node.jjtGetParent();
            boolean foundSelf = false;
            for (int i = 0; i < parent.jjtGetNumChildren(); i++) {
                if (foundSelf) {
                    methodFound = parent.jjtGetChild(i) instanceof ASTMethodNode;
                    break;
                } else {
                    foundSelf = parent.jjtGetChild(i) == node;
                }
            }
        }
        return methodFound;
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
            while ((result == null || !arithmetic.toBoolean(result)) && !children.isEmpty()) {
                // Child nodes were put onto the stack left to right. PollLast to evaluate left to right.
                result = interpretOr(children.pollLast().jjtAccept(this, data), result);
            }
        }

        return result;
    }

    /**
     * Determine the result of or'ing left with right. null means no result, empty functional set is essentially a false, otherwise true.
     *
     * @param left
     *            left object
     * @param right
     *            right object
     * @return null of a functional set
     */
    protected Object interpretOr(Object left, Object right) {
        FunctionalSet leftFunctionalSet = getFunctionalSet(left);
        FunctionalSet rightFunctionalSet = getFunctionalSet(right);

        // when an identifier is expanded by the data model within a Function node, the results of the matches
        // for both fields must be gathered into a single collection to be returned.
        if (leftFunctionalSet != null && rightFunctionalSet != null) { // add left and right
            FunctionalSet functionalSet = new FunctionalSet(leftFunctionalSet);
            functionalSet.addAll(rightFunctionalSet);
            return functionalSet;
        } else if (leftFunctionalSet != null) {
            return leftFunctionalSet;
        } else {
            return rightFunctionalSet;
        }
    }

    /**
     * Turn a value into a functional set
     *
     * @param value
     *            a value
     * @return a functional set containing the value. null if value is null.
     */
    protected FunctionalSet getFunctionalSet(Object value) {
        FunctionalSet functionalSet = null;
        // use value if already a functional set
        if (value instanceof FunctionalSet) {
            functionalSet = (FunctionalSet) value;
        }
        // if a collection, then create a functional set out of that
        else if (value instanceof Collection) {
            functionalSet = new FunctionalSet((Collection) value);
        }
        // otherwise create a functional set from the value iff true
        // ignore null (e.g. assignments should not affect results)
        else if (value != null) {
            functionalSet = new FunctionalSet();
            if (arithmetic.toBoolean(value)) {
                functionalSet.add(getTuple(value));
            }
        }
        return functionalSet;
    }

    /**
     * Create a tuple from a value
     *
     * @param o
     *            an object
     * @return a tuple containing value
     */
    protected ValueTuple getTuple(Object o) {
        if (o instanceof ValueTuple) {
            return (ValueTuple) o;
        }
        return new ValueTuple("_unknown_", o, o, null);
    }

    private JexlNode dereference(JexlNode node) {
        while (node.jjtGetNumChildren() == 1 && node instanceof ASTReferenceExpression) {
            node = node.jjtGetChild(0);
        }
        return node;
    }

    /**
     * This will determine if this ANDNode contains a range, and will invoke the appropriate range function instead of evaluating the LT/LE and GT/GE nodes *
     * independently as that does not work when there are sets of values in the context.
     *
     * @param node
     *            a node
     * @return a collection of hits (or empty set) if we evaluated a range. null otherwise.
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
                    String fieldName = ((ASTIdentifier) leftIdentifier).getName();
                    if (fieldName.equals(((ASTIdentifier) rightIdentifier).getName())) {
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

    /**
     * Wrapper method for adding fielded phrases to the HIT_TERM
     *
     * @param o
     *            a collection of fields that hit for this function
     * @param node
     *            an ASTFunctionNode
     */
    private void addHitsForFunction(Object o, ASTFunctionNode node) {
        if (this.arithmetic instanceof HitListArithmetic && o != null) {
            HitListArithmetic hitListArithmetic = (HitListArithmetic) arithmetic;
            if (o instanceof Set<?>) {
                Set<String> hitFields = (Set<String>) o;
                for (String hitField : hitFields) {
                    addHitsForFunction(hitField, node, hitListArithmetic);
                }
            }
        }
    }

    /**
     * Add a fielded phrase to the HIT_TERM
     *
     * @param field
     *            the phrase function hit on this field
     * @param node
     *            an ASTFunctionNode
     * @param hitListArithmetic
     *            a JexlArithmetic that supports hit lists
     */
    private void addHitsForFunction(String field, ASTFunctionNode node, HitListArithmetic hitListArithmetic) {
        ColumnVisibility cv;
        // aggregate individual hits for the content function
        Collection<ColumnVisibility> cvs = new HashSet<>();
        Attributes source = new Attributes(true);
        ContentFunctionsDescriptor.ContentJexlArgumentDescriptor jexlArgDescriptor = new ContentFunctionsDescriptor().getArgumentDescriptor(node);

        Set<String> values = jexlArgDescriptor.getHitTermValues();
        FunctionalSet<?> set = (FunctionalSet<?>) this.context.get(field);
        if (set != null) {
            for (ValueTuple tuple : set) {
                if (values.contains(tuple.getNormalizedValue())) {
                    Attribute<?> attr = tuple.getSource();
                    source.add(attr);
                    cvs.add(attr.getColumnVisibility());
                }
            }
        }

        try {
            cv = MarkingFunctionsFactory.createMarkingFunctions().combine(cvs);
        } catch (MarkingFunctions.Exception e) {
            log.error("Failed to combine column visibilities while generating HIT_TERM for phrase function for field [" + field + "]");
            log.error("msg: ", e);
            return;
        }
        source.setColumnVisibility(cv);

        // create an Attributes<?> backed ValueTuple
        String phrase = jexlArgDescriptor.getHitTermValue();

        ValueTuple vt = new ValueTuple(field, phrase, phrase, source);
        hitListArithmetic.add(vt);
    }

    public Object visit(ASTAndNode node, Object data) {

        QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(node);
        // we could have arrived here after the node was dereferenced
        if (instance.isType(EXCEEDED_OR)) {
            return visitExceededOrThresholdMarker(node);
        } else if (instance.isType(DROPPED)) {
            return null;
        }

        // check for the special case of a range (conjunction of a G/GE and a L/LE node) and reinterpret as a function
        Object evaluation = evaluateRange(node);
        if (evaluation != null) {
            return evaluation;
        }

        // holds all values for intersection
        FunctionalSet functionalSet = null;
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode child = node.jjtGetChild(i);
            Object o = child.jjtAccept(this, data);
            // null return means there was no actual evaluation
            if (o == null) {
                continue;
            }
            if (functionalSet == null) {
                functionalSet = new FunctionalSet();
            }
            if (o instanceof Collection) {
                if (((Collection) o).isEmpty()) {
                    return Boolean.FALSE;
                }
                functionalSet.addAll((Collection<?>) o);
            } else {
                try {
                    boolean value = arithmetic.toBoolean(o);
                    if (value) {
                        functionalSet.add(getTuple(o));
                    } else {
                        // we got a false value, so return false
                        return Boolean.FALSE;
                    }
                } catch (RuntimeException xrt) {
                    throw new JexlException(child, "boolean coercion error", xrt);
                }
            }
        }

        return functionalSet;
    }

    /**
     * a function node that has siblings has a method paired with it, like the size method in includeRegex(foo,bar).size() It must return its collection of
     * results for the other method to use, instead of a boolean indicating that there were results
     *
     * @param node
     *            a node
     * @return if the node has siblings
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

    private Object visitExceededOrThresholdMarker(JexlNode node) {
        ExceededOr exceededOr = new ExceededOr(node);

        Set<String> evalValues = null;
        FST evalFst = null;
        SortedSet<Range> evalRanges = null;

        // if the context isn't cached, load it now
        if (!context.has(exceededOr.getId())) {
            try {
                if (exceededOr.getParams() != null) {
                    if (exceededOr.getParams().getRanges() != null && !exceededOr.getParams().getRanges().isEmpty()) {
                        context.set(exceededOr.getId(), exceededOr.getParams().getSortedAccumuloRanges());
                    } else if (exceededOr.getParams().getValues() != null && !exceededOr.getParams().getValues().isEmpty()) {
                        context.set(exceededOr.getId(), exceededOr.getParams().getValues());
                    } else if (exceededOr.getParams().getFstURI() != null) {
                        context.set(exceededOr.getId(),
                                        DatawaveFieldIndexListIteratorJexl.FSTManager.get(new Path(new URI(exceededOr.getParams().getFstURI()))));
                    }
                }
            } catch (IOException | URISyntaxException e) {
                log.warn("Unable to load ExceededOrThreshold Parameters during evaluation", e);
            }
        }

        // determine what we're dealing with
        Object contextObj = context.get(exceededOr.getId());
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
        Object fieldValue = context.get(exceededOr.getField());
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
