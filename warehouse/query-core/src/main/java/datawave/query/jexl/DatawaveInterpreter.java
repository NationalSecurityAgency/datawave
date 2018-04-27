package datawave.query.jexl;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import datawave.query.attributes.ValueTuple;
import datawave.query.jexl.nodes.TreeHashNode;
import datawave.query.jexl.visitors.TreeHashVisitor;
import org.apache.commons.jexl2.Interpreter;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.JexlException;
import org.apache.commons.jexl2.parser.*;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

import datawave.query.collections.FunctionalSet;
import datawave.query.jexl.functions.QueryFunctions;

/**
 * Extended so that calls to a function node, which can return a collection of 'hits' instead of a Boolean, can be evaluated as true/false based on the size of
 * the hit collection. Also, if the member 'arithmetic' has been set to a HitListArithmetic, then the returned hits will be added to the hitSet of the
 * arithmetic.
 *
 * Also added in the ability to count attributes pulled from the ValueTuples which contribute to the positive evaluation.
 */
public class DatawaveInterpreter extends Interpreter {
    
    protected Map<TreeHashNode,Object> resultMap;
    
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
            Boolean result = (Boolean) scriptExecuteResult;
            matched = result;
        } else if (scriptExecuteResult != null && Collection.class.isAssignableFrom(scriptExecuteResult.getClass())) {
            // if the function returns a collection of matches, return true/false
            // based on the number of matches
            Collection<?> matches = (Collection<?>) scriptExecuteResult;
            matched = (matches.size() > 0);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Unable to process non-Boolean result from JEXL evaluation '" + scriptExecuteResult);
            }
        }
        return matched;
    }
    
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        TreeHashNode hash = TreeHashVisitor.getNodeHash(node);
        
        Object result = resultMap.get(hash);
        if (null != result) {
            return result;
        }
        result = super.visit(node, data);
        
        if (this.arithmetic instanceof HitListArithmetic) {
            HitListArithmetic hitListArithmetic = (HitListArithmetic) arithmetic;
            Set<String> hitSet = hitListArithmetic.getHitSet();
            if (hitSet != null && result instanceof Collection<?>) {
                for (Object o : ((Collection<?>) result)) {
                    if (o instanceof ValueTuple) {
                        hitListArithmetic.add((ValueTuple) o);
                    }
                }
            }
        }
        // if the function stands alone, then it needs to return ag boolean
        // if the function is paired with a method that is called on its results (like 'size') then the
        // actual results must be returned.
        if (hasSiblings(node)) {
            resultMap.put(hash, result);
            return result;
        }
        resultMap.put(hash, result instanceof Collection ? ((Collection) result).size() > 0 : result);
        return result instanceof Collection ? ((Collection) result).size() > 0 : result;
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
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        TreeHashNode hash = TreeHashVisitor.getNodeHash(node);
        
        Object result = resultMap.get(hash);
        if (null != result)
            return result;
        result = super.visit(node, data);
        resultMap.put(hash, result);
        return result;
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        TreeHashNode hash = TreeHashVisitor.getNodeHash(node);
        
        Object result = resultMap.get(hash);
        if (null != result)
            return result;
        result = super.visit(node, data);
        resultMap.put(hash, result);
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
        FunctionalSet leftFunctionalSet = null;
        FunctionalSet rightFunctionalSet = null;
        Object left = node.jjtGetChild(0).jjtAccept(this, data);
        if (left == null)
            left = FunctionalSet.empty();
        if (left instanceof Collection == false) {
            try {
                boolean leftValue = arithmetic.toBoolean(left);
                if (leftValue) {
                    return Boolean.TRUE;
                }
            } catch (ArithmeticException xrt) {
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
                if (rightValue) {
                    return Boolean.TRUE;
                }
            } catch (ArithmeticException xrt) {
                throw new JexlException(node.jjtGetChild(1), "boolean coercion error", xrt);
            }
        } else {
            if (rightFunctionalSet == null)
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
     * */
    private Collection<?> evaluateRange(ASTAndNode node) {
        Collection<?> evaluation = null;
        JexlNode left = node.jjtGetChild(0);
        JexlNode right = node.jjtGetChild(1);
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
                            evaluation = QueryFunctions.between((Collection) fieldValue, String.valueOf(leftValue), leftInclusive, String.valueOf(rightValue),
                                            rightInclusive);
                        } else {
                            evaluation = QueryFunctions.between(fieldValue, String.valueOf(leftValue), leftInclusive, String.valueOf(rightValue),
                                            rightInclusive);
                        }
                    }
                    if (this.arithmetic instanceof HitListArithmetic) {
                        HitListArithmetic hitListArithmetic = (HitListArithmetic) arithmetic;
                        Set<String> hitSet = hitListArithmetic.getHitSet();
                        if (hitSet != null) {
                            hitSet.addAll((Collection<String>) evaluation);
                        }
                    }
                }
            }
        }
        return evaluation;
    }
    
    public Object visit(ASTAndNode node, Object data) {
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
}
