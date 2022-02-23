package datawave.query.jexl;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import datawave.query.Constants;
import datawave.query.jexl.visitors.RebuildingVisitor;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.lookups.IndexLookupMap;
import datawave.query.jexl.lookups.ValueSet;
import datawave.query.jexl.nodes.ExceededTermThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededValueThresholdMarkerJexlNode;
import datawave.query.jexl.visitors.RebuildingVisitor;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import org.apache.commons.jexl2.parser.ASTAdditiveNode;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTBitwiseAndNode;
import org.apache.commons.jexl2.parser.ASTBitwiseComplNode;
import org.apache.commons.jexl2.parser.ASTBitwiseOrNode;
import org.apache.commons.jexl2.parser.ASTBitwiseXorNode;
import org.apache.commons.jexl2.parser.ASTDivNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTEmptyFunction;
import org.apache.commons.jexl2.parser.ASTFalseNode;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
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
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.ASTTrueNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.commons.jexl2.parser.ParserTreeConstants;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Factory methods that can create JexlNodes
 * 
 * 
 * 
 */
public class JexlNodeFactory {
    public static final Set<Class<?>> REAL_NUMBERS = Collections.unmodifiableSet(Sets.<Class<?>> newHashSet(BigDecimal.class, Double.class, Float.class));
    public static final Set<Class<?>> NATURAL_NUMBERS = Collections.unmodifiableSet(Sets.<Class<?>> newHashSet(Long.class, BigInteger.class, Integer.class,
                    Short.class, Byte.class));
    
    public enum ContainerType {
        OR_NODE, AND_NODE
    }
    
    /**
     * Expand a node given a mapping of fields to values. If the list is empty, then the original regex should be used.
     * 
     * @param containerType
     *            should we create OR nodes or AND nodes
     * @param node
     * @param fieldsToValues
     *            A mapping of fields to values. If the values for a field is empty, then the original regex should be used.
     * @param expandFields
     *            Expand fields if true
     * @param expandValues
     *            Expand values if true
     * @param keepOriginalNode
     *            Keep the original node along with any expansions
     * @return A new sub query
     */
    public static JexlNode createNodeTreeFromFieldsToValues(ContainerType containerType, JexlNode node, JexlNode orgNode, IndexLookupMap fieldsToValues,
                    boolean expandFields, boolean expandValues, boolean keepOriginalNode) {
        // do nothing if not expanding fields or values
        if (!expandFields && !expandValues) {
            return orgNode;
        }
        
        // no expansions needed if the fieldname threshold is exceeded
        if (fieldsToValues.isKeyThresholdExceeded()) {
            return new ExceededTermThresholdMarkerJexlNode(orgNode);
        }
        
        // collapse the value sets if not expanding fields
        if (!expandFields) {
            ValueSet allValues = new ValueSet(-1);
            for (ValueSet values : fieldsToValues.values()) {
                allValues.addAll(values);
            }
            fieldsToValues.clear();
            for (String identifier : JexlASTHelper.getIdentifierNames(orgNode)) {
                fieldsToValues.put(identifier, allValues);
            }
        }
        
        Set<String> fields = fieldsToValues.keySet();
        
        JexlNode parentNode = (containerType.equals(ContainerType.OR_NODE) ? new ASTOrNode(ParserTreeConstants.JJTORNODE) : new ASTAndNode(
                        ParserTreeConstants.JJTANDNODE));
        int parentNodeChildCount = 0;
        
        if (keepOriginalNode) {
            JexlNodes.ensureCapacity(parentNode, fields.size() + 1);
            JexlNode child = RebuildingVisitor.copy(orgNode);
            parentNode.jjtAddChild(child, parentNodeChildCount);
            child.jjtSetParent(parentNode);
            parentNodeChildCount++;
            // remove this entry from the fieldsToValues to avoid duplication
            for (String identifier : JexlASTHelper.getIdentifierNames(orgNode)) {
                for (Object value : JexlASTHelper.getLiteralValues(orgNode)) {
                    fieldsToValues.remove(identifier, value);
                }
            }
        } else {
            JexlNodes.ensureCapacity(parentNode, fields.size());
        }
        
        for (String field : fields) {
            ValueSet valuesForField = fieldsToValues.get(field);
            
            // if not expanding values, then reuse the original node with simply a new field name (anyfield only)
            if (!expandValues) {
                JexlNode child = RebuildingVisitor.copy(orgNode);
                for (ASTIdentifier identifier : JexlASTHelper.getIdentifiers(child)) {
                    if (identifier.image.equals(Constants.ANY_FIELD)) {
                        identifier.image = field;
                    }
                }
                parentNode.jjtAddChild(child, parentNodeChildCount);
                child.jjtSetParent(parentNode);
                
                parentNodeChildCount++;
            }
            
            // a threshold exceeded set of values requires using the original
            // node with a new fieldname, wrapped with a marker node
            else if (valuesForField.isThresholdExceeded()) {
                // create a set of nodes wrapping each pattern
                List<String> patterns = new ArrayList<>(fieldsToValues.getPatterns() == null ? new ArrayList<>() : fieldsToValues.getPatterns());
                if (patterns.isEmpty()) {
                    JexlNode child = new ExceededValueThresholdMarkerJexlNode(buildUntypedNode(orgNode, field));
                    
                    parentNode.jjtAddChild(child, parentNodeChildCount);
                    child.jjtSetParent(parentNode);
                    
                    parentNodeChildCount++;
                } else if (patterns.size() == 1) {
                    JexlNode child = new ExceededValueThresholdMarkerJexlNode(buildUntypedNode(orgNode, field, patterns.get(0)));
                    
                    parentNode.jjtAddChild(child, parentNodeChildCount);
                    child.jjtSetParent(parentNode);
                    
                    parentNodeChildCount++;
                } else {
                    int childNodeChildCount = 0;
                    JexlNode childNode = (containerType.equals(ContainerType.OR_NODE) ? new ASTOrNode(ParserTreeConstants.JJTORNODE) : new ASTAndNode(
                                    ParserTreeConstants.JJTANDNODE));
                    JexlNodes.ensureCapacity(childNode, patterns.size());
                    for (String pattern : patterns) {
                        JexlNode child = new ExceededValueThresholdMarkerJexlNode(buildUntypedNode(orgNode, field, pattern));
                        
                        childNode.jjtAddChild(child, childNodeChildCount);
                        child.jjtSetParent(childNode);
                        
                        childNodeChildCount++;
                    }
                    
                    if (0 < childNodeChildCount) {
                        JexlNode wrappedChildNode = wrap(childNode);
                        childNode.jjtSetParent(wrappedChildNode);
                        
                        parentNode.jjtAddChild(wrappedChildNode, parentNodeChildCount);
                        childNode.jjtSetParent(childNode);
                        
                        parentNodeChildCount++;
                    }
                }
            }
            
            // Don't create an OR if we have only one value, directly attach it
            else if (1 == valuesForField.size()) {
                JexlNode child = buildUntypedNode(node, field, valuesForField.iterator().next());
                
                parentNode.jjtAddChild(child, parentNodeChildCount);
                child.jjtSetParent(parentNode);
                
                parentNodeChildCount++;
            } else {
                int childNodeChildCount = 0;
                JexlNode childNode = (containerType.equals(ContainerType.OR_NODE) ? new ASTOrNode(ParserTreeConstants.JJTORNODE) : new ASTAndNode(
                                ParserTreeConstants.JJTANDNODE));
                JexlNodes.ensureCapacity(childNode, valuesForField.size());
                
                for (String value : valuesForField) {
                    JexlNode child = buildUntypedNode(node, field, value);
                    
                    childNode.jjtAddChild(child, childNodeChildCount);
                    child.jjtSetParent(childNode);
                    
                    childNodeChildCount++;
                }
                
                if (0 < childNodeChildCount) {
                    JexlNode wrappedChildNode = wrap(childNode);
                    childNode.jjtSetParent(wrappedChildNode);
                    
                    parentNode.jjtAddChild(wrappedChildNode, parentNodeChildCount);
                    childNode.jjtSetParent(childNode);
                    
                    parentNodeChildCount++;
                }
            }
        }
        
        switch (parentNodeChildCount) {
            case 0:
                // in this case we had no matches for the range, so this expression gets replaced with a FALSE node.
                return new ASTFalseNode(ParserTreeConstants.JJTFALSENODE);
            case 1:
                JexlNode child = parentNode.jjtGetChild(0);
                JexlNodes.promote(parentNode, child);
                return child;
            default:
                JexlNode wrappedParentNode = wrap(parentNode);
                parentNode.jjtSetParent(wrappedParentNode);
                return wrappedParentNode;
        }
    }
    
    public static JexlNode createNodeTreeFromPairs(ContainerType containerType, JexlNode node, Set<List<JexlNode>> pairs) {
        if (1 == pairs.size()) {
            List<JexlNode> pair = pairs.iterator().next();
            
            if (2 != pair.size()) {
                throw new UnsupportedOperationException("Cannot construct a node from a non-binary pair: " + pair);
            }
            
            return buildUntypedBinaryNode(node, pair.get(0), pair.get(1));
        }
        
        JexlNode parentNode = (containerType.equals(ContainerType.OR_NODE) ? new ASTOrNode(ParserTreeConstants.JJTORNODE) : new ASTAndNode(
                        ParserTreeConstants.JJTANDNODE));
        
        int i = 0;
        JexlNodes.ensureCapacity(parentNode, pairs.size());
        for (List<JexlNode> pair : pairs) {
            if (2 != pair.size()) {
                throw new UnsupportedOperationException("Cannot construct a node from a non-binary pair: " + pair);
            }
            
            JexlNode child = buildUntypedBinaryNode(node, pair.get(0), pair.get(1));
            parentNode.jjtAddChild(child, i);
            
            // We want to override the default parent that would be set by
            // buildUntypedNode because we're attaching this to an OR
            child.jjtSetParent(parentNode);
            
            i++;
        }
        
        // If we have more than one element in the new node, wrap it in
        // parentheses
        JexlNode wrapped = wrap(parentNode);
        
        // Set the parent pointer
        wrapped.jjtSetParent(node.jjtGetParent());
        
        return wrapped;
        
    }
    
    public static JexlNode createNodeTreeFromFieldNames(ContainerType containerType, JexlNode node, Object literal, Collection<String> fieldNames) {
        
        // A single field:term doesn't need to be an OR
        if (1 == fieldNames.size()) {
            return buildUntypedNode(node, fieldNames.iterator().next(), literal);
        }
        
        JexlNode parentNode = (containerType.equals(ContainerType.OR_NODE) ? new ASTOrNode(ParserTreeConstants.JJTORNODE) : new ASTAndNode(
                        ParserTreeConstants.JJTANDNODE));
        int i = 0;
        JexlNodes.ensureCapacity(parentNode, fieldNames.size());
        for (String fieldName : fieldNames) {
            JexlNode child = buildUntypedNode(node, fieldName, literal);
            parentNode.jjtAddChild(child, i);
            
            // We want to override the default parent that would be set by
            // buildUntypedNode because we're attaching this to an OR
            child.jjtSetParent(parentNode);
            
            i++;
        }
        
        // If we have more than one element in the new node, wrap it in
        // parentheses
        JexlNode wrapped = wrap(parentNode);
        
        // Set the parent pointer
        wrapped.jjtSetParent(node.jjtGetParent());
        
        return wrapped;
    }
    
    /**
     * Returns an Or of the given fieldname and values, wrapping the JexlOrNode correctly in a Reference and ReferenceExpression (parens)
     * 
     * @param node
     * @param fieldName
     * @param fieldValues
     * @return
     */
    public static JexlNode createNodeTreeFromFieldValues(ContainerType containerType, JexlNode node, JexlNode orgNode, String fieldName,
                    Collection<String> fieldValues) {
        
        // an empty set of values requires using the original node with a new
        // fieldname
        if (fieldValues == null || fieldValues.isEmpty()) {
            return buildUntypedNode(orgNode, fieldName);
        }
        
        // A single field:term doesn't need to be an OR
        if (1 == fieldValues.size()) {
            return buildUntypedNode(node, fieldName, fieldValues.iterator().next());
        }
        
        JexlNode parentNode = (containerType.equals(ContainerType.OR_NODE) ? new ASTOrNode(ParserTreeConstants.JJTORNODE) : new ASTAndNode(
                        ParserTreeConstants.JJTANDNODE));
        JexlNodes.ensureCapacity(parentNode, fieldValues.size());
        
        int i = 0;
        for (String fieldValue : fieldValues) {
            JexlNode child = buildUntypedNode(node, fieldName, fieldValue);
            parentNode.jjtAddChild(child, i);
            child.jjtSetParent(parentNode);
            i++;
        }
        
        // If we have more than one element in the new node, wrap it in
        // parentheses
        JexlNode wrapped = wrap(parentNode);
        
        // Set the parent pointer
        wrapped.jjtSetParent(node.jjtGetParent());
        
        return wrapped;
    }
    
    public static JexlNode createNewValueNodeTreeFromFieldValues(ContainerType containerType, JexlNode node, JexlNode orgNode, String fieldName,
                    Collection<String> fieldValues) {
        
        // an empty set of values requires using the original node with a new
        // fieldname
        if (fieldValues == null || fieldValues.isEmpty()) {
            return buildUntypedNode(orgNode, fieldName);
        }
        
        // A single field:term doesn't need to be an OR
        if (1 == fieldValues.size()) {
            return buildUntypedNewLiteralNode(node, fieldName, fieldValues.iterator().next());
        }
        
        JexlNode parentNode = (containerType.equals(ContainerType.OR_NODE) ? new ASTOrNode(ParserTreeConstants.JJTORNODE) : new ASTAndNode(
                        ParserTreeConstants.JJTANDNODE));
        JexlNodes.ensureCapacity(parentNode, fieldValues.size());
        
        int i = 0;
        for (String fieldValue : fieldValues) {
            JexlNode child = buildUntypedNewLiteralNode(node, fieldName, fieldValue);
            parentNode.jjtAddChild(child, i);
            child.jjtSetParent(parentNode);
            i++;
        }
        
        // If we have more than one element in the new node, wrap it in
        // parentheses
        JexlNode wrapped = wrap(parentNode);
        
        // Set the parent pointer
        wrapped.jjtSetParent(node.jjtGetParent());
        
        return wrapped;
    }
    
    /**
     * Wrap collection of JexlNodes into an ASTAndNode
     * 
     * @param children
     * @return
     */
    public static JexlNode createAndNode(Iterable<? extends JexlNode> children) {
        return wrapChildren(new ASTAndNode(ParserTreeConstants.JJTANDNODE), children);
    }
    
    /**
     * Wrap collection of JexlNodes into an ASTAndNode
     * 
     * @param children
     * @return
     */
    public static JexlNode createUnwrappedAndNode(Iterable<? extends JexlNode> children) {
        return setChildren(new ASTAndNode(ParserTreeConstants.JJTANDNODE), children);
    }
    
    /**
     * Wrap a collection of JexlNodes into an ASTOrNode
     * 
     * @param children
     * @return
     */
    public static JexlNode createOrNode(Iterable<? extends JexlNode> children) {
        return wrapChildren(new ASTOrNode(ParserTreeConstants.JJTORNODE), children);
    }
    
    /**
     * Wrap a collection of JexlNodes into an ASTOrNode
     * 
     * @param children
     * @return
     */
    public static JexlNode createUnwrappedOrNode(Iterable<? extends JexlNode> children) {
        return setChildren(new ASTOrNode(ParserTreeConstants.JJTORNODE), children);
    }
    
    /**
     * Add the children JexlNodes to the parent JexlNode, correctly setting parent pointers, parenthesis, and reference nodes.
     * 
     * @param parent
     * @param children
     * @return
     */
    public static JexlNode wrapChildren(JexlNode parent, Iterable<? extends JexlNode> children) {
        parent = setChildren(parent, children);
        
        JexlNode grandParent = parent.jjtGetParent();
        
        // If we have more than one element in the new node, wrap it in
        // parentheses
        JexlNode wrapped = wrap(parent);
        
        // Set the parent pointer
        wrapped.jjtSetParent(grandParent);
        
        return wrapped;
    }
    
    /**
     * Add the children JexlNodes to the parent JexlNode, correctly setting parent pointers, parenthesis, and reference nodes.
     * 
     * @param parent
     * @param children
     * @return
     */
    public static JexlNode setChildren(JexlNode parent, Iterable<? extends JexlNode> children) {
        int i = 0;
        JexlNodes.ensureCapacity(parent, Iterables.size(children)); // hopefully
                                                                    // we got
                                                                    // passed a
                                                                    // collection...
        for (JexlNode child : children) {
            parent.jjtAddChild(child, i);
            child.jjtSetParent(parent);
            i++;
        }
        return parent;
    }
    
    /**
     * Wrap an ASTAndNode or ASTOrNode in parenthesis if it has more than one child. Will return itself if wrapping is unnecessary.
     * 
     * @param toWrap
     * @return
     */
    public static JexlNode wrap(JexlNode toWrap) {
        if ((toWrap instanceof ASTAndNode || toWrap instanceof ASTOrNode) && toWrap.jjtGetNumChildren() > 1) {
            ASTReference reference = new ASTReference(ParserTreeConstants.JJTREFERENCE);
            ASTReferenceExpression parens = new ASTReferenceExpression(ParserTreeConstants.JJTREFERENCEEXPRESSION);
            
            parens.jjtAddChild(toWrap, 0);
            toWrap.jjtSetParent(parens);
            
            reference.jjtAddChild(parens, 0);
            parens.jjtSetParent(reference);
            
            return reference;
        }
        
        return toWrap;
    }
    
    /**
     * Create an ASTJexlScript with the provided child
     * 
     * @param child
     * @return
     */
    public static ASTJexlScript createScript(JexlNode child) {
        ASTJexlScript script = new ASTJexlScript(ParserTreeConstants.JJTJEXLSCRIPT);
        
        // Wrap the child in parens to be sure we don't misconstrue the intent
        // of the query
        JexlNode wrappedChild = wrap(child);
        script.jjtAddChild(wrappedChild, 0);
        wrappedChild.jjtSetParent(script);
        
        return script;
    }
    
    public static JexlNode buildUntypedBinaryNode(JexlNode original, JexlNode left, JexlNode right) {
        if (left instanceof ASTIdentifier && right instanceof ASTIdentifier) {
            return buildUntypedDblIdentifierNode(shallowCopy(original), left, right);
            
        } else if (left instanceof ASTIdentifier && JexlASTHelper.isLiteral(right)) {
            // Every instance of JexlNode.Literal is also a JexlNode
            return buildUntypedNewNode(shallowCopy(original), (ASTIdentifier) left, right);
            
        } else if (JexlASTHelper.isLiteral(left) && right instanceof ASTIdentifier) {
            // Every instance of JexlNode.Literal is also a JexlNode
            return buildUntypedNewNode(shallowCopy(original), left, (ASTIdentifier) right);
            
        } else if (JexlASTHelper.isLiteral(left) && JexlASTHelper.isLiteral(right)) {
            // Every instance of JexlNode.Literal is also a JexlNode
            return buildUntypedDblLiteralNode(shallowCopy(original), left, right);
            
        } else if (left instanceof ASTReference && JexlASTHelper.isLiteral(right)) {
            return buildUntypedDblLiteralNode(shallowCopy(original), left, right);
        } else if (right instanceof ASTReference && JexlASTHelper.isLiteral(left)) {
            return buildUntypedDblLiteralNode(shallowCopy(original), left, right);
        } else if (left instanceof ASTAdditiveNode && JexlASTHelper.isLiteral(right)) {
            return buildUntypedDblLiteralNode(shallowCopy(original), left, right);
            
        } else if (left instanceof ASTReference && right instanceof ASTReference) {
            return buildUntypedDblIdentifierNode(shallowCopy(original), left, right);
            
        } else if (left instanceof ASTReference && right instanceof ASTIdentifier) {
            return buildUntypedDblIdentifierNode(shallowCopy(original), left, right);
            
        } else if (left instanceof ASTMulNode || right instanceof ASTMulNode) {
            return RebuildingVisitor.copy(original);
        } else if (left instanceof ASTAdditiveNode || right instanceof ASTAdditiveNode) {
            return RebuildingVisitor.copy(original);
            
        } else if (left instanceof ASTModNode || right instanceof ASTModNode) {
            return RebuildingVisitor.copy(original);
            
        } else if (left instanceof ASTDivNode && JexlASTHelper.isLiteral(right)) {
            return buildUntypedDblLiteralNode(shallowCopy(original), left, right);
            
        }
        
        throw new UnsupportedOperationException("Could not create a node from the given children: " + left + ", " + right);
    }
    
    public static JexlNode buildUntypedNode(JexlNode original, String fieldName, Object fieldValue) {
        if (fieldValue instanceof String) {
            return buildUntypedNode(original, fieldName, (String) fieldValue);
        } else if (fieldValue instanceof Number) {
            return buildUntypedNode(original, fieldName, (Number) fieldValue);
        } else if (fieldValue instanceof Boolean) {
            return buildBooleanNode(original, buildIdentifier(fieldName), (Boolean) fieldValue);
        } else if (fieldValue instanceof JexlNode) {
            return buildUntypedNode(original, fieldName, fieldValue);
        } else if (null == fieldValue) {
            return buildNullNode(original, buildIdentifier(fieldName));
        } else {
            throw new UnsupportedOperationException("Literal was not a String nor a Number");
        }
    }
    
    public static JexlNode buildUntypedNewLiteralNode(JexlNode original, String fieldName, Object fieldValue) {
        if (fieldValue instanceof String) {
            return buildUntypedNewLiteralNode(original, fieldName, (String) fieldValue);
        } else if (fieldValue instanceof Number) {
            return buildUntypedNewLiteralNode(original, fieldName, (Number) fieldValue);
        } else if (fieldValue instanceof Boolean) {
            return buildBooleanNode(original, buildIdentifier(fieldName), (Boolean) fieldValue);
        } else if (fieldValue instanceof JexlNode) {
            return buildUntypedNode(original, fieldName, fieldValue);
        } else if (null == fieldValue) {
            return buildNullNode(original, buildIdentifier(fieldName));
        } else {
            throw new UnsupportedOperationException("Literal was not a String nor a Number");
        }
    }
    
    public static JexlNode buildUntypedNode(JexlNode original, String fieldName, String fieldValue) {
        if (original instanceof ASTEQNode) {
            return buildNode((ASTEQNode) original, fieldName, fieldValue);
        } else if (original instanceof ASTNENode) {
            return buildNode((ASTNENode) original, fieldName, fieldValue);
        } else if (original instanceof ASTERNode) {
            return buildNode((ASTERNode) original, fieldName, fieldValue);
        } else if (original instanceof ASTNRNode) {
            return buildNode((ASTNRNode) original, fieldName, fieldValue);
        } else if (original instanceof ASTGTNode) {
            return buildNode((ASTGTNode) original, fieldName, fieldValue);
        } else if (original instanceof ASTGENode) {
            return buildNode((ASTGENode) original, fieldName, fieldValue);
        } else if (original instanceof ASTLTNode) {
            return buildNode((ASTLTNode) original, fieldName, fieldValue);
        } else if (original instanceof ASTLENode) {
            return buildNode((ASTLENode) original, fieldName, fieldValue);
        } else {
            throw new UnsupportedOperationException("Cannot handle " + original);
        }
    }
    
    public static JexlNode buildUntypedNewLiteralNode(JexlNode original, String fieldName, String fieldValue) {
        
        if (original instanceof ASTEQNode) {
            return buildNewLiteralNode(original, fieldName, fieldValue);
        } else if (original instanceof ASTNENode) {
            return buildNewLiteralNode(original, fieldName, fieldValue);
        } else if (original instanceof ASTERNode) {
            return buildNewLiteralNode(original, fieldName, fieldValue);
        } else if (original instanceof ASTNRNode) {
            return buildNewLiteralNode(original, fieldName, fieldValue);
        } else if (original instanceof ASTGTNode) {
            return buildNewLiteralNode(original, fieldName, fieldValue);
        } else if (original instanceof ASTGENode) {
            return buildNewLiteralNode(original, fieldName, fieldValue);
        } else if (original instanceof ASTLTNode) {
            return buildNewLiteralNode(original, fieldName, fieldValue);
        } else if (original instanceof ASTLENode) {
            return buildNewLiteralNode(original, fieldName, fieldValue);
        } else {
            throw new UnsupportedOperationException("Cannot handle " + original);
        }
    }
    
    public static JexlNode buildUntypedNode(JexlNode original, String fieldName) {
        // required to handle ranges that could not be expanded
        if (original instanceof ASTAndNode || original instanceof ASTOrNode || original instanceof ASTReferenceExpression || original instanceof ASTReference
                        || original instanceof ASTAssignment || original instanceof ASTIdentifier || original instanceof ASTTrueNode) {
            JexlNode newNode = shallowCopy(original);
            for (int i = 0; i < original.jjtGetNumChildren(); i++) {
                newNode.jjtAddChild(buildUntypedNode(original.jjtGetChild(i), fieldName), i);
            }
            return newNode;
        }
        
        // first pull the field value if any from the original
        JexlNode fieldValue = null;
        if (original.jjtGetNumChildren() == 1) {
            return buildNullNode(original, buildIdentifier(fieldName));
        } else if (original.jjtGetNumChildren() == 2) {
            fieldValue = original.jjtGetChild(1);
            while (fieldValue.jjtGetNumChildren() == 1) {
                fieldValue = fieldValue.jjtGetChild(0);
            }
            if (fieldValue.jjtGetNumChildren() != 0) {
                throw new UnsupportedOperationException("Cannot handle " + original);
            }
        } else if (original.jjtGetNumChildren() > 2) {
            throw new UnsupportedOperationException("Cannot handle " + original);
        }
        
        if (original instanceof ASTEQNode) {
            return buildNode((ASTEQNode) original, fieldName, fieldValue);
        } else if (original instanceof ASTNENode) {
            return buildNode((ASTNENode) original, fieldName, fieldValue);
        } else if (original instanceof ASTERNode) {
            return buildNode((ASTERNode) original, fieldName, fieldValue);
        } else if (original instanceof ASTNRNode) {
            return buildNode((ASTNRNode) original, fieldName, fieldValue);
        } else if (original instanceof ASTGTNode) {
            return buildNode((ASTGTNode) original, fieldName, fieldValue);
        } else if (original instanceof ASTGENode) {
            return buildNode((ASTGENode) original, fieldName, fieldValue);
        } else if (original instanceof ASTLTNode) {
            return buildNode((ASTLTNode) original, fieldName, fieldValue);
        } else if (original instanceof ASTLENode) {
            return buildNode((ASTLENode) original, fieldName, fieldValue);
        } else {
            throw new UnsupportedOperationException("Cannot handle " + original);
        }
    }
    
    public static JexlNode buildUntypedNode(JexlNode original, String fieldName, Number fieldValue) {
        
        if (original instanceof ASTEQNode) {
            return buildNode((ASTEQNode) original, fieldName, fieldValue);
        } else if (original instanceof ASTNENode) {
            return buildNode((ASTNENode) original, fieldName, fieldValue);
        } else if (original instanceof ASTERNode) {
            return buildNode((ASTERNode) original, fieldName, fieldValue);
        } else if (original instanceof ASTNRNode) {
            return buildNode((ASTNRNode) original, fieldName, fieldValue);
        } else if (original instanceof ASTGTNode) {
            return buildNode((ASTGTNode) original, fieldName, fieldValue);
        } else if (original instanceof ASTGENode) {
            return buildNode((ASTGENode) original, fieldName, fieldValue);
        } else if (original instanceof ASTLTNode) {
            return buildNode((ASTLTNode) original, fieldName, fieldValue);
        } else if (original instanceof ASTLENode) {
            return buildNode((ASTLENode) original, fieldName, fieldValue);
        } else {
            throw new UnsupportedOperationException("Cannot handle " + original);
        }
    }
    
    public static JexlNode buildUntypedNewLiteralNode(JexlNode original, String fieldName, Number fieldValue) {
        if (original instanceof ASTEQNode) {
            return buildNewLiteralNode(original, fieldName, fieldValue);
        } else if (original instanceof ASTNENode) {
            return buildNewLiteralNode(original, fieldName, fieldValue);
        } else if (original instanceof ASTERNode) {
            return buildNewLiteralNode(original, fieldName, fieldValue);
        } else if (original instanceof ASTNRNode) {
            return buildNewLiteralNode(original, fieldName, fieldValue);
        } else if (original instanceof ASTGTNode) {
            return buildNewLiteralNode(original, fieldName, fieldValue);
        } else if (original instanceof ASTGENode) {
            return buildNewLiteralNode(original, fieldName, fieldValue);
        } else if (original instanceof ASTLTNode) {
            return buildNewLiteralNode(original, fieldName, fieldValue);
        } else if (original instanceof ASTLENode) {
            return buildNewLiteralNode(original, fieldName, fieldValue);
        } else {
            throw new UnsupportedOperationException("Cannot handle " + original);
        }
    }
    
    public static JexlNode buildEQNode(String fieldName, String fieldValue) {
        return buildNode((ASTEQNode) null, fieldName, fieldValue);
    }
    
    public static JexlNode buildERNode(String fieldName, String fieldValue) {
        return buildNode((ASTERNode) null, fieldName, fieldValue);
    }
    
    public static JexlNode buildNRNode(String fieldName, String fieldValue) {
        return buildNode((ASTNRNode) null, fieldName, fieldValue);
    }
    
    public static JexlNode createFromClass(Class<?> clz) {
        if (clz.equals(ASTEQNode.class)) {
            return new ASTEQNode(ParserTreeConstants.JJTEQNODE);
        } else if (clz.equals(ASTERNode.class)) {
            return new ASTERNode(ParserTreeConstants.JJTERNODE);
        } else if (clz.equals(ASTNENode.class)) {
            return new ASTNENode(ParserTreeConstants.JJTNENODE);
        } else if (clz.equals(ASTNRNode.class)) {
            return new ASTNRNode(ParserTreeConstants.JJTNRNODE);
        } else {
            throw new RuntimeException("unhandled class " + clz.getName());
        }
    }
    
    /**
     * A shallow copy of the given JexlNode, creates a new node of the same type with the same parent and image. Children are not copied
     * 
     * @param original
     * @return
     */
    public static JexlNode shallowCopy(JexlNode original) {
        if (null == original) {
            throw new IllegalArgumentException();
        }
        
        JexlNode copy;
        Class<?> clz = original.getClass();
        
        if (ASTAndNode.class.isAssignableFrom(clz)) {
            copy = new ASTAndNode(ParserTreeConstants.JJTANDNODE);
        } else if (ASTBitwiseAndNode.class.isAssignableFrom(clz)) {
            copy = new ASTBitwiseAndNode(ParserTreeConstants.JJTBITWISEANDNODE);
        } else if (ASTBitwiseComplNode.class.isAssignableFrom(clz)) {
            copy = new ASTBitwiseComplNode(ParserTreeConstants.JJTBITWISECOMPLNODE);
        } else if (ASTBitwiseOrNode.class.isAssignableFrom(clz)) {
            copy = new ASTBitwiseOrNode(ParserTreeConstants.JJTBITWISEORNODE);
        } else if (ASTBitwiseXorNode.class.isAssignableFrom(clz)) {
            copy = new ASTBitwiseXorNode(ParserTreeConstants.JJTBITWISEXORNODE);
        } else if (ASTEmptyFunction.class.isAssignableFrom(clz)) {
            copy = new ASTEmptyFunction(ParserTreeConstants.JJTEMPTYFUNCTION);
        } else if (ASTEQNode.class.isAssignableFrom(clz)) {
            copy = new ASTEQNode(ParserTreeConstants.JJTEQNODE);
        } else if (ASTERNode.class.isAssignableFrom(clz)) {
            copy = new ASTERNode(ParserTreeConstants.JJTERNODE);
        } else if (ASTFalseNode.class.isAssignableFrom(clz)) {
            copy = new ASTFalseNode(ParserTreeConstants.JJTFALSENODE);
        } else if (ASTGENode.class.isAssignableFrom(clz)) {
            copy = new ASTGENode(ParserTreeConstants.JJTGENODE);
        } else if (ASTGTNode.class.isAssignableFrom(clz)) {
            copy = new ASTGTNode(ParserTreeConstants.JJTGTNODE);
        } else if (ASTIdentifier.class.isAssignableFrom(clz)) {
            copy = new ASTIdentifier(ParserTreeConstants.JJTIDENTIFIER);
        } else if (ASTLENode.class.isAssignableFrom(clz)) {
            copy = new ASTLENode(ParserTreeConstants.JJTLENODE);
        } else if (ASTLTNode.class.isAssignableFrom(clz)) {
            copy = new ASTLTNode(ParserTreeConstants.JJTLTNODE);
        } else if (ASTNENode.class.isAssignableFrom(clz)) {
            copy = new ASTNENode(ParserTreeConstants.JJTNENODE);
        } else if (ASTNRNode.class.isAssignableFrom(clz)) {
            copy = new ASTNRNode(ParserTreeConstants.JJTNRNODE);
        } else if (ASTNotNode.class.isAssignableFrom(clz)) {
            copy = new ASTNotNode(ParserTreeConstants.JJTNOTNODE);
        } else if (ASTNullLiteral.class.isAssignableFrom(clz)) {
            copy = new ASTNullLiteral(ParserTreeConstants.JJTNULLLITERAL);
        } else if (ASTNumberLiteral.class.isAssignableFrom(clz)) {
            copy = new ASTNumberLiteral(ParserTreeConstants.JJTNUMBERLITERAL);
            JexlNodes.setLiteral((ASTNumberLiteral) copy, ((ASTNumberLiteral) original).getLiteral());
        } else if (ASTOrNode.class.isAssignableFrom(clz)) {
            copy = new ASTOrNode(ParserTreeConstants.JJTORNODE);
        } else if (ASTStringLiteral.class.isAssignableFrom(clz)) {
            copy = new ASTStringLiteral(ParserTreeConstants.JJTSTRINGLITERAL);
            JexlNodes.setLiteral((ASTStringLiteral) copy, ((ASTStringLiteral) original).getLiteral());
        } else if (ASTTrueNode.class.isAssignableFrom(clz)) {
            copy = new ASTTrueNode(ParserTreeConstants.JJTTRUENODE);
        } else if (ASTReferenceExpression.class.isAssignableFrom(clz)) {
            copy = new ASTReferenceExpression(ParserTreeConstants.JJTREFERENCEEXPRESSION);
        } else if (ASTReference.class.isAssignableFrom(clz)) {
            copy = new ASTReference(ParserTreeConstants.JJTREFERENCE);
        } else if (ASTAdditiveNode.class.isAssignableFrom(clz)) {
            copy = new ASTAdditiveNode(ParserTreeConstants.JJTADDITIVENODE);
        } else if (ASTMethodNode.class.isAssignableFrom(clz)) {
            copy = new ASTMethodNode(ParserTreeConstants.JJTMETHODNODE);
        } else if (ASTFunctionNode.class.isAssignableFrom(clz)) {
            copy = new ASTFunctionNode(ParserTreeConstants.JJTFUNCTIONNODE);
        } else if (ASTMulNode.class.isAssignableFrom(clz)) {
            copy = new ASTMulNode(ParserTreeConstants.JJTMULNODE);
        } else if (ASTAssignment.class.isAssignableFrom(clz)) {
            copy = new ASTAssignment(ParserTreeConstants.JJTASSIGNMENT);
        } else {
            throw new UnsupportedOperationException();
        }
        
        copy.jjtSetParent(original.jjtGetParent());
        copy.image = original.image;
        return copy;
    }
    
    /**
     * Create a new ASTEQNode from the given field name and value
     * 
     * @param original
     * @param fieldName
     * @param fieldValue
     * @return
     */
    public static JexlNode buildNode(ASTEQNode original, String fieldName, String fieldValue) {
        ASTEQNode newNode = new ASTEQNode(ParserTreeConstants.JJTEQNODE);
        if (null != original) {
            newNode.jjtSetParent(original.jjtGetParent());
        }
        return buildUntypedNewNode(newNode, buildIdentifier(fieldName), fieldValue);
    }
    
    /**
     * Create a new ASTEQNode from the given field name and value
     * 
     * @param original
     * @param fieldName
     * @param fieldValue
     * @return
     */
    public static JexlNode buildNode(ASTEQNode original, String fieldName, Number fieldValue) {
        ASTEQNode newNode = new ASTEQNode(ParserTreeConstants.JJTEQNODE);
        if (null != original) {
            newNode.jjtSetParent(original.jjtGetParent());
        }
        return buildUntypedNewNode(newNode, buildIdentifier(fieldName), fieldValue);
    }
    
    public static JexlNode buildNewLiteralNode(JexlNode original, String fieldName, Number fieldValue) {
        ASTReference literalReference = new ASTReference(ParserTreeConstants.JJTREFERENCE);
        ASTNumberLiteral literal = getLiteral(fieldValue);
        literalReference.jjtAddChild(literal, 0);
        literal.jjtSetParent(literalReference);
        
        // we don't know whether the left or right side is the literal to replace. find it.
        if (original.jjtGetChild(0) instanceof ASTReference && original.jjtGetChild(0).jjtGetChild(0) instanceof ASTIdentifier) {
            original.jjtAddChild(literalReference, 1); // replace the original reference/literal (on left) with new reference/literal
        }
        if (original.jjtGetChild(1) instanceof ASTReference && original.jjtGetChild(1).jjtGetChild(0) instanceof ASTIdentifier) {
            original.jjtAddChild(literalReference, 0); // replace the original reference/literal (on right) with new reference/literal
        }
        
        literalReference.jjtSetParent(original);
        
        return original;
    }
    
    public static JexlNode buildNewLiteralNode(JexlNode original, String fieldName, String fieldValue) {
        ASTReference literalReference = new ASTReference(ParserTreeConstants.JJTREFERENCE);
        
        ASTStringLiteral literal = getLiteral(fieldValue);
        literalReference.jjtAddChild(literal, 0);
        literal.jjtSetParent(literalReference);
        
        // we don't know whether the left or right side is the literal to replace. find it.
        if (original.jjtGetChild(0) instanceof ASTReference && original.jjtGetChild(0).jjtGetChild(0) instanceof ASTIdentifier) {
            original.jjtAddChild(literalReference, 1); // replace the original reference/literal (on left) with new reference/literal
        }
        if (original.jjtGetChild(1) instanceof ASTReference && original.jjtGetChild(1).jjtGetChild(0) instanceof ASTIdentifier) {
            original.jjtAddChild(literalReference, 0); // replace the original reference/literal (on right) with new reference/literal
        }
        
        literalReference.jjtSetParent(original);
        
        return original;
    }
    
    /**
     * Create a new ASTEQNode from the given field name and value
     * 
     * @param original
     * @param fieldName
     * @param fieldValue
     * @return
     */
    public static JexlNode buildNode(ASTEQNode original, String fieldName, JexlNode fieldValue) {
        ASTEQNode newNode = new ASTEQNode(ParserTreeConstants.JJTEQNODE);
        if (null != original) {
            newNode.jjtSetParent(original.jjtGetParent());
        }
        return buildUntypedNewNode(newNode, buildIdentifier(fieldName), fieldValue);
    }
    
    /**
     * Create a new ASTNENode from the given field name and value
     * 
     * @param original
     * @param fieldName
     * @param fieldValue
     * @return
     */
    public static JexlNode buildNode(ASTNENode original, String fieldName, String fieldValue) {
        ASTNENode newNode = new ASTNENode(ParserTreeConstants.JJTNENODE);
        if (null != original) {
            newNode.jjtSetParent(original.jjtGetParent());
        }
        return buildUntypedNewNode(newNode, buildIdentifier(fieldName), fieldValue);
    }
    
    /**
     * Create a new ASTNENode from the given field name and value
     * 
     * @param original
     * @param fieldName
     * @param fieldValue
     * @return
     */
    public static JexlNode buildNode(ASTNENode original, String fieldName, Number fieldValue) {
        ASTNENode newNode = new ASTNENode(ParserTreeConstants.JJTNENODE);
        if (null != original) {
            newNode.jjtSetParent(original.jjtGetParent());
        }
        return buildUntypedNewNode(newNode, buildIdentifier(fieldName), fieldValue);
    }
    
    /**
     * Create a new ASTNENode from the given field name and value
     * 
     * @param original
     * @param fieldName
     * @param fieldValue
     * @return
     */
    public static JexlNode buildNode(ASTNENode original, String fieldName, JexlNode fieldValue) {
        ASTNENode newNode = new ASTNENode(ParserTreeConstants.JJTNENODE);
        if (null != original) {
            newNode.jjtSetParent(original.jjtGetParent());
        }
        return buildUntypedNewNode(newNode, buildIdentifier(fieldName), fieldValue);
    }
    
    /**
     * Create a new ASTERNode from the given field name and value
     * 
     * @param original
     * @param fieldName
     * @param fieldValue
     * @return
     */
    public static JexlNode buildNode(ASTERNode original, String fieldName, String fieldValue) {
        ASTERNode newNode = new ASTERNode(ParserTreeConstants.JJTERNODE);
        if (null != original) {
            newNode.jjtSetParent(original.jjtGetParent());
        }
        return buildUntypedNewNode(newNode, buildIdentifier(fieldName), fieldValue);
    }
    
    /**
     * Create a new ASTERNode from the given field name and value
     * 
     * @param original
     * @param fieldName
     * @param fieldValue
     * @return
     */
    public static JexlNode buildNode(ASTERNode original, String fieldName, Number fieldValue) {
        ASTERNode newNode = new ASTERNode(ParserTreeConstants.JJTERNODE);
        if (null != original) {
            newNode.jjtSetParent(original.jjtGetParent());
        }
        return buildUntypedNewNode(newNode, buildIdentifier(fieldName), fieldValue);
    }
    
    /**
     * Create a new ASTERNode from the given field name and value
     * 
     * @param original
     * @param fieldName
     * @param fieldValue
     * @return
     */
    public static JexlNode buildNode(ASTERNode original, String fieldName, JexlNode fieldValue) {
        ASTERNode newNode = new ASTERNode(ParserTreeConstants.JJTERNODE);
        if (null != original) {
            newNode.jjtSetParent(original.jjtGetParent());
        }
        return buildUntypedNewNode(newNode, buildIdentifier(fieldName), fieldValue);
    }
    
    /**
     * Create a new ASTNRNode from the given field name and value
     * 
     * @param original
     * @param fieldName
     * @param fieldValue
     * @return
     */
    public static JexlNode buildNode(ASTNRNode original, String fieldName, String fieldValue) {
        ASTNRNode newNode = new ASTNRNode(ParserTreeConstants.JJTNRNODE);
        if (null != original) {
            newNode.jjtSetParent(original.jjtGetParent());
        }
        return buildUntypedNewNode(newNode, buildIdentifier(fieldName), fieldValue);
    }
    
    /**
     * Create a new ASTNRNode from the given field name and value
     * 
     * @param original
     * @param fieldName
     * @param fieldValue
     * @return
     */
    public static JexlNode buildNode(ASTNRNode original, String fieldName, Number fieldValue) {
        ASTNRNode newNode = new ASTNRNode(ParserTreeConstants.JJTNRNODE);
        if (null != original) {
            newNode.jjtSetParent(original.jjtGetParent());
        }
        return buildUntypedNewNode(newNode, buildIdentifier(fieldName), fieldValue);
    }
    
    /**
     * Create a new ASTNRNode from the given field name and value
     * 
     * @param original
     * @param fieldName
     * @param fieldValue
     * @return
     */
    public static JexlNode buildNode(ASTNRNode original, String fieldName, JexlNode fieldValue) {
        ASTNRNode newNode = new ASTNRNode(ParserTreeConstants.JJTNRNODE);
        if (null != original) {
            newNode.jjtSetParent(original.jjtGetParent());
        }
        return buildUntypedNewNode(newNode, buildIdentifier(fieldName), fieldValue);
    }
    
    /**
     * Create a new ASTLTNode from the given field name and value
     * 
     * @param original
     * @param fieldName
     * @param fieldValue
     * @return
     */
    public static JexlNode buildNode(ASTLTNode original, String fieldName, String fieldValue) {
        ASTLTNode newNode = new ASTLTNode(ParserTreeConstants.JJTLTNODE);
        if (null != original) {
            newNode.jjtSetParent(original.jjtGetParent());
        }
        return buildUntypedNewNode(newNode, buildIdentifier(fieldName), fieldValue);
    }
    
    /**
     * Create a new ASTLTNode from the given field name and value
     * 
     * @param original
     * @param fieldName
     * @param fieldValue
     * @return
     */
    public static JexlNode buildNode(ASTLTNode original, String fieldName, Number fieldValue) {
        ASTLTNode newNode = new ASTLTNode(ParserTreeConstants.JJTLTNODE);
        if (null != original) {
            newNode.jjtSetParent(original.jjtGetParent());
        }
        return buildUntypedNewNode(newNode, buildIdentifier(fieldName), fieldValue);
    }
    
    /**
     * Create a new ASTLTNode from the given field name and value
     * 
     * @param original
     * @param fieldName
     * @param fieldValue
     * @return
     */
    public static JexlNode buildNode(ASTLTNode original, String fieldName, JexlNode fieldValue) {
        ASTLTNode newNode = new ASTLTNode(ParserTreeConstants.JJTLTNODE);
        if (null != original) {
            newNode.jjtSetParent(original.jjtGetParent());
        }
        return buildUntypedNewNode(newNode, buildIdentifier(fieldName), fieldValue);
    }
    
    /**
     * Create a new ASTLENode from the given field name and value
     * 
     * @param original
     * @param fieldName
     * @param fieldValue
     * @return
     */
    public static JexlNode buildNode(ASTLENode original, String fieldName, String fieldValue) {
        ASTLENode newNode = new ASTLENode(ParserTreeConstants.JJTLENODE);
        if (null != original) {
            newNode.jjtSetParent(original.jjtGetParent());
        }
        return buildUntypedNewNode(newNode, buildIdentifier(fieldName), fieldValue);
    }
    
    /**
     * Create a new ASTLENOde from the given field name and value
     * 
     * @param original
     * @param fieldName
     * @param fieldValue
     * @return
     */
    public static JexlNode buildNode(ASTLENode original, String fieldName, Number fieldValue) {
        ASTLENode newNode = new ASTLENode(ParserTreeConstants.JJTLENODE);
        if (null != original) {
            newNode.jjtSetParent(original.jjtGetParent());
        }
        return buildUntypedNewNode(newNode, buildIdentifier(fieldName), fieldValue);
    }
    
    /**
     * Create a new ASTLENOde from the given field name and value
     * 
     * @param original
     * @param fieldName
     * @param fieldValue
     * @return
     */
    public static JexlNode buildNode(ASTLENode original, String fieldName, JexlNode fieldValue) {
        ASTLENode newNode = new ASTLENode(ParserTreeConstants.JJTLENODE);
        if (null != original) {
            newNode.jjtSetParent(original.jjtGetParent());
        }
        return buildUntypedNewNode(newNode, buildIdentifier(fieldName), fieldValue);
    }
    
    /**
     * Create a new ASTGTNode from the given field name and value
     * 
     * @param original
     * @param fieldName
     * @param fieldValue
     * @return
     */
    public static JexlNode buildNode(ASTGTNode original, String fieldName, String fieldValue) {
        ASTGTNode newNode = new ASTGTNode(ParserTreeConstants.JJTGTNODE);
        if (null != original) {
            newNode.jjtSetParent(original.jjtGetParent());
        }
        return buildUntypedNewNode(newNode, buildIdentifier(fieldName), fieldValue);
    }
    
    /**
     * Create a new ASTGTNode from the given field name and value
     * 
     * @param original
     * @param fieldName
     * @param fieldValue
     * @return
     */
    public static JexlNode buildNode(ASTGTNode original, String fieldName, Number fieldValue) {
        ASTGTNode newNode = new ASTGTNode(ParserTreeConstants.JJTGTNODE);
        if (null != original) {
            newNode.jjtSetParent(original.jjtGetParent());
        }
        return buildUntypedNewNode(newNode, buildIdentifier(fieldName), fieldValue);
    }
    
    /**
     * Create a new ASTGTNode from the given field name and value
     * 
     * @param original
     * @param fieldName
     * @param fieldValue
     * @return
     */
    public static JexlNode buildNode(ASTGTNode original, String fieldName, JexlNode fieldValue) {
        ASTGTNode newNode = new ASTGTNode(ParserTreeConstants.JJTGTNODE);
        if (null != original) {
            newNode.jjtSetParent(original.jjtGetParent());
        }
        return buildUntypedNewNode(newNode, buildIdentifier(fieldName), fieldValue);
    }
    
    /**
     * Create a new ASTGENode from the given field name and value
     * 
     * @param original
     * @param fieldName
     * @param fieldValue
     * @return
     */
    public static JexlNode buildNode(ASTGENode original, String fieldName, String fieldValue) {
        ASTGENode newNode = new ASTGENode(ParserTreeConstants.JJTGENODE);
        if (null != original) {
            newNode.jjtSetParent(original.jjtGetParent());
        }
        return buildUntypedNewNode(newNode, buildIdentifier(fieldName), fieldValue);
    }
    
    /**
     * Create a new ASTGENode from the given field name and value
     * 
     * @param original
     * @param fieldName
     * @param fieldValue
     * @return
     */
    public static JexlNode buildNode(ASTGENode original, String fieldName, Number fieldValue) {
        ASTGENode newNode = new ASTGENode(ParserTreeConstants.JJTGENODE);
        if (null != original) {
            newNode.jjtSetParent(original.jjtGetParent());
        }
        return buildUntypedNewNode(newNode, buildIdentifier(fieldName), fieldValue);
    }
    
    /**
     * Create a new ASTGENode from the given field name and value
     * 
     * @param original
     * @param fieldName
     * @param fieldValue
     * @return
     */
    public static JexlNode buildNode(ASTGENode original, String fieldName, JexlNode fieldValue) {
        ASTGENode newNode = new ASTGENode(ParserTreeConstants.JJTGENODE);
        if (null != original) {
            newNode.jjtSetParent(original.jjtGetParent());
        }
        return buildUntypedNewNode(newNode, buildIdentifier(fieldName), fieldValue);
    }
    
    /**
     * Create a new JexlNode from the given node (possible an OR Node) and value
     *
     * @param original
     * @param node
     * @param fieldValue
     * @return
     */
    public static JexlNode buildNode(JexlNode original, JexlNode node, String fieldValue) {
        List<JexlNode> list = Lists.newArrayList();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode kid = node.jjtGetChild(i);
            Set<String> identifiers = JexlASTHelper.getIdentifierNames(kid);
            if (!identifiers.isEmpty()) {
                JexlNode newNode = JexlNodeFactory.shallowCopy(original);
                JexlNode n = buildUntypedNewNode(newNode, buildIdentifier(identifiers.iterator().next()), fieldValue);
                list.add(n);
            }
        }
        if (!list.isEmpty()) {
            return createOrNode(list);
        } else {
            JexlNode newNode = JexlNodeFactory.shallowCopy(original);
            return buildUntypedNewNode(newNode, buildIdentifier(node.image), fieldValue);
        }
    }
    
    /**
     * Create a new JexlNode from the given node (possible an OR Node) and value
     *
     * @param original
     * @param node
     * @param fieldValue
     * @return
     */
    public static JexlNode buildNode(JexlNode original, JexlNode node, Number fieldValue) {
        JexlNode newNode = JexlNodeFactory.shallowCopy(original);
        if (null != original) {
            newNode.jjtSetParent(original.jjtGetParent());
        }
        List<JexlNode> list = Lists.newArrayList();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode kid = node.jjtGetChild(i);
            list.add(buildUntypedNewNode(newNode, (ASTIdentifier) JexlNodeFactory.shallowCopy(kid), fieldValue));
        }
        if (!list.isEmpty()) {
            return createOrNode(list);
        }
        return null;
    }
    
    /**
     * Create a new JexlNode from the given node (possible an OR Node) and value
     *
     * @param original
     * @param node
     * @param fieldValue
     * @return
     */
    public static JexlNode buildNode(JexlNode original, ASTOrNode node, JexlNode fieldValue) {
        JexlNode newNode = JexlNodeFactory.shallowCopy(original);
        if (null != original) {
            newNode.jjtSetParent(original.jjtGetParent());
        }
        List<JexlNode> list = Lists.newArrayList();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode kid = node.jjtGetChild(i);
            list.add(buildUntypedNewNode(newNode, (ASTIdentifier) JexlNodeFactory.shallowCopy(kid), fieldValue));
        }
        if (!list.isEmpty()) {
            return createOrNode(list);
        } else {
            return buildUntypedNewNode(newNode, buildIdentifier(node.image), fieldValue);
        }
    }
    
    /**
     * Create a new ASTGENode from the given field name and value
     * 
     * @param namespace
     * @param function
     * @param field
     * @param args
     * @return
     */
    public static JexlNode buildFunctionNode(String namespace, String function, String field, Object... args) {
        ASTFunctionNode newNode = new ASTFunctionNode(ParserTreeConstants.JJTFUNCTIONNODE);
        JexlNodes.ensureCapacity(newNode, args.length + 3);
        int childIndex = 0;
        ASTIdentifier nsNode = new ASTIdentifier(ParserTreeConstants.JJTIDENTIFIER);
        nsNode.image = namespace;
        newNode.jjtAddChild(nsNode, childIndex++);
        nsNode.jjtSetParent(newNode);
        ASTIdentifier functionNode = new ASTIdentifier(ParserTreeConstants.JJTIDENTIFIER);
        functionNode.image = function;
        newNode.jjtAddChild(functionNode, childIndex++);
        functionNode.jjtSetParent(newNode);
        ASTIdentifier fieldNode = new ASTIdentifier(ParserTreeConstants.JJTIDENTIFIER);
        fieldNode.image = field;
        newNode.jjtAddChild(fieldNode, childIndex++);
        fieldNode.jjtSetParent(newNode);
        for (int i = 0; i < args.length; i++) {
            ASTStringLiteral literal = new ASTStringLiteral(ParserTreeConstants.JJTSTRINGLITERAL);
            literal.image = args[i].toString();
            newNode.jjtAddChild(JexlNodeFactory.wrap(literal), childIndex);
            newNode.jjtGetChild(childIndex++).jjtSetParent(newNode);
        }
        return JexlNodeFactory.wrap(newNode);
    }
    
    /**
     * Assign the field name and value to the given newNode
     * 
     * @param newNode
     * @param fieldName
     * @param fieldValue
     * @return
     */
    protected static JexlNode buildUntypedNewNode(JexlNode newNode, ASTIdentifier fieldName, String fieldValue) {
        ASTStringLiteral literal = new ASTStringLiteral(ParserTreeConstants.JJTSTRINGLITERAL);
        literal.image = fieldValue;
        
        return buildUntypedNewNode(newNode, fieldName, literal);
    }
    
    /**
     * Build a JexlNode with a Number literal
     * 
     * @param fieldName
     * @param fieldValue
     * @return
     */
    protected static JexlNode buildUntypedNewNode(JexlNode newNode, ASTIdentifier fieldName, Number fieldValue) {
        ASTNumberLiteral literal = new ASTNumberLiteral(ParserTreeConstants.JJTNUMBERLITERAL);
        literal.image = fieldValue.toString();
        
        if (NATURAL_NUMBERS.contains(fieldValue.getClass())) {
            literal.setNatural(fieldValue.toString());
        } else if (REAL_NUMBERS.contains(fieldValue.getClass())) {
            literal.setReal(fieldValue.toString());
        } else {
            QueryException qe = new QueryException(DatawaveErrorCode.ASTNUMBERLITERAL_TYPE_ASCERTAIN_ERROR, MessageFormat.format("{0}", literal));
            throw new DatawaveFatalQueryException(qe);
        }
        
        return buildUntypedNewNode(newNode, fieldName, literal);
    }
    
    private static ASTNumberLiteral getLiteral(Number fieldValue) {
        ASTNumberLiteral literal = new ASTNumberLiteral(ParserTreeConstants.JJTNUMBERLITERAL);
        literal.image = fieldValue.toString();
        
        if (NATURAL_NUMBERS.contains(fieldValue.getClass())) {
            literal.setNatural(fieldValue.toString());
        } else if (REAL_NUMBERS.contains(fieldValue.getClass())) {
            literal.setReal(fieldValue.toString());
        } else {
            QueryException qe = new QueryException(DatawaveErrorCode.ASTNUMBERLITERAL_TYPE_ASCERTAIN_ERROR, MessageFormat.format("{0}", literal));
            throw new DatawaveFatalQueryException(qe);
        }
        return literal;
    }
    
    private static ASTStringLiteral getLiteral(String fieldValue) {
        ASTStringLiteral literal = new ASTStringLiteral(ParserTreeConstants.JJTSTRINGLITERAL);
        literal.image = fieldValue;
        return literal;
    }
    
    /**
     * Given the provide newNode, add the fieldName (as an identifier) and the literal
     * 
     * @param newNode
     * @param identifier
     * @param literal
     * @return
     */
    protected static JexlNode buildUntypedNewNode(JexlNode newNode, ASTIdentifier identifier, JexlNode literal) {
        ASTReference literalReference = new ASTReference(ParserTreeConstants.JJTREFERENCE), identifierReference = new ASTReference(
                        ParserTreeConstants.JJTREFERENCE);
        
        literalReference.jjtAddChild(literal, 0);
        literal.jjtSetParent(literalReference);
        
        identifierReference.jjtAddChild(identifier, 0);
        identifier.jjtSetParent(identifierReference);
        
        newNode.jjtAddChild(identifierReference, 0);
        newNode.jjtAddChild(literalReference, 1);
        
        identifierReference.jjtSetParent(newNode);
        literalReference.jjtSetParent(newNode);
        
        return newNode;
    }
    
    protected static JexlNode buildUntypedNewNode(JexlNode newNode, JexlNode literal, ASTIdentifier identifier) {
        ASTReference literalReference = new ASTReference(ParserTreeConstants.JJTREFERENCE), identifierReference = new ASTReference(
                        ParserTreeConstants.JJTREFERENCE);
        
        literalReference.jjtAddChild(literal, 0);
        literal.jjtSetParent(literalReference);
        
        identifierReference.jjtAddChild(identifier, 0);
        identifier.jjtSetParent(identifierReference);
        
        newNode.jjtAddChild(literalReference, 0);
        newNode.jjtAddChild(identifierReference, 1);
        
        identifierReference.jjtSetParent(newNode);
        literalReference.jjtSetParent(newNode);
        
        return newNode;
    }
    
    /**
     * Like {@link #buildUntypedNewNode(JexlNode, ASTIdentifier, String)} except it does not wrap {@code literal} in an {@link ASTReference}
     * 
     * @param newNode
     * @param identifier
     * @param literal
     * @return
     */
    protected static JexlNode buildUntypedNewLiteralNode(JexlNode newNode, ASTIdentifier identifier, JexlNode literal) {
        ASTReference identifierReference = new ASTReference(ParserTreeConstants.JJTREFERENCE);
        identifierReference.jjtAddChild(identifier, 0);
        identifier.jjtSetParent(identifierReference);
        
        newNode.jjtAddChild(identifierReference, 0);
        newNode.jjtAddChild(literal, 1);
        
        identifierReference.jjtSetParent(newNode);
        literal.jjtSetParent(newNode);
        
        return newNode;
    }
    
    protected static JexlNode buildUntypedDblIdentifierNode(JexlNode newNode, JexlNode identifier1, JexlNode identifier2) {
        ASTReference identifierReference1 = new ASTReference(ParserTreeConstants.JJTREFERENCE);
        identifierReference1.jjtAddChild(identifier1, 0);
        identifier1.jjtSetParent(identifierReference1);
        
        ASTReference identifierReference2 = new ASTReference(ParserTreeConstants.JJTREFERENCE);
        identifierReference2.jjtAddChild(identifier2, 0);
        identifier2.jjtSetParent(identifierReference2);
        
        newNode.jjtAddChild(identifierReference1, 0);
        newNode.jjtAddChild(identifierReference2, 1);
        
        identifierReference1.jjtSetParent(newNode);
        identifierReference2.jjtSetParent(newNode);
        
        return newNode;
    }
    
    protected static JexlNode buildUntypedDblLiteralNode(JexlNode newNode, JexlNode literal1, JexlNode literal2) {
        newNode.jjtAddChild(literal1, 0);
        newNode.jjtAddChild(literal2, 1);
        
        literal1.jjtSetParent(newNode);
        literal2.jjtSetParent(newNode);
        
        return newNode;
    }
    
    public static JexlNode buildBooleanNode(JexlNode original, ASTIdentifier identifier, Boolean literal) {
        JexlNode copy;
        if (original instanceof ASTEQNode) {
            copy = new ASTEQNode(ParserTreeConstants.JJTEQNODE);
        } else if (original instanceof ASTNENode) {
            copy = new ASTNENode(ParserTreeConstants.JJTNENODE);
        } else if (original instanceof ASTERNode) {
            copy = new ASTERNode(ParserTreeConstants.JJTERNODE);
        } else if (original instanceof ASTNRNode) {
            copy = new ASTNRNode(ParserTreeConstants.JJTNRNODE);
        } else if (original instanceof ASTGTNode) {
            copy = new ASTGTNode(ParserTreeConstants.JJTGTNODE);
        } else if (original instanceof ASTGENode) {
            copy = new ASTGENode(ParserTreeConstants.JJTGENODE);
        } else if (original instanceof ASTLTNode) {
            copy = new ASTLTNode(ParserTreeConstants.JJTLTNODE);
        } else if (original instanceof ASTLENode) {
            copy = new ASTLENode(ParserTreeConstants.JJTLENODE);
        } else {
            throw new UnsupportedOperationException("Cannot handle " + original);
        }
        
        copy.jjtSetParent(original.jjtGetParent());
        
        JexlNode literalNode;
        if (literal) {
            literalNode = new ASTTrueNode(ParserTreeConstants.JJTTRUENODE);
        } else {
            literalNode = new ASTFalseNode(ParserTreeConstants.JJTFALSENODE);
        }
        
        return buildUntypedNewLiteralNode(copy, identifier, literalNode);
    }
    
    public static JexlNode buildNullNode(JexlNode original, ASTIdentifier identifier) {
        JexlNode copy;
        if (original instanceof ASTEQNode) {
            copy = new ASTEQNode(ParserTreeConstants.JJTEQNODE);
        } else if (original instanceof ASTNENode) {
            copy = new ASTNENode(ParserTreeConstants.JJTNENODE);
        } else if (original instanceof ASTERNode) {
            copy = new ASTERNode(ParserTreeConstants.JJTERNODE);
        } else if (original instanceof ASTNRNode) {
            copy = new ASTNRNode(ParserTreeConstants.JJTNRNODE);
        } else if (original instanceof ASTGTNode) {
            copy = new ASTGTNode(ParserTreeConstants.JJTGTNODE);
        } else if (original instanceof ASTGENode) {
            copy = new ASTGENode(ParserTreeConstants.JJTGENODE);
        } else if (original instanceof ASTLTNode) {
            copy = new ASTLTNode(ParserTreeConstants.JJTLTNODE);
        } else if (original instanceof ASTLENode) {
            copy = new ASTLENode(ParserTreeConstants.JJTLENODE);
        } else {
            throw new UnsupportedOperationException("Cannot handle " + original);
        }
        
        copy.jjtSetParent(original.jjtGetParent());
        
        ASTNullLiteral literalNode = new ASTNullLiteral(ParserTreeConstants.JJTNULLLITERAL);
        
        return buildUntypedNewLiteralNode(copy, identifier, literalNode);
    }
    
    public static ASTIdentifier buildIdentifier(String fieldName) {
        ASTIdentifier identifier = new ASTIdentifier(ParserTreeConstants.JJTIDENTIFIER);
        // When the fieldName starts with an invalid character, add the prefix
        // onto it
        identifier.image = JexlASTHelper.rebuildIdentifier(fieldName);
        
        return identifier;
    }
    
    /**
     * Creates a reference expression fro a child node
     * 
     * @param child
     * @return
     */
    public static JexlNode createExpression(JexlNode child) {
        if (child instanceof ASTReference && child.jjtGetChild(0) instanceof ASTReferenceExpression) {
            return child;
        }
        
        ASTReference ref = new ASTReference(ParserTreeConstants.JJTREFERENCE);
        ref.jjtSetParent(child.jjtGetParent());
        
        if (child instanceof ASTReferenceExpression) {
            child.jjtSetParent(ref);
            ref.jjtAddChild(child, 0);
        } else {
            ASTReferenceExpression exp = new ASTReferenceExpression(ParserTreeConstants.JJTREFERENCEEXPRESSION);
            
            child.jjtSetParent(exp);
            exp.jjtAddChild(child, 0);
            
            exp.jjtSetParent(ref);
            ref.jjtAddChild(exp, 0);
        }
        
        return ref;
    }
    
    /**
     * Creates a reference expression fro a child node
     * 
     * @param childContainer
     * @param wrappingContainer
     * @return
     */
    public static JexlNode createExpression(JexlNode childContainer, JexlNode wrappingContainer) {
        
        ASTReference ref = new ASTReference(ParserTreeConstants.JJTREFERENCE);
        
        ASTReferenceExpression exp = new ASTReferenceExpression(ParserTreeConstants.JJTREFERENCEEXPRESSION);
        
        for (int i = 0; i < childContainer.jjtGetNumChildren(); i++) {
            JexlNode child = childContainer.jjtGetChild(i);
            child.jjtSetParent(ref);
            exp.jjtAddChild(child, i);
            i++;
        }
        
        exp.jjtSetParent(ref);
        ref.jjtAddChild(exp, 0);
        
        JexlNode newWrapper = shallowCopy(wrappingContainer);
        
        ref.jjtSetParent(newWrapper);
        
        newWrapper.jjtAddChild(ref, 0);
        
        return newWrapper;
    }
    
    /**
     * Create an assignment node
     * 
     * @param name
     * @param value
     * @return the assignment node
     */
    public static ASTAssignment createAssignment(String name, boolean value) {
        ASTAssignment assignNode = new ASTAssignment(ParserTreeConstants.JJTASSIGNMENT);
        
        ASTReference refNode2 = new ASTReference(ParserTreeConstants.JJTREFERENCE);
        refNode2.jjtSetParent(assignNode);
        assignNode.jjtAddChild(refNode2, 0);
        
        ASTIdentifier idNode = new ASTIdentifier(ParserTreeConstants.JJTIDENTIFIER);
        idNode.image = name;
        idNode.jjtSetParent(refNode2);
        refNode2.jjtAddChild(idNode, 0);
        
        if (value) {
            ASTTrueNode trueNode = new ASTTrueNode(ParserTreeConstants.JJTTRUENODE);
            trueNode.jjtSetParent(assignNode);
            assignNode.jjtAddChild(trueNode, 1);
        } else {
            ASTFalseNode falseNode = new ASTFalseNode(ParserTreeConstants.JJTFALSENODE);
            falseNode.jjtSetParent(assignNode);
            assignNode.jjtAddChild(falseNode, 1);
        }
        
        return assignNode;
    }
    
    /**
     * Create an assignment node
     * 
     * @param name
     * @param value
     * @return the assignment node
     */
    public static ASTAssignment createAssignment(String name, String value) {
        ASTAssignment assignNode = new ASTAssignment(ParserTreeConstants.JJTASSIGNMENT);
        
        ASTReference refNode2 = new ASTReference(ParserTreeConstants.JJTREFERENCE);
        refNode2.jjtSetParent(assignNode);
        assignNode.jjtAddChild(refNode2, 0);
        
        ASTIdentifier idNode = new ASTIdentifier(ParserTreeConstants.JJTIDENTIFIER);
        idNode.image = name;
        idNode.jjtSetParent(refNode2);
        refNode2.jjtAddChild(idNode, 0);
        
        ASTStringLiteral literalNode = new ASTStringLiteral(ParserTreeConstants.JJTSTRINGLITERAL);
        literalNode.jjtSetParent(assignNode);
        literalNode.image = value;
        assignNode.jjtAddChild(literalNode, 1);
        
        return assignNode;
    }
}
