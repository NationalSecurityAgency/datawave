package datawave.query.jexl;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_VALUE;
import static datawave.query.jexl.visitors.RebuildingVisitor.copy;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTAddNode;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTArguments;
import org.apache.commons.jexl3.parser.ASTAssignment;
import org.apache.commons.jexl3.parser.ASTBitwiseAndNode;
import org.apache.commons.jexl3.parser.ASTBitwiseComplNode;
import org.apache.commons.jexl3.parser.ASTBitwiseOrNode;
import org.apache.commons.jexl3.parser.ASTBitwiseXorNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTEmptyFunction;
import org.apache.commons.jexl3.parser.ASTFalseNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTMethodNode;
import org.apache.commons.jexl3.parser.ASTMulNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTNamespaceIdentifier;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.ASTNullLiteral;
import org.apache.commons.jexl3.parser.ASTNumberLiteral;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.ASTStringLiteral;
import org.apache.commons.jexl3.parser.ASTSubNode;
import org.apache.commons.jexl3.parser.ASTTrueNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.commons.jexl3.parser.ParserTreeConstants;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import datawave.query.Constants;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.lookups.IndexLookupMap;
import datawave.query.jexl.lookups.ValueSet;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

/**
 * Factory methods that can create JexlNodes
 *
 *
 *
 */
public class JexlNodeFactory {
    public static final Set<Class<?>> REAL_NUMBERS = Collections.unmodifiableSet(Sets.<Class<?>> newHashSet(BigDecimal.class, Double.class, Float.class));
    public static final Set<Class<?>> NATURAL_NUMBERS = Collections
                    .unmodifiableSet(Sets.<Class<?>> newHashSet(Long.class, BigInteger.class, Integer.class, Short.class, Byte.class));

    public enum ContainerType {
        OR_NODE, AND_NODE
    }

    /**
     * Expand a node given a mapping of fields to values. If the list is empty, then the original regex should be used.
     *
     * @param containerType
     *            should we create OR nodes or AND nodes
     * @param isNegated
     *            is this node negated?
     * @param original
     *            the original JexlNode
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
    public static JexlNode createNodeTreeFromFieldsToValues(ContainerType containerType, boolean isNegated, JexlNode original, IndexLookupMap fieldsToValues,
                    boolean expandFields, boolean expandValues, boolean keepOriginalNode) {
        // do nothing if not expanding fields or values
        if (!expandFields && !expandValues) {
            return original;
        }

        // no expansions needed if the field name threshold is exceeded
        if (fieldsToValues.isKeyThresholdExceeded()) {
            throw new DatawaveFatalQueryException("Failed to expand unfielded term");
        }

        // collapse the value sets if not expanding fields
        if (!expandFields) {
            ValueSet allValues = new ValueSet(-1);
            for (ValueSet values : fieldsToValues.values()) {
                allValues.addAll(values);
            }
            fieldsToValues.clear();
            for (String identifier : JexlASTHelper.getIdentifierNames(original)) {
                fieldsToValues.put(identifier, allValues);
            }
        }

        List<JexlNode> children = new LinkedList<>();
        Set<String> fields = fieldsToValues.keySet();
        if (keepOriginalNode) {
            JexlNode child = copy(original);
            children.add(child);
            // remove this entry from the fieldsToValues to avoid duplication
            Set<String> identifiers = JexlASTHelper.getIdentifierNames(original);
            for (Object value : JexlASTHelper.getLiteralValues(original)) {
                identifiers.forEach((id) -> fieldsToValues.remove(id, value));
            }
        }

        for (String field : fields) {
            JexlNode child = createNodeFromValuesForField(field, fieldsToValues, original, containerType, expandValues, isNegated);
            if (child != null) {
                children.add(child);
            }
        }

        switch (children.size()) {
            case 0:
                if (isNegated) {
                    // retain negated regex nodes that find nothing in the global index
                    return original;
                } else {
                    // a positive regex that found nothing gets pruned with a FALSE node.
                    return new ASTFalseNode(ParserTreeConstants.JJTFALSENODE);
                }
            case 1:
                // Only one child node was generated, just return that
                return children.get(0);
            default:
                // create the proper parent node and add children
                JexlNode junction;
                if (containerType.equals(ContainerType.OR_NODE)) {
                    junction = new ASTOrNode(ParserTreeConstants.JJTORNODE);
                } else {
                    junction = new ASTAndNode(ParserTreeConstants.JJTANDNODE);
                }
                return JexlNodes.setChildren(junction, children.toArray(new JexlNode[0]));
        }
    }

    /**
     * Helper method for {@link JexlNodeFactory#createNodeTreeFromFieldsToValues(ContainerType, boolean, JexlNode, IndexLookupMap, boolean, boolean, boolean)}
     *
     * @param field
     *            create a node for this field
     * @param fieldsToValues
     *            an {@link datawave.query.jexl.lookups.IndexLookup}
     * @param original
     *            the original JexlNode
     * @param type
     *            create an ASTOrNode or an ASTAndNode
     * @param expandValues
     *            should we expand values
     * @param isNegated
     *            boolean indicating if the original node is negated
     * @return a JexlNode
     */
    public static JexlNode createNodeFromValuesForField(String field, IndexLookupMap fieldsToValues, JexlNode original, ContainerType type,
                    boolean expandValues, boolean isNegated) {

        ValueSet valuesForField = fieldsToValues.get(field);

        if (!expandValues) {

            // If not expanding values replace the original node's '_ANYFIELD_' with the specified field
            JexlNode child = copy(original);
            for (ASTIdentifier identifier : JexlASTHelper.getIdentifiers(child)) {
                if (identifier.getName().equals(Constants.ANY_FIELD)) {
                    JexlNodes.setIdentifier(identifier, field);
                }
            }
            return child;

        } else if (valuesForField.isThresholdExceeded()) {

            // a threshold exceeded set of values requires using the original
            // node with a new field name, wrapped with a marker node

            // create a set of nodes wrapping each pattern
            List<String> patterns = new ArrayList<>(fieldsToValues.getPatterns() == null ? new ArrayList<>() : fieldsToValues.getPatterns());
            if (patterns.isEmpty()) {
                return QueryPropertyMarker.create(buildUntypedNode(copy(original), field), EXCEEDED_VALUE);
            } else if (patterns.size() == 1) {
                return QueryPropertyMarker.create(buildUntypedNode(copy(original), field, patterns.get(0)), EXCEEDED_VALUE);
            } else {

                JexlNode junction;
                List<JexlNode> children = new LinkedList<>();

                if (type.equals(ContainerType.OR_NODE)) {
                    junction = new ASTOrNode(ParserTreeConstants.JJTORNODE);
                } else {
                    junction = new ASTAndNode(ParserTreeConstants.JJTANDNODE);
                }

                for (String pattern : patterns) {
                    JexlNode child = QueryPropertyMarker.create(buildUntypedNode(copy(original), field, pattern), EXCEEDED_VALUE);
                    children.add(child);
                }

                switch (children.size()) {
                    case 0:
                        return null;
                    case 1:
                        return children.get(0);
                    default:
                        return JexlNodes.setChildren(junction, children.toArray(new JexlNode[0]));
                }
            }

        } else if (1 == valuesForField.size()) {

            // Don't create an OR if we have only one value, directly attach it
            String value = valuesForField.iterator().next();
            return isNegated ? JexlNodeFactory.buildNENode(field, value) : JexlNodeFactory.buildEQNode(field, value);

        } else {

            JexlNode junction;
            List<JexlNode> children = new LinkedList<>();

            if (type.equals(ContainerType.OR_NODE)) {
                junction = new ASTOrNode(ParserTreeConstants.JJTORNODE);
            } else {
                junction = new ASTAndNode(ParserTreeConstants.JJTANDNODE);
            }

            for (String value : valuesForField) {
                JexlNode child = isNegated ? JexlNodeFactory.buildNENode(field, value) : JexlNodeFactory.buildEQNode(field, value);
                children.add(child);
            }

            switch (children.size()) {
                case 0:
                    return null;
                case 1:
                    return children.get(0);
                default:
                    return JexlNodes.setChildren(junction, children.toArray(new JexlNode[0]));
            }
        }
    }

    public static JexlNode createNodeTreeFromFieldNames(ContainerType containerType, JexlNode node, Object literal, Collection<String> fieldNames) {

        // A single field:term doesn't need to be an OR
        if (1 == fieldNames.size()) {
            return buildUntypedNode(node, fieldNames.iterator().next(), literal);
        }

        JexlNode parentNode = (containerType.equals(ContainerType.OR_NODE) ? new ASTOrNode(ParserTreeConstants.JJTORNODE)
                        : new ASTAndNode(ParserTreeConstants.JJTANDNODE));
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
     *            a node
     * @param containerType
     *            the container type
     * @param orgNode
     *            the org node
     * @param fieldName
     *            the field name
     * @param fieldValues
     *            the field values
     * @return a node reference or for the fieldname and values
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

        JexlNode parentNode = (containerType.equals(ContainerType.OR_NODE) ? new ASTOrNode(ParserTreeConstants.JJTORNODE)
                        : new ASTAndNode(ParserTreeConstants.JJTANDNODE));
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

        JexlNode parentNode = (containerType.equals(ContainerType.OR_NODE) ? new ASTOrNode(ParserTreeConstants.JJTORNODE)
                        : new ASTAndNode(ParserTreeConstants.JJTANDNODE));
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
     *            collection of nodes
     * @return a jexl node
     */
    public static JexlNode createAndNode(Iterable<? extends JexlNode> children) {
        return setChildren(new ASTAndNode(ParserTreeConstants.JJTANDNODE), children);
    }

    /**
     * Wrap a collection of JexlNodes into an ASTOrNode
     *
     * @param children
     *            collection of nodes
     * @return a jexl node
     */
    public static JexlNode createOrNode(Iterable<? extends JexlNode> children) {
        return setChildren(new ASTOrNode(ParserTreeConstants.JJTORNODE), children);
    }

    /**
     * Add the children JexlNodes to the parent JexlNode, correctly setting parent pointers, parenthesis, and reference nodes.
     *
     * @param parent
     *            the parent node
     * @param children
     *            collection of nodes
     * @return a jexl node
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
     *            the node to wrap
     * @return a wrapped node
     */
    public static JexlNode wrap(JexlNode toWrap) {
        if ((toWrap instanceof ASTAndNode || toWrap instanceof ASTOrNode) && toWrap.jjtGetNumChildren() > 1) {
            ASTReferenceExpression parens = JexlNodes.makeRefExp();
            parens.jjtAddChild(toWrap, 0);
            toWrap.jjtSetParent(parens);

            return parens;
        }

        return toWrap;
    }

    /**
     * Create an ASTJexlScript with the provided child
     *
     * @param child
     *            a child node
     * @return a jexl script
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
        JexlNode copy = shallowCopy(original);
        copy.jjtAddChild(left, 0);
        copy.jjtAddChild(right, 1);

        left.jjtSetParent(copy);
        right.jjtSetParent(copy);

        return copy;
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
            return buildNewLiteralNode(original, fieldValue);
        } else if (original instanceof ASTNENode) {
            return buildNewLiteralNode(original, fieldValue);
        } else if (original instanceof ASTERNode) {
            return buildNewLiteralNode(original, fieldValue);
        } else if (original instanceof ASTNRNode) {
            return buildNewLiteralNode(original, fieldValue);
        } else if (original instanceof ASTGTNode) {
            return buildNewLiteralNode(original, fieldValue);
        } else if (original instanceof ASTGENode) {
            return buildNewLiteralNode(original, fieldValue);
        } else if (original instanceof ASTLTNode) {
            return buildNewLiteralNode(original, fieldValue);
        } else if (original instanceof ASTLENode) {
            return buildNewLiteralNode(original, fieldValue);
        } else {
            throw new UnsupportedOperationException("Cannot handle " + original);
        }
    }

    public static JexlNode buildUntypedNode(JexlNode original, String fieldName) {
        // required to handle ranges that could not be expanded
        if (original instanceof ASTAndNode || original instanceof ASTOrNode || original instanceof ASTReferenceExpression || original instanceof ASTAssignment
                        || original instanceof ASTIdentifier || original instanceof ASTTrueNode) {
            JexlNode newNode = shallowCopy(original);
            newNode.jjtSetParent(original.jjtGetParent());
            for (int i = 0; i < original.jjtGetNumChildren(); i++) {
                JexlNode newChild = buildUntypedNode(original.jjtGetChild(i), fieldName);
                newNode.jjtAddChild(newChild, i);
                newChild.jjtSetParent(newNode);
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
            return buildNewLiteralNode(original, fieldValue);
        } else if (original instanceof ASTNENode) {
            return buildNewLiteralNode(original, fieldValue);
        } else if (original instanceof ASTERNode) {
            return buildNewLiteralNode(original, fieldValue);
        } else if (original instanceof ASTNRNode) {
            return buildNewLiteralNode(original, fieldValue);
        } else if (original instanceof ASTGTNode) {
            return buildNewLiteralNode(original, fieldValue);
        } else if (original instanceof ASTGENode) {
            return buildNewLiteralNode(original, fieldValue);
        } else if (original instanceof ASTLTNode) {
            return buildNewLiteralNode(original, fieldValue);
        } else if (original instanceof ASTLENode) {
            return buildNewLiteralNode(original, fieldValue);
        } else {
            throw new UnsupportedOperationException("Cannot handle " + original);
        }
    }

    public static JexlNode buildEQNode(String fieldName, String fieldValue) {
        return buildNode((ASTEQNode) null, fieldName, fieldValue);
    }

    public static JexlNode buildNENode(String fieldName, String fieldValue) {
        return buildNode((ASTNENode) null, fieldName, fieldValue);
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
     *            a jexl node
     * @return a new jexl node
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
            copy = JexlNodes.makeIdentifier();
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
            copy = JexlNodes.makeNumberLiteral();
            JexlNodes.setLiteral((ASTNumberLiteral) copy, ((ASTNumberLiteral) original).getLiteral());
        } else if (ASTOrNode.class.isAssignableFrom(clz)) {
            copy = new ASTOrNode(ParserTreeConstants.JJTORNODE);
        } else if (ASTStringLiteral.class.isAssignableFrom(clz)) {
            copy = JexlNodes.makeStringLiteral();
            JexlNodes.setLiteral((ASTStringLiteral) copy, ((ASTStringLiteral) original).getLiteral());
        } else if (ASTTrueNode.class.isAssignableFrom(clz)) {
            copy = new ASTTrueNode(ParserTreeConstants.JJTTRUENODE);
        } else if (ASTReferenceExpression.class.isAssignableFrom(clz)) {
            copy = JexlNodes.makeRefExp();
        } else if (ASTReference.class.isAssignableFrom(clz)) {
            copy = new ASTReference(ParserTreeConstants.JJTREFERENCE);
        } else if (ASTAddNode.class.isAssignableFrom(clz)) {
            copy = new ASTAddNode(ParserTreeConstants.JJTADDNODE);
        } else if (ASTSubNode.class.isAssignableFrom(clz)) {
            copy = new ASTSubNode(ParserTreeConstants.JJTSUBNODE);
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
        JexlNodes.copyIdentifierOrLiteral(original, copy);
        return copy;
    }

    /**
     * Create a new ASTEQNode from the given field name and value
     *
     * @param original
     *            the original node
     * @param fieldName
     *            a field name
     * @param fieldValue
     *            a field value
     * @return a new node
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
     *            the original node
     * @param fieldName
     *            a field name
     * @param fieldValue
     *            a field value
     * @return a new node
     */
    public static JexlNode buildNode(ASTEQNode original, String fieldName, Number fieldValue) {
        ASTEQNode newNode = new ASTEQNode(ParserTreeConstants.JJTEQNODE);
        if (null != original) {
            newNode.jjtSetParent(original.jjtGetParent());
        }
        return buildUntypedNewNode(newNode, buildIdentifier(fieldName), fieldValue);
    }

    public static JexlNode buildNewLiteralNode(JexlNode original, Number fieldValue) {
        replaceLiteralNode(original, getLiteral(fieldValue));
        return original;
    }

    public static JexlNode buildNewLiteralNode(JexlNode original, String fieldValue) {
        replaceLiteralNode(original, getLiteral(fieldValue));
        return original;
    }

    public static void replaceLiteralNode(JexlNode original, JexlNode literalNode) {
        // is the literal the first or second child?
        if (JexlASTHelper.getLiteral(original.jjtGetChild(0)) != null) {
            // if it's the first child, replace the first
            original.jjtAddChild(literalNode, 0);
        } else {
            // if it's the second child, replace the second
            original.jjtAddChild(literalNode, 1);
        }

        literalNode.jjtSetParent(original);
    }

    /**
     * Create a new ASTEQNode from the given field name and value
     *
     * @param original
     *            the original node
     * @param fieldName
     *            a field name
     * @param fieldValue
     *            a field value
     * @return a new node
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
     *            the original node
     * @param fieldName
     *            a field name
     * @param fieldValue
     *            a field value
     * @return a new node
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
     *            the original node
     * @param fieldName
     *            a field name
     * @param fieldValue
     *            a field value
     * @return a new node
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
     *            the original node
     * @param fieldName
     *            a field name
     * @param fieldValue
     *            a field value
     * @return a new node
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
     *            the original node
     * @param fieldName
     *            a field name
     * @param fieldValue
     *            a field value
     * @return a new node
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
     *            the original node
     * @param fieldName
     *            a field name
     * @param fieldValue
     *            a field value
     * @return a new node
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
     *            the original node
     * @param fieldName
     *            a field name
     * @param fieldValue
     *            a field value
     * @return a new node
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
     *            the original node
     * @param fieldName
     *            a field name
     * @param fieldValue
     *            a field value
     * @return a new node
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
     *            the original node
     * @param fieldName
     *            a field name
     * @param fieldValue
     *            a field value
     * @return a new node
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
     *            the original node
     * @param fieldName
     *            a field name
     * @param fieldValue
     *            a field value
     * @return a new node
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
     *            the original node
     * @param fieldName
     *            a field name
     * @param fieldValue
     *            a field value
     * @return a new node
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
     *            the original node
     * @param fieldName
     *            a field name
     * @param fieldValue
     *            a field value
     * @return a new node
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
     *            the original node
     * @param fieldName
     *            a field name
     * @param fieldValue
     *            a field value
     * @return a new node
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
     *            the original node
     * @param fieldName
     *            a field name
     * @param fieldValue
     *            a field value
     * @return a new node
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
     *            the original node
     * @param fieldName
     *            a field name
     * @param fieldValue
     *            a field value
     * @return a new node
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
     *            the original node
     * @param fieldName
     *            a field name
     * @param fieldValue
     *            a field value
     * @return a new node
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
     *            the original node
     * @param fieldName
     *            a field name
     * @param fieldValue
     *            a field value
     * @return a new node
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
     *            the original node
     * @param fieldName
     *            a field name
     * @param fieldValue
     *            a field value
     * @return a new node
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
     *            the original node
     * @param fieldName
     *            a field name
     * @param fieldValue
     *            a field value
     * @return a new node
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
     *            the original node
     * @param fieldName
     *            a field name
     * @param fieldValue
     *            a field value
     * @return a new node
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
     *            the original node
     * @param fieldName
     *            a field name
     * @param fieldValue
     *            a field value
     * @return a new node
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
     *            the original node
     * @param fieldName
     *            a field name
     * @param fieldValue
     *            a field value
     * @return a new node
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
     *            the original node
     * @param node
     *            a jexl node
     * @param fieldValue
     *            a field value
     * @return a new node
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
            return buildUntypedNewNode(newNode, buildIdentifier(JexlNodes.getIdentifierOrLiteralAsString(node)), fieldValue);
        }
    }

    /**
     * Create a new JexlNode from the given node (possible an OR Node) and value
     *
     * @param original
     *            the original node
     * @param node
     *            a jexl node
     * @param fieldValue
     *            a field value
     * @return a new node
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
     *            the original node
     * @param node
     *            a jexl node
     * @param fieldValue
     *            a field value
     * @return a new node
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
            return buildUntypedNewNode(newNode, buildIdentifier(JexlNodes.getIdentifierOrLiteralAsString(node)), fieldValue);
        }
    }

    /**
     * Create a new ASTGENode from the given field name and value
     *
     * @param namespace
     *            namespace string
     * @param function
     *            function string
     * @param field
     *            the field string
     * @param args
     *            the arguments
     * @return a new node
     */
    public static JexlNode buildFunctionNode(String namespace, String function, String field, Object... args) {
        ASTFunctionNode functionNode = new ASTFunctionNode(ParserTreeConstants.JJTFUNCTIONNODE);

        ASTNamespaceIdentifier namespaceNode = new ASTNamespaceIdentifier(ParserTreeConstants.JJTNAMESPACEIDENTIFIER);
        namespaceNode.setNamespace(namespace, function);
        functionNode.jjtAddChild(namespaceNode, 0);
        namespaceNode.jjtSetParent(functionNode);

        ASTArguments argsNode = new ASTArguments(ParserTreeConstants.JJTARGUMENTS);
        functionNode.jjtAddChild(argsNode, 1);
        argsNode.jjtSetParent(functionNode);

        int childIndex = 0;

        ASTIdentifier fieldNode = JexlNodes.makeIdentifier(field);
        argsNode.jjtAddChild(fieldNode, childIndex++);
        fieldNode.jjtSetParent(argsNode);

        for (Object arg : args) {
            ASTStringLiteral literal = JexlNodes.makeStringLiteral();
            JexlNodes.setLiteral(literal, arg.toString());
            literal.jjtSetParent(argsNode);

            argsNode.jjtAddChild(JexlNodeFactory.wrap(literal), childIndex++);
        }

        return functionNode;
    }

    /**
     * Assign the field name and value to the given newNode
     *
     * @param newNode
     *            a new node
     * @param fieldName
     *            the field name string
     * @param fieldValue
     *            the field value string
     * @return a untyped new node
     */
    protected static JexlNode buildUntypedNewNode(JexlNode newNode, ASTIdentifier fieldName, String fieldValue) {
        ASTStringLiteral literal = JexlNodes.makeStringLiteral();
        JexlNodes.setLiteral(literal, fieldValue);
        return buildUntypedNewNode(newNode, fieldName, literal);
    }

    /**
     * Build a JexlNode with a Number literal
     *
     * @param newNode
     *            the new node
     * @param fieldName
     *            a field name
     * @param fieldValue
     *            a field value
     * @return a jexl node
     */
    protected static JexlNode buildUntypedNewNode(JexlNode newNode, ASTIdentifier fieldName, Number fieldValue) {
        ASTNumberLiteral literal = JexlNodes.makeNumberLiteral();

        if (!JexlNodes.setLiteral(literal, fieldValue)) {
            QueryException qe = new QueryException(DatawaveErrorCode.ASTNUMBERLITERAL_TYPE_ASCERTAIN_ERROR, MessageFormat.format("{0}", literal));
            throw new DatawaveFatalQueryException(qe);
        }

        return buildUntypedNewNode(newNode, fieldName, literal);
    }

    private static ASTNumberLiteral getLiteral(Number fieldValue) {
        ASTNumberLiteral literal = JexlNodes.makeNumberLiteral();

        if (!JexlNodes.setLiteral(literal, fieldValue)) {
            QueryException qe = new QueryException(DatawaveErrorCode.ASTNUMBERLITERAL_TYPE_ASCERTAIN_ERROR, MessageFormat.format("{0}", literal));
            throw new DatawaveFatalQueryException(qe);
        }
        return literal;
    }

    private static ASTStringLiteral getLiteral(String fieldValue) {
        ASTStringLiteral literal = JexlNodes.makeStringLiteral();
        JexlNodes.setLiteral(literal, fieldValue);
        return literal;
    }

    /**
     * Given the provide newNode, add the fieldName (as an identifier) and the literal
     *
     * @param newNode
     *            the new node
     * @param identifier
     *            the field name identifier
     * @param literal
     *            the node literal
     * @return a jexl node
     */
    protected static JexlNode buildUntypedNewNode(JexlNode newNode, ASTIdentifier identifier, JexlNode literal) {
        newNode.jjtAddChild(identifier, 0);
        newNode.jjtAddChild(literal, 1);

        identifier.jjtSetParent(newNode);
        literal.jjtSetParent(newNode);

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

        return buildUntypedNewNode(copy, identifier, literalNode);
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

        return buildUntypedNewNode(copy, identifier, literalNode);
    }

    public static ASTIdentifier buildIdentifier(String fieldName) {
        // When the fieldName starts with an invalid character, add the prefix
        // onto it
        return JexlNodes.makeIdentifier(JexlASTHelper.rebuildIdentifier(fieldName));
    }

    /**
     * Creates a reference expression fro a child node
     *
     * @param child
     *            a child node
     * @return a jexl node
     */
    public static JexlNode createExpression(JexlNode child) {
        if (child instanceof ASTReferenceExpression) {
            return child;
        } else {
            ASTReferenceExpression exp = JexlNodes.makeRefExp();
            child.jjtSetParent(exp);
            exp.jjtAddChild(child, 0);

            return exp;
        }
    }

    /**
     * Create an assignment node
     *
     * @param name
     *            the name string
     * @param value
     *            the value string
     * @return the assignment node
     */
    public static ASTAssignment createAssignment(String name, boolean value) {
        ASTAssignment assignNode = new ASTAssignment(ParserTreeConstants.JJTASSIGNMENT);

        ASTIdentifier idNode = JexlNodes.makeIdentifier(name);
        idNode.jjtSetParent(assignNode);
        assignNode.jjtAddChild(idNode, 0);
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
     *            the name string
     * @param value
     *            the value string
     * @return the assignment node
     */
    public static ASTAssignment createAssignment(String name, String value) {
        ASTAssignment assignNode = new ASTAssignment(ParserTreeConstants.JJTASSIGNMENT);

        ASTIdentifier idNode = JexlNodes.makeIdentifier(name);
        idNode.jjtSetParent(assignNode);
        assignNode.jjtAddChild(idNode, 0);

        ASTStringLiteral literalNode = JexlNodes.makeStringLiteral();
        literalNode.jjtSetParent(assignNode);
        JexlNodes.setLiteral(literalNode, value);
        assignNode.jjtAddChild(literalNode, 1);

        return assignNode;
    }
}
