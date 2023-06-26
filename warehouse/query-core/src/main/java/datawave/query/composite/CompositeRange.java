package datawave.query.composite;

import com.google.common.collect.Sets;
import datawave.data.type.DiscreteIndexType;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.LiteralRange;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.JexlNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A composite range is a special type of composite which is used to create a single bounded range from multiple terms. Composite ranges can only be created
 * when the base composite term produces ranges whose terms, and underlying data within that range are of fixed length.
 *
 */
public class CompositeRange extends Composite {

    public static final Set<Class<?>> INVALID_LEAF_NODE_CLASSES = Collections.unmodifiableSet(Sets.newHashSet(ASTNENode.class, ASTERNode.class));
    public static final Set<Class<?>> VALID_LEAF_NODE_CLASSES = Collections
                    .unmodifiableSet(Sets.newHashSet(ASTAndNode.class, ASTEQNode.class, ASTGTNode.class, ASTGENode.class, ASTLTNode.class, ASTLENode.class));

    private final List<JexlNode> jexlNodeListLowerBound = new ArrayList<>();
    private final List<JexlNode> jexlNodeListUpperBound = new ArrayList<>();
    private final List<String> expressionListLowerBound = new ArrayList<>();
    private final List<String> expressionListUpperBound = new ArrayList<>();

    public CompositeRange(String compositeName, String separator) {
        super(compositeName, separator);
    }

    public CompositeRange(Composite other) {
        this(other.compositeName, other.separator);
        for (JexlNode node : other.jexlNodeList)
            addComponent(node);
    }

    @Override
    public Composite clone() {
        return new CompositeRange(this);
    }

    @Override
    public void addComponent(JexlNode node) {
        List<JexlNode> nodes = new ArrayList<>();
        LiteralRange range = JexlASTHelper.findRange().getRange(node);
        if (range != null) {
            nodes.add(range.getLowerNode());
            nodes.add(range.getUpperNode());
        } else if (node instanceof ASTEQNode) {
            String fieldName = JexlASTHelper.getIdentifier(node);
            String expression = JexlASTHelper.getLiteralValue(node).toString();
            nodes.add(JexlNodeFactory.buildNode((ASTGENode) null, fieldName, expression));
            nodes.add(JexlNodeFactory.buildNode((ASTLENode) null, fieldName, expression));
        } else {
            nodes.add(node);
        }

        jexlNodeList.add(node);
        fieldNameList.add(JexlASTHelper.getIdentifier(nodes.get(0)));

        for (JexlNode boundNode : nodes) {
            String expression = JexlASTHelper.getLiteralValue(boundNode).toString();
            if (boundNode instanceof ASTGENode || boundNode instanceof ASTGTNode) {
                jexlNodeListLowerBound.add(boundNode);
                expressionListLowerBound.add(expression);
                if (nodes.size() < 2) {
                    jexlNodeListUpperBound.add(null);
                    expressionListUpperBound.add(null);
                }
            } else if (boundNode instanceof ASTLENode || boundNode instanceof ASTLTNode) {
                jexlNodeListUpperBound.add(boundNode);
                expressionListUpperBound.add(expression);
                if (nodes.size() < 2) {
                    jexlNodeListLowerBound.add(null);
                    expressionListLowerBound.add(null);
                }
            }
        }
    }

    @Override
    public String toString() {
        return "CompositeRange [compositeName=" + compositeName + ", fieldNameList=" + fieldNameList + ", jexlNodeList=" + jexlNodeList + ", expressionList="
                        + expressionList + ", jexlNodeListLowerBound=" + jexlNodeListLowerBound + ", expressionListLowerBound=" + expressionListLowerBound
                        + ", jexlNodeListUpperBound=" + jexlNodeListUpperBound + ", expressionListUpperBound=" + expressionListUpperBound + "]";
    }

    private boolean isLowerUnbounded() {
        for (int i = 0; i < jexlNodeList.size(); i++)
            if (jexlNodeListLowerBound.get(i) == null && jexlNodeListUpperBound.get(i) != null)
                return true;
        return false;
    }

    private boolean isUpperUnbounded() {
        for (int i = 0; i < jexlNodeList.size(); i++)
            if (jexlNodeListUpperBound.get(i) == null && jexlNodeListLowerBound.get(i) != null)
                return true;
        return false;
    }

    @Override
    public void getNodesAndExpressions(List<Class<? extends JexlNode>> nodeClasses, List<String> expressions,
                    Map<String,DiscreteIndexType<?>> discreteIndexTypeMap, boolean includeOldData) {
        if (includeOldData) {
            expressions.add(getFullyInclusiveLowerBoundExpression(discreteIndexTypeMap));
            nodeClasses.add(ASTGENode.class);
        } else {
            Class<? extends JexlNode> lowerBoundNodeClass = getLowerBoundNodeClass();
            String lowerBoundExpression = getLowerBoundExpression(discreteIndexTypeMap);
            if (lowerBoundNodeClass != null && !lowerBoundExpression.equals("")) {
                nodeClasses.add(lowerBoundNodeClass);
                expressions.add(lowerBoundExpression);
            }
        }
        Class<? extends JexlNode> upperBoundNodeClass = getUpperBoundNodeClass();
        String upperBoundExpression = getUpperBoundExpression(discreteIndexTypeMap);
        if (upperBoundNodeClass != null && !upperBoundExpression.equals("")) {
            nodeClasses.add(upperBoundNodeClass);
            expressions.add(upperBoundExpression);
        }
    }

    private Class<? extends JexlNode> getLowerBoundNodeClass() {
        // get the last non-null jexl node class
        Class<? extends JexlNode> nodeClass = jexlNodeListLowerBound.stream().filter(Objects::nonNull).map(JexlNode::getClass).reduce((x, y) -> y).orElse(null);
        // if we don't have an expression for each component field, and this is a GT node, return GE. Otherwise, return whatever the class of the last node is
        if (expressionListLowerBound.get(expressionListLowerBound.size() - 1) == null && nodeClass != null && nodeClass.equals(ASTGTNode.class))
            return ASTGENode.class;
        return nodeClass;
    }

    private Class<? extends JexlNode> getUpperBoundNodeClass() {
        // get the last non-null jexl node class
        Class<? extends JexlNode> nodeClass = jexlNodeListUpperBound.stream().filter(Objects::nonNull).map(JexlNode::getClass).reduce((x, y) -> y).orElse(null);
        // if we don't have an expression for each component field, and this is a LE node, return LT. Otherwise, return whatever the class of the last node is
        if (expressionListUpperBound.get(expressionListUpperBound.size() - 1) == null && nodeClass != null && nodeClass.equals(ASTLENode.class))
            return ASTLTNode.class;
        return nodeClass;
    }

    // used to handle special case where our index is overloaded and runs against legacy (i.e. non-composite) data
    private String getFullyInclusiveLowerBoundExpression(Map<String,DiscreteIndexType<?>> discreteIndexTypeMap) {
        String expression;
        if (jexlNodeListLowerBound.get(0) instanceof ASTGTNode)
            expression = CompositeUtils.getInclusiveLowerBound(expressionListLowerBound.get(0), discreteIndexTypeMap.get(fieldNameList.get(0)));
        else
            expression = expressionListLowerBound.get(0);
        return expression;
    }

    private String getLowerBoundExpression(Map<String,DiscreteIndexType<?>> discreteIndexTypeMap) {
        StringBuilder buf = new StringBuilder();
        boolean lastNode = false;
        for (int i = 0; i < expressionListLowerBound.size(); i++) {
            String expression = expressionListLowerBound.get(i);
            JexlNode node = jexlNodeListLowerBound.get(i);

            if (expression != null && node != null) {
                // Special handling for GT nodes
                // only the LAST component field can be >, all others must be >=
                if (node instanceof ASTGTNode && i != (jexlNodeListLowerBound.size() - 1)) {
                    String inclusiveLowerBound = CompositeUtils.getInclusiveLowerBound(expression, discreteIndexTypeMap.get(fieldNameList.get(i)));

                    // if the length of the term changed, use the original exclusive
                    // bound, and signal that this is the last expression
                    if (inclusiveLowerBound.length() != expression.length())
                        lastNode = true;
                    else
                        expression = inclusiveLowerBound;
                }

                if (i > 0)
                    buf.append(separator);

                buf.append(expression);
            } else {
                break;
            }

            if (lastNode)
                break;
        }
        return buf.toString();
    }

    private String getUpperBoundExpression(Map<String,DiscreteIndexType<?>> discreteIndexTypeMap) {
        StringBuilder buf = new StringBuilder();
        boolean lastNode = false;
        for (int i = 0; i < expressionListUpperBound.size(); i++) {
            String expression = expressionListUpperBound.get(i);
            JexlNode node = jexlNodeListUpperBound.get(i);

            if (expression != null && node != null) {
                if (i != (expressionListUpperBound.size() - 1)) {
                    // Convert LE node to LT node if it is the last valid node in the list
                    if (node instanceof ASTLENode && expressionListUpperBound.get(i + 1) == null) {
                        String exclusiveUpperBound = CompositeUtils.getExclusiveUpperBound(expression, discreteIndexTypeMap.get(fieldNameList.get(i)));

                        // if the length of the term changed, use the original exclusive
                        // bound, and signal that this is the last expression
                        if (exclusiveUpperBound.length() != expression.length())
                            lastNode = true;
                        else
                            expression = exclusiveUpperBound;
                    }
                    // Convert LT nodes to inclusive LE nodes if they are not the last valid node in the list
                    else if (node instanceof ASTLTNode && expressionListUpperBound.get(i + 1) != null) {
                        String inclusiveUpperBound = CompositeUtils.getInclusiveUpperBound(expression, discreteIndexTypeMap.get(fieldNameList.get(i)));

                        // if the length of the term changed, use the original exclusive
                        // bound, and signal that this is the last expression
                        if (inclusiveUpperBound.length() != expression.length())
                            lastNode = true;
                        else
                            expression = inclusiveUpperBound;
                    }
                }

                if (i > 0)
                    buf.append(separator);

                buf.append(expression);
            } else {
                break;
            }

            if (lastNode)
                break;
        }
        return buf.toString();
    }

    // this composite range is invalid if
    // - it doesn't contain any nodes, or they are all null
    // - it contains an invalid leaf node, or doesn't contain a valid leaf node
    // - it contains a regex node
    // - it represents an unbounded range (i.e. contains all GT/GE nodes, or all LT/LE nodes)
    // - there are gaps in the upper or lower bound
    @Override
    public boolean isValid() {
        // if we have no nodes, or they are all null
        if (jexlNodeList.isEmpty() || jexlNodeList.stream().allMatch(Objects::isNull) || jexlNodeListLowerBound.stream().allMatch(Objects::isNull)
                        || jexlNodeListUpperBound.stream().allMatch(Objects::isNull))
            return false;

        boolean allGTOrGENodes = true;
        boolean allLTOrLENodes = true;
        for (JexlNode node : jexlNodeList) {
            Class nodeClass = node.getClass();

            // if this is an invalid leaf node, or not a valid leaf node, we're done
            if (INVALID_LEAF_NODE_CLASSES.contains(nodeClass) || !VALID_LEAF_NODE_CLASSES.contains(nodeClass))
                return false;

            // regex and not equals nodes are not allowed in a bounded range
            if (node instanceof ASTERNode || node instanceof ASTNENode)
                return false;

            // keeping track of whether all the nodes are GT or GE
            if (!(node instanceof ASTGTNode || node instanceof ASTGENode))
                allGTOrGENodes = false;

            // keeping track of whether all the nodes are LT or LE
            if (!(node instanceof ASTLTNode || node instanceof ASTLENode))
                allLTOrLENodes = false;
        }

        // there's no value in creating composites for unbounded ranges
        if (allGTOrGENodes || allLTOrLENodes)
            return false;

        // look for gaps in the lower bound. trailing nulls are ok, but nulls followed by non-nulls are not
        boolean hasNullNode = false;
        for (JexlNode lowerNode : jexlNodeListLowerBound) {
            if (hasNullNode && lowerNode != null)
                return false;
            hasNullNode = hasNullNode || lowerNode == null;
        }

        // look for gaps in the upper bound. trailing nulls are ok, but nulls followed by non-nulls are not
        hasNullNode = false;
        for (JexlNode upperNode : jexlNodeListUpperBound) {
            if (hasNullNode && upperNode != null)
                return false;
            hasNullNode = hasNullNode || upperNode == null;
        }

        return true;
    }

    public boolean contains(JexlNode node) {
        boolean success;
        LiteralRange range = JexlASTHelper.findRange().getRange(node);
        if (range != null)
            success = this.jexlNodeListLowerBound.contains(range.getLowerNode()) && this.jexlNodeListUpperBound.contains(range.getUpperNode());
        else if (node instanceof ASTEQNode)
            success = this.jexlNodeList.contains(node);
        else
            success = this.jexlNodeListLowerBound.contains(node) || this.jexlNodeListUpperBound.contains(node);
        return success;
    }

    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((expressionListLowerBound == null) ? 0 : expressionListLowerBound.hashCode());
        result = prime * result + ((jexlNodeListLowerBound == null) ? 0 : jexlNodeListLowerBound.hashCode());
        result = prime * result + ((expressionListUpperBound == null) ? 0 : expressionListUpperBound.hashCode());
        result = prime * result + ((jexlNodeListUpperBound == null) ? 0 : jexlNodeListUpperBound.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        if (!super.equals(obj))
            return false;
        CompositeRange other = (CompositeRange) obj;
        if (expressionListLowerBound == null) {
            if (other.expressionListLowerBound != null)
                return false;
        } else if (!expressionListLowerBound.equals(other.expressionListLowerBound))
            return false;
        if (jexlNodeListLowerBound == null) {
            if (other.jexlNodeListLowerBound != null)
                return false;
        } else if (!jexlNodeListLowerBound.equals(other.jexlNodeListLowerBound))
            return false;
        if (expressionListUpperBound == null) {
            if (other.expressionListUpperBound != null)
                return false;
        } else if (!expressionListUpperBound.equals(other.expressionListUpperBound))
            return false;
        if (jexlNodeListUpperBound == null) {
            if (other.jexlNodeListUpperBound != null)
                return false;
        } else if (!jexlNodeListUpperBound.equals(other.jexlNodeListUpperBound))
            return false;
        return true;
    }

    public List<JexlNode> getJexlNodeListLowerBound() {
        return jexlNodeListLowerBound;
    }

    public List<JexlNode> getJexlNodeListUpperBound() {
        return jexlNodeListUpperBound;
    }

    public List<String> getExpressionListLowerBound() {
        return expressionListLowerBound;
    }

    public List<String> getExpressionListUpperBound() {
        return expressionListUpperBound;
    }
}
