package datawave.query.composite;

import com.google.common.collect.Sets;
import datawave.data.type.DiscreteIndexType;
import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.JexlNode;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A composite is used to combine multiple terms into a single term.
 *
 */
public class CompositeTerm extends Composite {
    
    public static final Set<Class<?>> INVALID_LEAF_NODE_CLASSES = Sets.newHashSet(ASTNENode.class, ASTGTNode.class, ASTGENode.class, ASTLTNode.class,
                    ASTLENode.class, ASTAndNode.class);
    public static final Set<Class<?>> VALID_LEAF_NODE_CLASSES = Sets.newHashSet(ASTEQNode.class, ASTERNode.class);
    
    public CompositeTerm(String compositeName, String separator) {
        super(compositeName, separator);
    }
    
    public CompositeTerm(Composite other) {
        this(other.compositeName, other.separator);
        for (JexlNode node : other.jexlNodeList)
            addComponent(node);
    }
    
    @Override
    public Composite clone() {
        return new CompositeRange(this);
    }
    
    public void addComponent(JexlNode node) {
        Object lit = JexlASTHelper.getLiteralValue(node);
        String identifier = JexlASTHelper.getIdentifier(node);
        jexlNodeList.add(node);
        fieldNameList.add(identifier);
        expressionList.add(lit.toString());
    }
    
    @Override
    public String toString() {
        return "CompositeTerm [compositeName=" + compositeName + ", separator=" + separator + ", fieldNameList=" + fieldNameList + ", jexlNodeList="
                        + jexlNodeList + ", expressionList=" + expressionList + "]";
    }
    
    public void getNodesAndExpressions(List<Class<? extends JexlNode>> nodeClasses, List<String> expressions,
                    Map<String,DiscreteIndexType<?>> discreteIndexFieldMap, boolean includeOldData) {
        Class nodeClass = (jexlNodeList.stream().anyMatch(x -> x instanceof ASTERNode)) ? ASTERNode.class : ASTEQNode.class;
        String expression = getAppendedExpressions();
        
        if (includeOldData) {
            if (nodeClass.equals(ASTEQNode.class)) {
                expressions.add(expressionList.get(0));
                nodeClasses.add(ASTGENode.class);
                
                expressions.add(expression);
                nodeClasses.add(ASTLENode.class);
            } else if (nodeClass.equals(ASTERNode.class)) {
                expressions.add(expressionList.get(0) + "|" + expression);
                nodeClasses.add(ASTERNode.class);
            }
        } else {
            nodeClasses.add(nodeClass);
            expressions.add(expression);
        }
    }
    
    /**
     *
     * @return
     */
    private String getAppendedExpressions() {
        return String.join(separator, expressionList);
    }
    
    // what this essentially boils down to is that the only valid nodes are equals or equals regex nodes
    // this composite is invalid if:
    // - it doesn't contain any nodes, or they are all null
    // - it contains an invalid leaf node, or doesn't contain a valid leaf node
    // - it contains an unbounded range node
    public boolean isValid() {
        // if we have no nodes, or they are all null
        if (jexlNodeList.isEmpty() || jexlNodeList.stream().allMatch(Objects::isNull))
            return false;
        
        for (JexlNode node : jexlNodeList) {
            Class nodeClass = node.getClass();
            
            // if this is an invalid leaf node, or not a valid leaf node, we're done
            if (INVALID_LEAF_NODE_CLASSES.contains(nodeClass) || !VALID_LEAF_NODE_CLASSES.contains(nodeClass))
                return false;
        }
        
        return true;
    }
    
    public boolean contains(JexlNode node) {
        return jexlNodeList.contains(node);
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((compositeName == null) ? 0 : compositeName.hashCode());
        result = prime * result + ((expressionList == null) ? 0 : expressionList.hashCode());
        result = prime * result + ((fieldNameList == null) ? 0 : fieldNameList.hashCode());
        result = prime * result + ((jexlNodeList == null) ? 0 : jexlNodeList.hashCode());
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
        CompositeTerm other = (CompositeTerm) obj;
        if (compositeName == null) {
            if (other.compositeName != null)
                return false;
        } else if (!compositeName.equals(other.compositeName))
            return false;
        if (expressionList == null) {
            if (other.expressionList != null)
                return false;
        } else if (!expressionList.equals(other.expressionList))
            return false;
        if (fieldNameList == null) {
            if (other.fieldNameList != null)
                return false;
        } else if (!fieldNameList.equals(other.fieldNameList))
            return false;
        if (jexlNodeList == null) {
            if (other.jexlNodeList != null)
                return false;
        } else if (!jexlNodeList.equals(other.jexlNodeList))
            return false;
        return true;
    }
    
}
