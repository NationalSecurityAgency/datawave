package datawave.query.composite;

import com.google.common.collect.Lists;
import datawave.query.jexl.JexlNodeFactory;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.JexlNode;

import java.util.Arrays;
import java.util.List;

/**
 * A composite is used to combine multiple terms into a single term.
 *
 */
public class Composite {
    
    public final String compositeName; // FU_BA_BAZ
    public final List<String> fieldNameList = Lists.newArrayList(); // [ FOO, BAR, BAZ ]
    public final List<JexlNode> jexlNodeList = Lists.newArrayList(); // [ JexlEQ, JexlEq, JexlER ]
    public final List<String> expressionList = Lists.newArrayList(); // [ 'moe', 'larry', 'cu*' ]
    
    public Composite(String compositeName) {
        this.compositeName = compositeName;
    }
    
    public Composite clone() {
        final Composite clone = new Composite(this.compositeName);
        for (String fieldName : this.fieldNameList) {
            clone.fieldNameList.add(new String(fieldName));
        }
        
        for (JexlNode jexlNode : this.jexlNodeList) {
            clone.jexlNodeList.add(jexlNode);
        }
        
        for (String expression : this.expressionList) {
            clone.expressionList.add(new String(expression));
        }
        
        return clone;
    }
    
    @Override
    public String toString() {
        return "Composite [compositeName=" + compositeName + ", fieldNameList=" + fieldNameList + ", jexlNodeList=" + jexlNodeList + ", expressionList="
                        + expressionList + "]";
    }
    
    protected Class<? extends JexlNode> getNodeClass(List<JexlNode> jexlNodeList) {
        JexlNode lastNode = null;
        boolean allEqOrRegexSoFar = true;
        boolean regexFound = false;
        for (JexlNode node : jexlNodeList) {
            if (node != null) {
                if (!(node instanceof ASTEQNode || node instanceof ASTERNode)) {
                    lastNode = node;
                    allEqOrRegexSoFar = false;
                } else if (allEqOrRegexSoFar) {
                    if (node instanceof ASTEQNode && !regexFound)
                        lastNode = node;
                    else if (node instanceof ASTERNode) {
                        regexFound = true;
                        lastNode = node;
                    }
                }
            } else
                break;
        }
        return lastNode.getClass();
    }
    
    public void getNodesAndExpressions(List<Class<? extends JexlNode>> nodeClasses, List<String> expressions, boolean includeOldData) {
        nodeClasses.addAll(Arrays.asList(getNodeClass(jexlNodeList)));
        expressions.addAll(Arrays.asList(getAppendedExpressions()));
        
        if (includeOldData) {
            JexlNode node = jexlNodeList.get(0);
            if (node instanceof ASTGTNode) {
                nodeClasses.clear();
                expressions.clear();
                
                expressions.add(CompositeUtils.getInclusiveLowerBound(expressionList.get(0)));
                nodeClasses.add(ASTGENode.class);
            } else if (node instanceof ASTGENode || node instanceof ASTEQNode) {
                String origExpression = expressions.get(0);
                
                nodeClasses.clear();
                expressions.clear();
                
                expressions.add(expressionList.get(0));
                nodeClasses.add(ASTGENode.class);
                
                if (node instanceof ASTEQNode) {
                    expressions.add(origExpression);
                    nodeClasses.add(ASTLENode.class);
                }
            }
        }
    }
    
    /**
     * stop at the first regex expression
     * 
     * @return
     */
    private String getAppendedExpressions() {
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < jexlNodeList.size(); i++) {
            JexlNode node = jexlNodeList.get(i);
            String expression = expressionList.get(i);
            
            buf.append(expression);
            
            if (i + 1 < jexlNodeList.size()) {
                buf.append(CompositeUtils.SEPARATOR);
            }
            
            if (node instanceof ASTERNode) {
                // this will be the last expression to append
                break;
            }
        }
        return buf.toString();
    }
    
    // this composite is invalid if:
    // - it contains a mix of GT/GE and LT/LE nodes
    // - it contains a 'regex' or 'not equals' node in any position other than the last position
    // - it contains a 'regex' or 'not equals' node in the last position, and any of the preceeding nodes are not 'equals' nodes
    // -- e.g. no unbounded ranges ending in a 'regex' or 'not equals' node
    public boolean isValid() {
        if (jexlNodeList.isEmpty())
            return false;
        
        boolean hasRegexNodes = false;
        boolean hasEqNodes = false;
        boolean hasGTOrGENodes = false;
        boolean hasLTOrLENodes = false;
        for (int i = 0; i < jexlNodeList.size(); i++) {
            JexlNode node = jexlNodeList.get(i);
            Class nodeClass = node.getClass();
            
            // if this is an invalid leaf node, or not a valid leaf node, we're done
            if (CompositeUtils.INVALID_LEAF_NODE_CLASSES.contains(nodeClass) || !CompositeUtils.VALID_LEAF_NODE_CLASSES.contains(nodeClass))
                return false;
            
            hasEqNodes |= (node instanceof ASTEQNode);
            hasRegexNodes |= (node instanceof ASTERNode);
            hasGTOrGENodes |= (node instanceof ASTGTNode || node instanceof ASTGENode);
            hasLTOrLENodes |= (node instanceof ASTLTNode || node instanceof ASTLENode);
            
            // can't combine opposing bounds
            if (hasGTOrGENodes && hasLTOrLENodes)
                return false;
            
            // can't combine regex with bounds
            if (hasRegexNodes && (hasGTOrGENodes || hasLTOrLENodes))
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
        Composite other = (Composite) obj;
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
