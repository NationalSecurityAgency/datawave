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
    
    protected JexlNode getNodeClass(List<JexlNode> jexlNodeList) {
        JexlNode lastNode = null;
        boolean allEqSoFar = true;
        for (JexlNode node : jexlNodeList) {
            if (node != null) {
                if (allEqSoFar && node instanceof ASTEQNode)
                    lastNode = node;
                else if (!(node instanceof ASTEQNode)) {
                    lastNode = node;
                    allEqSoFar = false;
                }
            } else
                break;
        }
        return lastNode;
    }
    
    public void getNodesAndExpressions(List<JexlNode> nodes, List<String> expressions, boolean includeOldData) {
        nodes.addAll(Arrays.asList(getNodeClass(jexlNodeList)));
        expressions.addAll(Arrays.asList(getAppendedExpressions()));
        
        if (includeOldData) {
            JexlNode node = jexlNodeList.get(0);
            if (node instanceof ASTGTNode) {
                nodes.clear();
                expressions.clear();
                
                expressions.add(CompositeUtils.getInclusiveLowerBound(expressionList.get(0)));
                nodes.add(JexlNodeFactory.buildNode((ASTGENode) null, (String) null, (String) null));
            } else if (node instanceof ASTGENode || node instanceof ASTEQNode) {
                String origExpression = expressions.get(0);
                
                nodes.clear();
                expressions.clear();
                
                expressions.add(expressionList.get(0));
                nodes.add(JexlNodeFactory.buildNode((ASTGENode) null, (String) null, (String) null));
                
                if (node instanceof ASTEQNode) {
                    expressions.add(origExpression);
                    nodes.add(JexlNodeFactory.buildNode((ASTLENode) null, (String) null, (String) null));
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
        
        boolean allEqNodes = true;
        boolean hasGTOrGENodes = false;
        boolean hasLTOrLENodes = false;
        for (int i = 0; i < jexlNodeList.size(); i++) {
            JexlNode node = jexlNodeList.get(i);
            
            hasGTOrGENodes |= (node instanceof ASTGTNode || node instanceof ASTGENode);
            hasLTOrLENodes |= (node instanceof ASTLTNode || node instanceof ASTLENode);
            if (hasGTOrGENodes && hasLTOrLENodes)
                return false;
            
            // if this is a regex or not equals node, and this is either not the last node,
            // or was preceeded by something other than an equals node
            if (CompositeUtils.WILDCARD_NODE_CLASSES.contains(node.getClass()) && ((i + 1) != jexlNodeList.size() || !allEqNodes))
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
