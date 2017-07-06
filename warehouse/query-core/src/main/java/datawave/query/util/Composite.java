package datawave.query.util;

import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;
import datawave.query.Constants;
import org.apache.commons.jexl2.parser.*;

import com.google.common.collect.Lists;

public class Composite {
    public static final String START_SEPARATOR = Constants.MAX_UNICODE_STRING;
    public static final String END_SEPARATOR = "";
    public static final Set<Class<?>> WILDCARD_NODE_CLASSES = Sets.<Class<?>> newHashSet(ASTNENode.class, ASTERNode.class, ASTGTNode.class, ASTGENode.class,
                    ASTLTNode.class, ASTLENode.class);
    public static final Set<Class<?>> LEAF_NODE_CLASSES = Sets.<Class<?>> newHashSet(ASTEQNode.class, ASTNENode.class, ASTERNode.class, ASTGTNode.class,
                    ASTGENode.class, ASTLTNode.class, ASTLENode.class);
    
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
    
    /**
     * stop at the first regex expression
     * 
     * @return
     */
    public String getAppendedExpressions() {
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < jexlNodeList.size(); i++) {
            JexlNode node = jexlNodeList.get(i);
            String expression = expressionList.get(i);
            
            buf.append(expression);
            
            if (i > 0) {
                buf.append(END_SEPARATOR);
            }
            
            if (i + 1 < jexlNodeList.size()) {
                buf.append(START_SEPARATOR);
            }
            
            if (node instanceof ASTERNode) {
                // this will be the last expression to append
                break;
            }
        }
        return buf.toString();
    }
    
    public boolean isValid() {
        return (this.jexlNodeList.size() >= 1 && !WILDCARD_NODE_CLASSES.contains(jexlNodeList.get(0).getClass()));
    }
    
    public boolean contains(JexlNode node) {
        return this.jexlNodeList.contains(node);
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
