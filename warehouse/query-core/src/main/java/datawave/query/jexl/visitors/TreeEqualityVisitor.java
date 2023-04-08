package datawave.query.jexl.visitors;

import datawave.core.common.logging.ThreadConfigurableLogger;
import org.apache.commons.jexl2.parser.ASTAdditiveNode;
import org.apache.commons.jexl2.parser.ASTAdditiveOperator;
import org.apache.commons.jexl2.parser.ASTAmbiguous;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTArrayAccess;
import org.apache.commons.jexl2.parser.ASTArrayLiteral;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTBitwiseAndNode;
import org.apache.commons.jexl2.parser.ASTBitwiseComplNode;
import org.apache.commons.jexl2.parser.ASTBitwiseOrNode;
import org.apache.commons.jexl2.parser.ASTBitwiseXorNode;
import org.apache.commons.jexl2.parser.ASTBlock;
import org.apache.commons.jexl2.parser.ASTConstructorNode;
import org.apache.commons.jexl2.parser.ASTDivNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTEmptyFunction;
import org.apache.commons.jexl2.parser.ASTFalseNode;
import org.apache.commons.jexl2.parser.ASTFloatLiteral;
import org.apache.commons.jexl2.parser.ASTForeachStatement;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTIfStatement;
import org.apache.commons.jexl2.parser.ASTIntegerLiteral;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTMapEntry;
import org.apache.commons.jexl2.parser.ASTMapLiteral;
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
import org.apache.commons.jexl2.parser.ASTReturnStatement;
import org.apache.commons.jexl2.parser.ASTSizeFunction;
import org.apache.commons.jexl2.parser.ASTSizeMethod;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.ASTTernaryNode;
import org.apache.commons.jexl2.parser.ASTTrueNode;
import org.apache.commons.jexl2.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl2.parser.ASTVar;
import org.apache.commons.jexl2.parser.ASTWhileStatement;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParserVisitor;
import org.apache.commons.jexl2.parser.SimpleNode;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Determine whether two trees are equivalent, accounting for arbitrary order within subtrees.
 */
public class TreeEqualityVisitor implements ParserVisitor {
    private static final Logger log = ThreadConfigurableLogger.getLogger(TreeEqualityVisitor.class);
    
    private boolean equal = true;
    
    public final static class Comparison {
        
        private static final Comparison IS_EQUAL = new Comparison(true, null);
        
        private static Comparison notEqual(String reason) {
            return new Comparison(false, reason);
        }
        
        private final boolean equal;
        private final String reason;
        
        private Comparison(boolean equal, String reason) {
            this.equal = equal;
            this.reason = reason;
        }
        
        public boolean isEqual() {
            return equal;
        }
        
        public String getReason() {
            return reason;
        }
    }
    
    /**
     * Return whether or not the provided query trees are considered equivalent.
     * 
     * @param first
     *            the first query tree
     * @param second
     *            the second query tree
     * @return true if the query trees are considered equivalent, or false otherwise
     */
    public static boolean isEqual(JexlNode first, JexlNode second) {
        return checkEquality(first, second).isEqual();
    }
    
    /**
     * Compare the provided query trees for equivalency and return the resulting comparison.
     * 
     * @param first
     *            the first query tree
     * @param second
     *            the second query tree
     * @return the comparison result
     */
    public static Comparison checkEquality(JexlNode first, JexlNode second) {
        if (first != null && second != null) {
            TreeEqualityVisitor visitor = new TreeEqualityVisitor();
            String reason = (String) first.jjtAccept(visitor, second);
            return visitor.equal ? Comparison.IS_EQUAL : Comparison.notEqual(reason);
        } else if (first == null && second == null) {
            return Comparison.IS_EQUAL;
        } else {
            return Comparison.notEqual("One tree is null: " + first + " vs " + second);
        }
    }
    
    /**
     * Accept the visitor on all this node's children.
     *
     * @param node1
     *            first node
     * @param node2
     *            second node
     * @return result of visit
     **/
    private Object visitEquality(SimpleNode node1, SimpleNode node2) {
        if (!equal) {
            return "Already not equal";
        } else if (!node1.getClass().equals(node2.getClass())) {
            equal = false;
            return "Classes differ: " + node1.getClass().getSimpleName() + " vs " + node2.getClass().getSimpleName();
        } else if (!equal(node1.jjtGetValue(), node2.jjtGetValue())) {
            equal = false;
            return ("Node values differ: " + node1.jjtGetValue() + " vs " + node2.jjtGetValue());
        } else if (node1 instanceof JexlNode && !equal(((JexlNode) node1).image, ((JexlNode) node2).image)) {
            equal = false;
            return ("Node images differ: " + ((JexlNode) node1).image + " vs " + ((JexlNode) node2).image);
        } else if (node1.jjtGetNumChildren() > 0 || node2.jjtGetNumChildren() > 0) {
            List<SimpleNode> list1 = listChildren(node1);
            List<SimpleNode> list2 = listChildren(node2);
            if (list1.size() != list2.size()) {
                if (log.isDebugEnabled()) {
                    log.debug("not equal " + list1.size() + " " + list2.size());
                }
                equal = false;
                return ("Num children differ: " + list1 + " vs " + list2);
            }
            Object reason = null;
            // start visiting recursively to find equality
            for (SimpleNode child1 : list1) {
                // compare the list1 node to each node in list2 until we find a match
                for (int i = 0; i < list2.size(); i++) {
                    SimpleNode child2 = list2.get(i);
                    equal = true;
                    reason = child1.jjtAccept(this, child2);
                    if (equal) { // equal may be made false by child
                        // found a match, break;
                        list2.remove(i);
                        break;
                    }
                }
                // if we get here with !equal, then we never found a match for a node...break out
                if (!equal) {
                    return "Did not find a matching child for " + child1 + " in " + list2 + ": " + reason;
                }
            }
        }
        return null;
    }
    
    private List<SimpleNode> listChildren(SimpleNode node) {
        List<SimpleNode> list = new ArrayList<>();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            list.add(node.jjtGetChild(i));
        }
        boolean changed = true;
        List<SimpleNode> newList = new ArrayList<>();
        while (changed) {
            changed = false;
            for (SimpleNode child : list) {
                // note the isAssignableFrom is to handle QueryPropertyMarker nodes
                if (((child.getClass().equals(ASTReference.class) || ASTReference.class.isAssignableFrom(child.getClass())) && (child.jjtGetNumChildren() == 1))
                                || (child.getClass().equals(ASTReferenceExpression.class) && (child.jjtGetNumChildren() == 1))
                                || (child.getClass().equals(ASTOrNode.class) && ((child.jjtGetNumChildren() == 1) || node.getClass().equals(ASTOrNode.class)))
                                || (child.getClass().equals(ASTAndNode.class)
                                                && ((child.jjtGetNumChildren() == 1) || node.getClass().equals(ASTAndNode.class)))) {
                    for (int j = 0; j < child.jjtGetNumChildren(); j++) {
                        newList.add(child.jjtGetChild(j));
                    }
                    changed = true;
                } else {
                    newList.add(child);
                }
            }
            List<SimpleNode> temp = newList;
            newList = list;
            list = temp;
            newList.clear();
        }
        return list;
    }
    
    public boolean equal(Object o1, Object o2) {
        if (o1 == o2) {
            return true;
        } else if (o1 == null || o2 == null) {
            return false;
        } else {
            return o1.equals(o2);
        }
    }
    
    @Override
    public Object visit(SimpleNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTJexlScript node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTBlock node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTAmbiguous node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTIfStatement node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTWhileStatement node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTForeachStatement node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTAssignment node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTTernaryNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTOrNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTBitwiseOrNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTBitwiseXorNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTBitwiseAndNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTNENode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTLTNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTLENode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTGENode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTNRNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTAdditiveNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTAdditiveOperator node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTMulNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTDivNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTModNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTUnaryMinusNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTBitwiseComplNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTNotNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTIdentifier node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTNullLiteral node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTTrueNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTFalseNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    public Object visit(ASTIntegerLiteral node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    public Object visit(ASTFloatLiteral node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTStringLiteral node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTArrayLiteral node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTMapLiteral node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTMapEntry node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTEmptyFunction node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTSizeFunction node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTMethodNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTSizeMethod node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTConstructorNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTArrayAccess node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTReference node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTReturnStatement node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTVar node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTNumberLiteral node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
}
