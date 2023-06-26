package datawave.query.jexl.visitors;

import datawave.core.common.logging.ThreadConfigurableLogger;
import org.apache.commons.jexl3.parser.ASTAddNode;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTAnnotatedStatement;
import org.apache.commons.jexl3.parser.ASTAnnotation;
import org.apache.commons.jexl3.parser.ASTArguments;
import org.apache.commons.jexl3.parser.ASTArrayAccess;
import org.apache.commons.jexl3.parser.ASTArrayLiteral;
import org.apache.commons.jexl3.parser.ASTAssignment;
import org.apache.commons.jexl3.parser.ASTBitwiseAndNode;
import org.apache.commons.jexl3.parser.ASTBitwiseComplNode;
import org.apache.commons.jexl3.parser.ASTBitwiseOrNode;
import org.apache.commons.jexl3.parser.ASTBitwiseXorNode;
import org.apache.commons.jexl3.parser.ASTBlock;
import org.apache.commons.jexl3.parser.ASTBreak;
import org.apache.commons.jexl3.parser.ASTConstructorNode;
import org.apache.commons.jexl3.parser.ASTContinue;
import org.apache.commons.jexl3.parser.ASTDecrementGetNode;
import org.apache.commons.jexl3.parser.ASTDefineVars;
import org.apache.commons.jexl3.parser.ASTDivNode;
import org.apache.commons.jexl3.parser.ASTDoWhileStatement;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTEWNode;
import org.apache.commons.jexl3.parser.ASTEmptyFunction;
import org.apache.commons.jexl3.parser.ASTExtendedLiteral;
import org.apache.commons.jexl3.parser.ASTFalseNode;
import org.apache.commons.jexl3.parser.ASTForeachStatement;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTGetDecrementNode;
import org.apache.commons.jexl3.parser.ASTGetIncrementNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTIdentifierAccess;
import org.apache.commons.jexl3.parser.ASTIfStatement;
import org.apache.commons.jexl3.parser.ASTIncrementGetNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTJxltLiteral;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTMapEntry;
import org.apache.commons.jexl3.parser.ASTMapLiteral;
import org.apache.commons.jexl3.parser.ASTMethodNode;
import org.apache.commons.jexl3.parser.ASTModNode;
import org.apache.commons.jexl3.parser.ASTMulNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNEWNode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTNSWNode;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.ASTNullLiteral;
import org.apache.commons.jexl3.parser.ASTNullpNode;
import org.apache.commons.jexl3.parser.ASTNumberLiteral;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTQualifiedIdentifier;
import org.apache.commons.jexl3.parser.ASTRangeNode;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.ASTRegexLiteral;
import org.apache.commons.jexl3.parser.ASTReturnStatement;
import org.apache.commons.jexl3.parser.ASTSWNode;
import org.apache.commons.jexl3.parser.ASTSetAddNode;
import org.apache.commons.jexl3.parser.ASTSetAndNode;
import org.apache.commons.jexl3.parser.ASTSetDivNode;
import org.apache.commons.jexl3.parser.ASTSetLiteral;
import org.apache.commons.jexl3.parser.ASTSetModNode;
import org.apache.commons.jexl3.parser.ASTSetMultNode;
import org.apache.commons.jexl3.parser.ASTSetOrNode;
import org.apache.commons.jexl3.parser.ASTSetShiftLeftNode;
import org.apache.commons.jexl3.parser.ASTSetShiftRightNode;
import org.apache.commons.jexl3.parser.ASTSetShiftRightUnsignedNode;
import org.apache.commons.jexl3.parser.ASTSetSubNode;
import org.apache.commons.jexl3.parser.ASTSetXorNode;
import org.apache.commons.jexl3.parser.ASTShiftLeftNode;
import org.apache.commons.jexl3.parser.ASTShiftRightNode;
import org.apache.commons.jexl3.parser.ASTShiftRightUnsignedNode;
import org.apache.commons.jexl3.parser.ASTSizeFunction;
import org.apache.commons.jexl3.parser.ASTStringLiteral;
import org.apache.commons.jexl3.parser.ASTSubNode;
import org.apache.commons.jexl3.parser.ASTTernaryNode;
import org.apache.commons.jexl3.parser.ASTTrueNode;
import org.apache.commons.jexl3.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl3.parser.ASTUnaryPlusNode;
import org.apache.commons.jexl3.parser.ASTVar;
import org.apache.commons.jexl3.parser.ASTWhileStatement;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.commons.jexl3.parser.ParserVisitor;
import org.apache.commons.jexl3.parser.SimpleNode;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Determine whether two trees are equivalent, accounting for arbitrary order within subtrees.
 */
public class TreeEqualityVisitor extends ParserVisitor {
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
        } else if (node1 instanceof JexlNode && !equal(JexlNodes.getImage((JexlNode) node1), JexlNodes.getImage((JexlNode) node2))) {
            equal = false;
            return ("Node images differ: " + JexlNodes.getImage((JexlNode) node1) + " vs " + JexlNodes.getImage((JexlNode) node2));
        } else if (node1.jjtGetNumChildren() > 0 || node2.jjtGetNumChildren() > 0) {
            List<SimpleNode> list1 = listChildren(node1);
            List<SimpleNode> list2 = listChildren(node2);
            if (list1.size() != list2.size()) {
                if (log.isDebugEnabled()) {
                    log.debug("not equal " + list1.size() + " " + list2.size());
                }
                equal = false;
                return ("Num children differ: [" + list1.stream().map(x -> x.getClass().getSimpleName()).collect(Collectors.joining(", ")) + "] vs ["
                                + list2.stream().map(x -> x.getClass().getSimpleName()).collect(Collectors.joining(", ")) + "]");
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
                    return "Did not find a matching child for " + child1.getClass().getSimpleName() + " in [" + list2.get(0).getClass().getSimpleName() + "]: "
                                    + reason;
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
                if ((child.getClass().equals(ASTReferenceExpression.class) && (child.jjtGetNumChildren() == 1))
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
    public Object visit(ASTJexlScript node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    public Object visit(ASTBlock node, Object data) {
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

    @Override
    protected Object visit(ASTDoWhileStatement node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTContinue node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTBreak node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTDefineVars node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTNullpNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTShiftLeftNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTShiftRightNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTShiftRightUnsignedNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTSWNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTNSWNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTEWNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTNEWNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTAddNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTSubNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTUnaryPlusNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTRegexLiteral node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTSetLiteral node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTExtendedLiteral node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTRangeNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTIdentifierAccess node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTArguments node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTSetAddNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTSetSubNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTSetMultNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTSetDivNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTSetModNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTSetAndNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTSetOrNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTSetXorNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTSetShiftLeftNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTSetShiftRightNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTSetShiftRightUnsignedNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTGetDecrementNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTGetIncrementNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTDecrementGetNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTIncrementGetNode node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTJxltLiteral node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTAnnotation node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTAnnotatedStatement node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTQualifiedIdentifier node, Object data) {
        return visitEquality(node, (SimpleNode) data);
    }
}
