package datawave.query.jexl.visitors;

import static datawave.query.jexl.JexlASTHelper.isLiteral;

import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParserTreeConstants;

import datawave.query.jexl.JexlASTHelper;

/**
 * Detect and correct cases where an identifier is on the left and the field is on the right.
 */
public class InvertNodeVisitor extends RebuildingVisitor {

    public static <T extends JexlNode> T invertSwappedNodes(T script) {
        InvertNodeVisitor visitor = new InvertNodeVisitor();
        return (T) script.jjtAccept(visitor, null);
    }

    /**
     * Determines if a node's left child is a literal
     *
     * @param node
     *            a JexlNode
     * @return true if the left child is a literal
     */
    private boolean leftChildIsLiteral(JexlNode node) {
        if (node.jjtGetNumChildren() == 2) {
            JexlNode child = JexlASTHelper.dereference(node.jjtGetChild(0));
            return isLiteral(child);
        }
        return false;
    }

    /**
     * Reparent a JexlNode's children similar to JexlNodes.children(), but respects the RebuildingVisitor's method contract by copying the children.
     *
     * @param in
     *            the original JexlNode
     * @param out
     *            a new JexlNode
     * @return the new JexlNode
     */
    private JexlNode reparent(JexlNode in, JexlNode out) {
        int j = 0;
        for (int i = in.jjtGetNumChildren() - 1; i >= 0; i--) {
            JexlNode kid = in.jjtGetChild(i);
            kid = (JexlNode) kid.jjtAccept(this, null);
            out.jjtAddChild(kid, j++);
            kid.jjtSetParent(out);
        }
        return out;
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        if (leftChildIsLiteral(node)) {
            return reparent(node, new ASTEQNode(ParserTreeConstants.JJTEQNODE));
        }
        return super.visit(node, data);
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        if (leftChildIsLiteral(node)) {
            return reparent(node, new ASTNENode(ParserTreeConstants.JJTNENODE));
        }
        return super.visit(node, data);
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        if (leftChildIsLiteral(node)) {
            return reparent(node, new ASTGTNode(ParserTreeConstants.JJTGTNODE));
        }
        return super.visit(node, data);
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        if (leftChildIsLiteral(node)) {
            return reparent(node, new ASTLTNode(ParserTreeConstants.JJTLTNODE));
        }
        return super.visit(node, data);
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        if (leftChildIsLiteral(node)) {
            return reparent(node, new ASTGENode(ParserTreeConstants.JJTGENODE));
        }
        return super.visit(node, data);
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        if (leftChildIsLiteral(node)) {
            return reparent(node, new ASTLENode(ParserTreeConstants.JJTLENODE));
        }
        return super.visit(node, data);
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        if (leftChildIsLiteral(node)) {
            return reparent(node, new ASTERNode(ParserTreeConstants.JJTERNODE));
        }
        return super.visit(node, data);
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        if (leftChildIsLiteral(node)) {
            return reparent(node, new ASTNRNode(ParserTreeConstants.JJTNRNODE));
        }
        return super.visit(node, data);
    }
}
