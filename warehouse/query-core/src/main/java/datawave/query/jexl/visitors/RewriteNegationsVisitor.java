package datawave.query.jexl.visitors;

import static org.apache.commons.jexl3.parser.JexlNodes.negate;
import static org.apache.commons.jexl3.parser.JexlNodes.setChildren;
import static org.apache.commons.jexl3.parser.JexlNodes.swap;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParserTreeConstants;

/**
 * <pre>
 * Rewrites subtrees of Not Equals and Regex Not Equals nodes.
 * NE to EQ example: "A != B" into "!(A == B)"
 * ER to NR example: "A !~ B" into "!(A =~ B)"
 * This rewrite operation is destructive to the query tree.
 * </pre>
 */
public class RewriteNegationsVisitor extends ShortCircuitBaseVisitor {

    public static <T extends JexlNode> T rewrite(T node) {
        node.jjtAccept(new RewriteNegationsVisitor(), null);
        return node;
    }

    @Override
    public Object visit(ASTNENode notEquals, Object data) {
        final JexlNode root = notEquals.jjtGetParent();
        List<JexlNode> children = new ArrayList<>();
        for (int i = 0; i < notEquals.jjtGetNumChildren(); i++) {
            children.add(notEquals.jjtGetChild(i));
        }
        final JexlNode equals = setChildren(new ASTEQNode(ParserTreeConstants.JJTEQNODE), children.toArray(new JexlNode[0]));
        swap(root, notEquals, negate(equals));
        return null;
    }

    @Override
    public Object visit(ASTNRNode notEquals, Object data) {
        final JexlNode root = notEquals.jjtGetParent();
        List<JexlNode> children = new ArrayList<>();
        for (int i = 0; i < notEquals.jjtGetNumChildren(); i++) {
            children.add(notEquals.jjtGetChild(i));
        }
        final JexlNode equals = setChildren(new ASTERNode(ParserTreeConstants.JJTERNODE), children.toArray(new JexlNode[0]));
        swap(root, notEquals, negate(equals));
        return null;
    }

    // Ensure we descend through these nodes
    @Override
    public Object visit(ASTJexlScript node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTOrNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

}
