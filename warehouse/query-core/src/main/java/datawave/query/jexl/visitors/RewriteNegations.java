package datawave.query.jexl.visitors;

import static org.apache.commons.jexl2.parser.JexlNodes.children;
import static org.apache.commons.jexl2.parser.JexlNodes.negate;
import static org.apache.commons.jexl2.parser.JexlNodes.swap;

import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParserTreeConstants;

/**
 * Rewrites subtrees of the form "A != B" into "!(A == B)". This is destructive on the tree.
 */
public class RewriteNegations extends BaseVisitor {
    
    public static <T extends JexlNode> T rewrite(T node) {
        node.jjtAccept(new RewriteNegations(), null);
        return node;
    }
    
    @Override
    public Object visit(ASTNENode notEquals, Object data) {
        final JexlNode root = notEquals.jjtGetParent();
        final JexlNode equals = children(new ASTEQNode(ParserTreeConstants.JJTEQNODE), children(notEquals));
        swap(root, notEquals, negate(equals));
        return null;
    }
    
    @Override
    public Object visit(ASTNRNode notEquals, Object data) {
        final JexlNode root = notEquals.jjtGetParent();
        final JexlNode equals = children(new ASTERNode(ParserTreeConstants.JJTERNODE), children(notEquals));
        swap(root, notEquals, negate(equals));
        return null;
    }
    
}
