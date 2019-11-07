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
 * <pre>
 * Rewrites subtrees of Not Equals and Regex Not Equals nodes.
 * EQ example: "A != B" into "!(A == B)".
 * RN example: "A !~ B" into "!(A =~ B)".
 * This rewrite operation is destructive to the query tree.
 * </pre>
 */
public class RewriteNegationsVisitor extends BaseVisitor {
    
    public static <T extends JexlNode> T rewrite(T node) {
        node.jjtAccept(new RewriteNegationsVisitor(), null);
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
