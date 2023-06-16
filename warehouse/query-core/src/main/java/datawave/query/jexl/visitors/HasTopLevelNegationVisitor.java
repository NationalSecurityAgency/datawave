package datawave.query.jexl.visitors;

import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;

/**
 * Determine if a negated node exists at the root of the AST
 *
 */
public class HasTopLevelNegationVisitor {

    /**
     * Determine if a negation occurs at the root of the AST
     *
     * @param script
     *            a script
     * @return if a negation occurs
     */
    public static Boolean hasTopLevelNegation(ASTJexlScript script) {
        HasTopLevelNegationVisitor visitor = new HasTopLevelNegationVisitor();

        return visitor.recurseRoot(script);
    }

    protected boolean recurseRoot(JexlNode node) {
        boolean hasNegation = false;
        for (int i = 0; i < node.jjtGetNumChildren() && !hasNegation; i++) {
            JexlNode child = node.jjtGetChild(i);
            Class<?> childClass = child.getClass();

            if (ASTNENode.class.equals(childClass)) {
                hasNegation = true;
            } else if (ASTAndNode.class.equals(childClass)) {
                return recurseAnd(child);
            } else if (ASTOrNode.class.equals(childClass)) {
                return recurseOr(child);
            } else {
                hasNegation = recurseRoot(child);
            }
        }

        return hasNegation;
    }

    protected boolean recurseAnd(JexlNode node) {
        boolean hasNegation = false;

        for (int i = 0; i < node.jjtGetNumChildren() && !hasNegation; i++) {
            JexlNode child = node.jjtGetChild(i);
            Class<?> childClass = child.getClass();

            if (ASTAndNode.class.equals(childClass) || ASTReference.class.equals(childClass) || ASTReferenceExpression.class.equals(childClass)) {
                hasNegation = recurseAnd(child);
            }

            if (ASTNENode.class.equals(childClass)) {
                return true;
            }
        }

        return hasNegation;
    }

    protected boolean recurseOr(JexlNode node) {
        boolean hasNegation = false;

        for (int i = 0; i < node.jjtGetNumChildren() && !hasNegation; i++) {
            JexlNode child = node.jjtGetChild(i);
            Class<?> childClass = child.getClass();

            if (ASTAndNode.class.equals(childClass) || ASTReference.class.equals(childClass) || ASTReferenceExpression.class.equals(child.getClass())) {
                hasNegation = recurseOr(child);
            }

            if (ASTNENode.class.equals(childClass) || ASTNRNode.class.equals(childClass)) {
                return true;
            }
        }

        return hasNegation;
    }
}
