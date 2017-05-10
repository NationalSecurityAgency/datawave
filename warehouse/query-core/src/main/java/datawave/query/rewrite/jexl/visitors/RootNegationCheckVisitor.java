package datawave.query.rewrite.jexl.visitors;

import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.JexlNode;

/**
 * Determine if a negation occurs at the root of the AST
 */
public class RootNegationCheckVisitor {
    
    public static Boolean hasTopLevelNegation(JexlNode script) {
        RootNegationCheckVisitor visitor = new RootNegationCheckVisitor();
        return visitor.isNegation(script);
    }
    
    private boolean isNegation(JexlNode node) {
        boolean hasNegation = false;
        for (int i = 0; i < node.jjtGetNumChildren() && !hasNegation; i++) {
            JexlNode child = node.jjtGetChild(i);
            Class<?> childClass = child.getClass();
            
            if (ASTNENode.class.equals(childClass) || ASTNotNode.class.equals(childClass) || ASTNRNode.class.equals(childClass)) {
                hasNegation = true;
            } else {
                hasNegation = false;
            }
        }
        
        return hasNegation;
    }
}
