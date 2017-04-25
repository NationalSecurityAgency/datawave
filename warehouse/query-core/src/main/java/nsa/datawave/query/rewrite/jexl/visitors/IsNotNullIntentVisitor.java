package nsa.datawave.query.rewrite.jexl.visitors;

import nsa.datawave.query.rewrite.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNullLiteral;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.log4j.Logger;

/**
 * A visitor that checks the query tree to change any terms like FOO == '
 * 
 */
public class IsNotNullIntentVisitor extends RebuildingVisitor {
    private static final Logger log = Logger.getLogger(IsNotNullIntentVisitor.class);
    
    /**
     * 
     * @param script
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <T extends JexlNode> T fixNotNullIntent(T script) {
        IsNotNullIntentVisitor visitor = new IsNotNullIntentVisitor();
        
        return (T) script.jjtAccept(visitor, null);
    }
    
    /**
     */
    @Override
    public Object visit(ASTERNode node, Object data) {
        
        // if this ER node is of the form FIELD =~ '*', then the user probably means: FIELD != null
        // change it!
        String field = JexlASTHelper.getIdentifier(node);
        Object value = JexlASTHelper.getLiteralValue(node);
        
        if (".*?".equals(value)) {
            
            ASTNENode neNode = new ASTNENode(ParserTreeConstants.JJTNENODE);
            neNode.jjtAddChild(node.jjtGetChild(0), 0);
            neNode.jjtAddChild(new ASTNullLiteral(ParserTreeConstants.JJTNULLLITERAL), 1);
            return super.visit(neNode, data);
        }
        
        return super.visit(node, data);
    }
    
}
