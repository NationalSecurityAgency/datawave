package datawave.query.jexl.visitors;

import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.JexlNode;

import java.util.List;


/**
 * Validates all expressions in a query tree (e.g. literal == literal is considered invalid)
 */
public class ValidComparisonVisitor extends BaseVisitor {

    public static void check(JexlNode node) {
        ValidComparisonVisitor visitor = new ValidComparisonVisitor();
        node.jjtAccept(visitor, null);
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        validateExpression(node);
        return data;
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        validateExpression(node);
        return data;
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        validateExpression(node);
        return data;
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        validateExpression(node);
        return data;
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        validateExpression(node);
        return data;
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        validateExpression(node);
        return data;
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        validateExpression(node);
        return data;
    }
    
    @Override
    public Object visit(ASTNRNode node, Object data) {
        validateExpression(node);
        return data;
    }


    private void validateExpression(JexlNode node) {
        List<JexlNode> literals = JexlASTHelper.getLiterals(node);
        if (literals.size() > 1) {
            throw new DatawaveFatalQueryException("Cannot compare two literals.  Invalid expression: " + JexlStringBuildingVisitor.buildQuery(node));
        }
    }
    
}
