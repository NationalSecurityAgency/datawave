package datawave.query.jexl.visitors;

import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTAdditiveNode;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTDivNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTFalseNode;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTMethodNode;
import org.apache.commons.jexl2.parser.ASTModNode;
import org.apache.commons.jexl2.parser.ASTMulNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTNullLiteral;
import org.apache.commons.jexl2.parser.ASTNumberLiteral;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.ASTTrueNode;
import org.apache.commons.jexl2.parser.JexlNode;

import java.util.ArrayList;
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
    public Object visit(ASTFunctionNode node, Object data) {
        // not concerned with literals for function nodes
        return null;
    }
    
    @Override
    public Object visit(ASTMethodNode node, Object data) {
        // not concerned with literals for method nodes
        return null;
    }
    
    @Override
    public Object visit(ASTAdditiveNode node, Object data) {
        // not concerned with literals for additive nodes
        return null;
    }
    
    @Override
    public Object visit(ASTMulNode node, Object data) {
        // not concerned with literals for mul nodes
        return null;
    }
    
    @Override
    public Object visit(ASTDivNode node, Object data) {
        // not concerned with literals for div nodes
        return null;
    }
    
    @Override
    public Object visit(ASTModNode node, Object data) {
        // not concerned with literals for mod nodes
        return null;
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        List<Object> literals = new ArrayList<>();
        super.visit(node, literals);
        
        validateExpression(node, data, literals);
        
        return data;
    }
    
    @Override
    public Object visit(ASTNENode node, Object data) {
        List<Object> literals = new ArrayList<>();
        super.visit(node, literals);
        
        validateExpression(node, data, literals);
        
        return data;
    }
    
    @Override
    public Object visit(ASTLTNode node, Object data) {
        List<Object> literals = new ArrayList<>();
        super.visit(node, literals);
        
        validateExpression(node, data, literals);
        
        return data;
    }
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        List<Object> literals = new ArrayList<>();
        super.visit(node, literals);
        
        validateExpression(node, data, literals);
        
        return data;
    }
    
    @Override
    public Object visit(ASTLENode node, Object data) {
        List<Object> literals = new ArrayList<>();
        super.visit(node, literals);
        
        validateExpression(node, data, literals);
        
        return data;
    }
    
    @Override
    public Object visit(ASTGENode node, Object data) {
        List<Object> literals = new ArrayList<>();
        super.visit(node, literals);
        
        validateExpression(node, data, literals);
        
        return data;
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        List<Object> literals = new ArrayList<>();
        super.visit(node, literals);
        
        validateExpression(node, data, literals);
        
        return data;
    }
    
    @Override
    public Object visit(ASTNRNode node, Object data) {
        List<Object> literals = new ArrayList<>();
        super.visit(node, literals);
        
        validateExpression(node, data, literals);
        
        return data;
    }
    
    @Override
    public Object visit(ASTAssignment node, Object data) {
        List<Object> literals = new ArrayList<>();
        super.visit(node, literals);
        
        validateExpression(node, data, literals);
        
        return data;
    }
    
    /* literal visitors */
    @Override
    public Object visit(ASTNumberLiteral node, Object data) {
        visitLiteral(node, data);
        return null;
    }
    
    @Override
    public Object visit(ASTTrueNode node, Object data) {
        visitLiteral(node, data);
        return null;
    }
    
    @Override
    public Object visit(ASTFalseNode node, Object data) {
        visitLiteral(node, data);
        return null;
    }
    
    @Override
    public Object visit(ASTNullLiteral node, Object data) {
        visitLiteral(node, data);
        return null;
    }
    
    @Override
    public Object visit(ASTStringLiteral node, Object data) {
        visitLiteral(node, data);
        return null;
    }
    
    private void visitLiteral(JexlNode node, Object data) {
        List<Object> literals = (List<Object>) data;
        literals.add(JexlASTHelper.getLiteralValue(node));
    }
    
    private void validateExpression(JexlNode node, Object data, List<Object> literals) {
        // add the found literals to the parent list if necessary
        if (data instanceof List) {
            ((List<Object>) data).addAll(literals);
        }
        
        if (literals.size() > 1) {
            throw new DatawaveFatalQueryException("Cannot compare two literals.  Invalid expression: " + JexlStringBuildingVisitor.buildQuery(node));
        }
    }
    
}
