package datawave.query.jexl.visitors;

import com.google.common.collect.Maps;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.FunctionJexlNodeVisitor;
import datawave.query.jexl.functions.JexlFunctionArgumentDescriptorFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import org.apache.commons.jexl2.parser.ASTAdditiveNode;
import org.apache.commons.jexl2.parser.ASTAdditiveOperator;
import org.apache.commons.jexl2.parser.ASTAmbiguous;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTArrayAccess;
import org.apache.commons.jexl2.parser.ASTArrayLiteral;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTBitwiseAndNode;
import org.apache.commons.jexl2.parser.ASTBitwiseComplNode;
import org.apache.commons.jexl2.parser.ASTBitwiseOrNode;
import org.apache.commons.jexl2.parser.ASTBitwiseXorNode;
import org.apache.commons.jexl2.parser.ASTBlock;
import org.apache.commons.jexl2.parser.ASTConstructorNode;
import org.apache.commons.jexl2.parser.ASTDivNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTEmptyFunction;
import org.apache.commons.jexl2.parser.ASTFalseNode;
import org.apache.commons.jexl2.parser.ASTFloatLiteral;
import org.apache.commons.jexl2.parser.ASTForeachStatement;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTIfStatement;
import org.apache.commons.jexl2.parser.ASTIntegerLiteral;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTMapEntry;
import org.apache.commons.jexl2.parser.ASTMapLiteral;
import org.apache.commons.jexl2.parser.ASTMethodNode;
import org.apache.commons.jexl2.parser.ASTModNode;
import org.apache.commons.jexl2.parser.ASTMulNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.ASTNullLiteral;
import org.apache.commons.jexl2.parser.ASTNumberLiteral;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.ASTReturnStatement;
import org.apache.commons.jexl2.parser.ASTSizeFunction;
import org.apache.commons.jexl2.parser.ASTSizeMethod;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.ASTTernaryNode;
import org.apache.commons.jexl2.parser.ASTTrueNode;
import org.apache.commons.jexl2.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl2.parser.ASTVar;
import org.apache.commons.jexl2.parser.ASTWhileStatement;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.SimpleNode;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Validates all patterns in a query tree. Uses a cache to avoid parsing the same pattern twice.
 */
public class ValidPatternVisitor extends BaseVisitor {
    
    private Map<String,Pattern> patternCache;
    
    public ValidPatternVisitor() {
        this.patternCache = Maps.newHashMap();
    }
    
    public static void check(JexlNode node) {
        ValidPatternVisitor visitor = new ValidPatternVisitor();
        node.jjtAccept(visitor, null);
    }
    
    /**
     * Visit a Regex Equals node, catches the situation where a users might enter FIELD1 =~ VALUE1.
     *
     * @param node
     *            - an AST Regex Equals node
     * @param data
     *            - data
     * @return the data
     */
    @Override
    public Object visit(ASTERNode node, Object data) {
        parseAndPutPattern(node);
        return data;
    }
    
    /**
     * Visit a Regex Not Equals node, catches the situation where a user might enter FIELD1 !~ VALUE1
     *
     * @param node
     *            - an AST Regex Not Equals node
     * @param data
     *            - data
     * @return the data
     */
    @Override
    public Object visit(ASTNRNode node, Object data) {
        parseAndPutPattern(node);
        return data;
    }
    
    /**
     * Visit an ASTFunctionNode to catch cases like #INCLUDE or #EXCLUDE that accept a regex as an argument
     *
     * @param node
     *            - an ASTFunctionNode
     * @param data
     *            - the data
     * @return
     */
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        
        // Should pull back an EvaluationPhaseFilterFunctionsDescriptor
        JexlArgumentDescriptor descriptor = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(node);
        if (descriptor == null) {
            throw new IllegalStateException("Could not get descriptor for ASTFunctionNode");
        }
        
        if (descriptor.regexArguments()) {
            // Extract the args for this function
            FunctionJexlNodeVisitor functionVisitor = new FunctionJexlNodeVisitor();
            functionVisitor.visit(node, null);
            List<JexlNode> args = functionVisitor.args();
            for (JexlNode arg : args) {
                // Only take the literals
                if (arg instanceof ASTStringLiteral) {
                    parseAndPutPattern(arg);
                }
            }
        }
        // Do not descend to children, the ValidPatternVisitor views a function node as a leaf node.
        return data;
    }
    
    /**
     * Parse a literal value and put into the pattern cache if it does not exist.
     *
     * @param node
     */
    public void parseAndPutPattern(JexlNode node) {
        // Catch the situation where a user might enter FIELD1 !~ VALUE1
        Object literalValue;
        try {
            literalValue = JexlASTHelper.getLiteralValue(node);
        } catch (Exception e) {
            // in this case there was no literal (e.g. FIELD1 !~ FIELD2)
            return;
        }
        
        if (literalValue != null && String.class.equals(literalValue.getClass())) {
            String literalString = (String) literalValue;
            try {
                if (patternCache.containsKey(literalString)) {
                    return;
                }
                patternCache.put(literalString, Pattern.compile(literalString));
            } catch (PatternSyntaxException e) {
                String builtNode = JexlStringBuildingVisitor.buildQueryWithoutParse(node);
                String errMsg = "Invalid pattern found in filter function '" + builtNode + "'";
                throw new PatternSyntaxException(errMsg, e.getPattern(), e.getIndex());
            }
        }
    }
    
    // Descend through these nodes
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
    public Object visit(ASTReference node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    // Do not descend through these nodes
    @Override
    public Object visit(SimpleNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTBlock node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTAmbiguous node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTIfStatement node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTWhileStatement node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTForeachStatement node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTAssignment node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTTernaryNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTOrNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTBitwiseOrNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTBitwiseXorNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTBitwiseAndNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTNENode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTLTNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTLENode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTGENode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTAdditiveNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTAdditiveOperator node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTMulNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTDivNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTModNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTUnaryMinusNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTBitwiseComplNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTNotNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTIdentifier node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTNullLiteral node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTTrueNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTFalseNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTIntegerLiteral node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTFloatLiteral node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTStringLiteral node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTArrayLiteral node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTMapLiteral node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTMapEntry node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTEmptyFunction node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTSizeFunction node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTMethodNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTSizeMethod node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTConstructorNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTArrayAccess node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTReturnStatement node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTVar node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTNumberLiteral node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        return data;
    }
    
}
