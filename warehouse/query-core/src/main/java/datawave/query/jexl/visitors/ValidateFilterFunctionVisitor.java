package datawave.query.jexl.visitors;

import com.google.common.collect.Sets;
import datawave.core.common.logging.ThreadConfigurableLogger;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.FunctionJexlNodeVisitor;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
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
import org.apache.log4j.Logger;

import java.util.Set;

import static datawave.query.jexl.functions.EvaluationPhaseFilterFunctions.EVAL_PHASE_FUNCTION_NAMESPACE;
import static datawave.query.jexl.functions.QueryFunctions.NO_EXPANSION;

/**
 * Verifies that no filter function is run against an index-only field
 * <p>
 * The exception to this rule is the #NO_EXPANSION function, which is actually a filter function
 * <p>
 * #NO_EXPANSION controls query model expansion and is allowed to limit index only fields
 */
public class ValidateFilterFunctionVisitor extends BaseVisitor {
    
    private static final Logger log = ThreadConfigurableLogger.getLogger(ValidateFilterFunctionVisitor.class);
    
    private final Set<String> indexOnlyFields;
    
    public ValidateFilterFunctionVisitor(Set<String> indexOnlyFields) {
        this.indexOnlyFields = indexOnlyFields;
    }
    
    /**
     * Generic entrypoint
     *
     * @param node
     *            an arbitrary JexlNode
     * @param indexOnlyFields
     *            a set of index only fields
     * @return the original node
     */
    public static JexlNode validate(JexlNode node, Set<String> indexOnlyFields) {
        if (!indexOnlyFields.isEmpty()) {
            ValidateFilterFunctionVisitor visitor = new ValidateFilterFunctionVisitor(indexOnlyFields);
            node.jjtAccept(visitor, null);
        }
        return node;
    }
    
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        // visit children first, we may have function as an argument
        node.childrenAccept(this, data);
        validateFunction(node);
        return data;
    }
    
    private void validateFunction(ASTFunctionNode node) {
        FunctionJexlNodeVisitor visitor = new FunctionJexlNodeVisitor();
        node.jjtAccept(visitor, null);
        
        Set<String> identifiers;
        if (visitor.namespace().equals(EVAL_PHASE_FUNCTION_NAMESPACE) && !visitor.name().equals(NO_EXPANSION)) {
            for (JexlNode arg : visitor.args()) {
                identifiers = JexlASTHelper.getIdentifierNames(arg);
                for (String identifier : identifiers) {
                    if (indexOnlyFields.contains(identifier)) {
                        
                        if (log.isDebugEnabled()) {
                            log.debug("Filter function " + visitor.name() + " contained index-only field [" + identifier + "] for node ["
                                            + JexlStringBuildingVisitor.buildQueryWithoutParse(node) + "]");
                        }
                        
                        String msg = "Filter function cannot evaluate against index-only field [" + identifier + "]";
                        BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, msg);
                        throw new DatawaveFatalQueryException(qe);
                    }
                }
            }
        }
    }
    
    // pass-through
    
    @Override
    public Object visit(ASTJexlScript node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTOrNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(SimpleNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTNotNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTReference node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    // additional nodes to pass through (<, >, <=, >=, +, *, /, %)
    
    @Override
    public Object visit(ASTLTNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTLENode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTGENode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTAdditiveNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTMulNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTDivNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTModNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTAssignment node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    // short circuit on leaf nodes
    
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
    public Object visit(ASTTernaryNode node, Object data) {
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
    public Object visit(ASTNENode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTNRNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTAdditiveOperator node, Object data) {
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
    @SuppressWarnings("deprecation")
    public Object visit(ASTIntegerLiteral node, Object data) {
        return data;
    }
    
    @Override
    @SuppressWarnings({"deprecation"})
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
}
