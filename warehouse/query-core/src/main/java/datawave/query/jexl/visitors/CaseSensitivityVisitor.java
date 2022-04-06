package datawave.query.jexl.visitors;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.functions.JexlFunctionArgumentDescriptorFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.util.MetadataHelper;
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

import java.util.Set;

/**
 * Upper case all identifier nodes as our field names are always upper case
 * <p>
 * Example: {@code (foo == 'bar') -> (FOO == 'bar')}
 */
public class CaseSensitivityVisitor extends BaseVisitor {
    
    private ShardQueryConfiguration config;
    private MetadataHelper helper;
    
    public CaseSensitivityVisitor(ShardQueryConfiguration config, MetadataHelper helper) {
        this.config = config;
        this.helper = helper;
    }
    
    /**
     * Ensure that all ReferenceNode's are upper-cased. Modifies the provided ASTJexlScript in-place.
     *
     * @param script
     *            An ASTJexlScript
     * @return
     */
    public static <T extends JexlNode> T upperCaseIdentifiers(ShardQueryConfiguration config, MetadataHelper helper, T script) {
        CaseSensitivityVisitor visitor = new CaseSensitivityVisitor(config, helper);
        
        script.jjtAccept(visitor, null);
        
        return script;
    }
    
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        // lets determine which of the arguments are actually field name identifiers (e.g. termFrequencyMap is not)
        JexlArgumentDescriptor desc = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(node);
        
        Set<String> fields = desc.fields(helper, config.getDatatypeFilter());
        
        return super.visit(node, fields);
    }
    
    @Override
    public Object visit(ASTAssignment node, Object data) {
        // we do not want to touch assignment identifiers
        return data;
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public Object visit(ASTIdentifier node, Object data) {
        // if a field set was passed in, then check for existence before upcasing (@see visit(ASTFunctionNode, Object))
        if (data == null || ((Set<String>) data).contains(node.image)) {
            // don't uppercase an identifier under a ASTMethodNode, it is the method's name
            if (node.jjtGetParent() instanceof ASTMethodNode == false) {
                node.image = node.image.toUpperCase();
            }
        }
        node.childrenAccept(this, data);
        
        return data;
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
    public Object visit(ASTEQNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTNENode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTOrNode node, Object data) {
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
    
    // Short Circuit these nodes
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
        return data;
    }
    
    @Override
    public Object visit(ASTNRNode node, Object data) {
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
    
}
