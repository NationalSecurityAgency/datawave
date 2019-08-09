package datawave.query.jexl.visitors;

import java.util.Set;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.functions.JexlFunctionArgumentDescriptorFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.util.MetadataHelper;

import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTMethodNode;
import org.apache.commons.jexl2.parser.JexlNode;

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
    
}
