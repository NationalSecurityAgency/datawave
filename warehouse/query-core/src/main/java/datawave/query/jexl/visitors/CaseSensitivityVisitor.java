package datawave.query.jexl.visitors;

import java.util.Set;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTAssignment;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTMethodNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.functions.JexlFunctionArgumentDescriptorFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.util.MetadataHelper;

/**
 * Upper case all identifier nodes as our field names are always upper case
 * <p>
 * Example: {@code (foo == 'bar') -> (FOO == 'bar')}
 */
public class CaseSensitivityVisitor extends ShortCircuitBaseVisitor {

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
     * @param <T>
     *            type of the script
     * @param config
     *            the query configuration
     * @param helper
     *            the metadata helper
     * @return the provided script
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
        if (data == null || ((Set<String>) data).contains(node.getName())) {
            // don't uppercase an identifier under a ASTMethodNode, it is the method's name
            if (node.jjtGetParent() instanceof ASTMethodNode == false) {
                JexlNodes.setIdentifier(node, node.getName().toUpperCase());
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
    public Object visit(ASTNENode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTOrNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

}
