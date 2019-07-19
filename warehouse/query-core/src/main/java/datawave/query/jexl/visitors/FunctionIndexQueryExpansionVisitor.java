package datawave.query.jexl.visitors;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.functions.JexlFunctionArgumentDescriptorFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.jexl.functions.arguments.RebuildingJexlArgumentDescriptor;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.MetadataHelper;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTEvaluationOnly;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTTrueNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.log4j.Logger;

import java.util.Arrays;

/**
 * Visits an JexlNode tree, and expand the functions to be AND'ed with their index query equivalents. Note that the functions are left in the final query to
 * provide potentially additional filtering after applying the index query.
 *
 */
public class FunctionIndexQueryExpansionVisitor extends RebuildingVisitor {
    private static final Logger log = ThreadConfigurableLogger.getLogger(FunctionIndexQueryExpansionVisitor.class);
    
    protected ShardQueryConfiguration config;
    protected MetadataHelper metadataHelper;
    protected DateIndexHelper dateIndexHelper;
    
    public FunctionIndexQueryExpansionVisitor(ShardQueryConfiguration config, MetadataHelper metadataHelper, DateIndexHelper dateIndexHelper) {
        this.config = config;
        this.metadataHelper = metadataHelper;
        this.dateIndexHelper = dateIndexHelper;
    }
    
    /**
     * Expand functions to be AND'ed with their index query equivalents.
     *
     * @param script
     * @return The tree with additional index query portions
     */
    @SuppressWarnings("unchecked")
    public static <T extends JexlNode> T expandFunctions(ShardQueryConfiguration config, MetadataHelper metadataHelper, DateIndexHelper dateIndexHelper,
                    T script) {
        FunctionIndexQueryExpansionVisitor visitor = new FunctionIndexQueryExpansionVisitor(config, metadataHelper, dateIndexHelper);
        
        return (T) script.jjtAccept(visitor, null);
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        return node;
    }
    
    @Override
    public Object visit(ASTNRNode node, Object data) {
        return node;
    }
    
    @Override
    public Object visit(ASTLTNode node, Object data) {
        return node;
    }
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        return node;
    }
    
    @Override
    public Object visit(ASTLENode node, Object data) {
        return node;
    }
    
    @Override
    public Object visit(ASTGENode node, Object data) {
        return node;
    }
    
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        JexlArgumentDescriptor desc = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(node);
        
        if (desc instanceof RebuildingJexlArgumentDescriptor) {
            JexlNode rebuiltNode = ((RebuildingJexlArgumentDescriptor) desc).rebuildNode(config, this.metadataHelper, this.dateIndexHelper,
                            this.config.getDatatypeFilter(), node);
            
            // if the node changed, visit the rebuilt node
            if (rebuiltNode != node)
                return rebuiltNode.jjtAccept(this, data);
        }
        
        JexlNode indexQuery = desc.getIndexQuery(config, this.metadataHelper, this.dateIndexHelper, this.config.getDatatypeFilter());
        if (indexQuery != null && !(indexQuery instanceof ASTTrueNode)) {
            // now link em up
            return JexlNodeFactory.createAndNode(Arrays.asList(node, indexQuery));
        } else {
            return node;
        }
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        if (!ASTEvaluationOnly.instanceOf(node))
            return super.visit(node, data);
        else
            return node;
    }
    
    @Override
    public Object visit(ASTReference node, Object data) {
        if (!ASTEvaluationOnly.instanceOf(node))
            return super.visit(node, data);
        else
            return node;
    }
}
