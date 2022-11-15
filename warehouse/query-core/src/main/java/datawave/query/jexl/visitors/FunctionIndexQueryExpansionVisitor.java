package datawave.query.jexl.visitors;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.functions.ContentFunctionsDescriptor;
import datawave.query.jexl.functions.JexlFunctionArgumentDescriptorFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.jexl.functions.arguments.RebuildingJexlArgumentDescriptor;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.MetadataHelper;
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
import org.apache.commons.jexl2.parser.DroppedExpression;
import org.apache.commons.jexl2.parser.JexlNode;

import java.util.Arrays;

import static datawave.query.jexl.functions.ContentFunctionsDescriptor.ContentJexlArgumentDescriptor.distributeFunctionIntoIndexQuery;

/**
 * Visits an JexlNode tree, and expand the functions to be AND'ed with their index query equivalents. Note that the functions are left in the final query to
 * provide potentially additional filtering after applying the index query.
 *
 */
public class FunctionIndexQueryExpansionVisitor extends RebuildingVisitor {

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
     *            script
     * @param <T>
     *            the type cast
     * @param config
     *            config
     * @param dateIndexHelper
     *            dateIndexHelper
     * @param metadataHelper
     *            metadataHelper
     * @return The tree with additional index query portions
     */
    @SuppressWarnings("unchecked")
    public static <T extends JexlNode> T expandFunctions(ShardQueryConfiguration config, MetadataHelper metadataHelper, DateIndexHelper dateIndexHelper,
                    T script) {
        JexlNode copy = copy(script);

        FunctionIndexQueryExpansionVisitor visitor = new FunctionIndexQueryExpansionVisitor(config, metadataHelper, dateIndexHelper);

        return (T) copy.jjtAccept(visitor, null);
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        return copy(node);
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        return copy(node);
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        return copy(node);
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        return copy(node);
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        return copy(node);
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        return copy(node);
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        boolean evaluationOnly = (data instanceof Boolean) && (Boolean) data;

        JexlArgumentDescriptor desc = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor(node);

        if (desc instanceof RebuildingJexlArgumentDescriptor) {
            JexlNode rebuiltNode = ((RebuildingJexlArgumentDescriptor) desc).rebuildNode(config, this.metadataHelper, this.dateIndexHelper,
                            this.config.getDatatypeFilter(), node);

            // if the node changed, visit the rebuilt node
            if (rebuiltNode != node)
                return rebuiltNode.jjtAccept(this, data);
        }

        if (!evaluationOnly) {
            JexlNode indexQuery = desc.getIndexQuery(config, this.metadataHelper, this.dateIndexHelper, this.config.getDatatypeFilter());
            if (indexQuery != null && !(indexQuery instanceof ASTTrueNode)) {
                if (desc instanceof ContentFunctionsDescriptor.ContentJexlArgumentDescriptor) {
                    return distributeFunctionIntoIndexQuery(node, indexQuery);
                } else {
                    // now link em up
                    return JexlNodeFactory.createAndNode(Arrays.asList(node, indexQuery));
                }
            }
        }

        return copy(node);
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        // if we know from a parent that this is evaluation only (or ignored), pass that forward. if we don't know, check.
        return super.visit(node, (data instanceof Boolean && (Boolean) data)
                        || QueryPropertyMarker.findInstance(node).isAnyTypeOf(ASTEvaluationOnly.class, DroppedExpression.class));
    }

    @Override
    public Object visit(ASTReference node, Object data) {
        // if we know from a parent that this is evaluation only (or ignored), pass that forward. if we don't know, check.
        return super.visit(node, (data instanceof Boolean && (Boolean) data)
                        || QueryPropertyMarker.findInstance(node).isAnyTypeOf(ASTEvaluationOnly.class, DroppedExpression.class));
    }
}
