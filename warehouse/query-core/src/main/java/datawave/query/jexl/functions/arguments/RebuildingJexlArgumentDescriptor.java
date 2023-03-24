package datawave.query.jexl.functions.arguments;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.MetadataHelper;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.JexlNode;

import java.util.Set;

/**
 * JexlArgumentDescriptors should implement this interface if they need to conditionally rebuild or reconfigure their source ASTFunctionNode. The reasons for
 * rebuilding the ASTFunctionNode will vary from function to function. If the node is rebuilt, the FunctionIndexQueryExpansionVisitor will ignore this
 * JexlArgumentDescriptor, and reprocess the new node and it's children.
 */
public interface RebuildingJexlArgumentDescriptor extends JexlArgumentDescriptor {
    
    /**
     * Conditionally rebuilds the ASTFunctionNode that this JexlArgumentDescriptor was created from. If no changes to the original ASTFunctionNode are required,
     * the original node should be returned.
     *
     * @param settings
     *            configuration settings
     * @param metadataHelper
     *            the metadata helper
     * @param dateIndexHelper
     *            the date index helper
     * @param datatypeFilter
     *            the data type filter
     * @param node
     *            the function node
     * @return A new JexlNode representing the rebuilt ASTFunctionNode, or if no change is required, returns the original ASTFunctionNode
     */
    JexlNode rebuildNode(ShardQueryConfiguration settings, MetadataHelper metadataHelper, DateIndexHelper dateIndexHelper, Set<String> datatypeFilter,
                    ASTFunctionNode node);
}
