package datawave.query.jexl.visitors;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.index.stats.IndexStatsClient;
import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;
import static org.apache.commons.jexl2.parser.JexlNodes.id;
import org.apache.commons.jexl2.parser.ParserTreeConstants;

public class PruneLessSelectiveFieldsVisitor extends RebuildingVisitor {
    
    private final ShardQueryConfiguration config;
    private final IndexStatsClient stats;
    
    public PruneLessSelectiveFieldsVisitor(ShardQueryConfiguration config) {
        this.config = config;
        stats = new IndexStatsClient(this.config.getClient(), this.config.getIndexStatsTableName());
    }
    
    /**
     * Given a JexlNode, return the most selective child subtree.
     *
     * @param <T>
     * @param config
     * @param script
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <T extends JexlNode> T prune(ShardQueryConfiguration config, T script) {
        PruneLessSelectiveFieldsVisitor visitor = new PruneLessSelectiveFieldsVisitor(config);
        return (T) script.jjtAccept(visitor, null);
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        JexlNode mostSelectiveChild = null;
        Double maxSelectivity = Double.valueOf("-1");
        
        boolean foundSelectivity = false;
        
        ASTAndNode newNode = new ASTAndNode(ParserTreeConstants.JJTANDNODE);
        newNode.image = node.image;
        newNode.jjtSetParent(node.jjtGetParent());
        
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode child = (JexlNode) node.jjtGetChild(i).jjtAccept(this, null);
            
            Double selectivity;
            
            if (child instanceof ASTOrNode || child instanceof ASTReference || child instanceof ASTReferenceExpression) {
                // Check for the selectivity of the OR
                selectivity = getOrSelectivity(child);
            } else {
                selectivity = JexlASTHelper.getNodeSelectivity(child, config, stats);
            }
            
            // Don't want to fail if there's an issue with one child...
            if (!selectivity.equals(IndexStatsClient.DEFAULT_VALUE) && selectivity > maxSelectivity) {
                foundSelectivity = true;
                // Hold on to the most selective child
                mostSelectiveChild = RebuildingVisitor.copy(child);
                maxSelectivity = selectivity;
            }
        }
        
        if (!foundSelectivity || null == mostSelectiveChild) {
            // Something went wrong, so just return the node we started with
            return RebuildingVisitor.copy(node);
        } else {
            newNode.jjtAddChild(mostSelectiveChild, newNode.jjtGetNumChildren());
        }
        return newNode;
    }
    
    /**
     * The OR selectivity will be the lowest selectivity of any fields in the OR. If any field returns a selectivity of IndexStatsClient.DEFAULT_VALUE, that
     * will be returned as the selectivity of the OR since something went wrong (unindexed field, etc).
     *
     * @param node
     * @return
     */
    protected Double getOrSelectivity(JexlNode node) {
        switch (id(node)) {
            case ParserTreeConstants.JJTREFERENCE:
            case ParserTreeConstants.JJTREFERENCEEXPRESSION:
                // Recurse!
                if (node.jjtGetNumChildren() == 1) {
                    return getOrSelectivity(node.jjtGetChild(0));
                } else {
                    return IndexStatsClient.DEFAULT_VALUE;
                }
            case ParserTreeConstants.JJTORNODE:
                Double minSelectivity = IndexStatsClient.DEFAULT_VALUE;
                for (JexlNode child : JexlASTHelper.getEQNodes(node)) {
                    Double selectivity = JexlASTHelper.getNodeSelectivity(child, config, stats);
                    // In an OR, getting the accumulo ranges will be at least as bad as the least
                    // selective of the children of the OR
                    if (selectivity < minSelectivity) {
                        minSelectivity = selectivity;
                        // If the selectivity is IndexStatsClient.DEFAULT_VALUE, either the field is
                        // unindexed or there was some other error, so don't proceed with the OR
                    } else if (selectivity.equals(IndexStatsClient.DEFAULT_VALUE)) {
                        return IndexStatsClient.DEFAULT_VALUE;
                    }
                }
                
                return minSelectivity;
            default:
                return IndexStatsClient.DEFAULT_VALUE;
        }
    }
}
