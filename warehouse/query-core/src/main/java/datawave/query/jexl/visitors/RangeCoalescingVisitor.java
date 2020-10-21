package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.LiteralRange;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Visits an JexlNode tree, and coalesces bounded ranges into separate AND expressions.
 */
public class RangeCoalescingVisitor extends RebuildingVisitor {
    private static final Logger log = ThreadConfigurableLogger.getLogger(RangeCoalescingVisitor.class);
    
    /**
     * Coalesce ranges into separate AND expressions.
     *
     * @param script
     * @return The tree with ranges coalesced
     */
    @SuppressWarnings("unchecked")
    public static <T extends JexlNode> T coalesceRanges(T script) {
        RangeCoalescingVisitor visitor = new RangeCoalescingVisitor();
        
        return (T) script.jjtAccept(visitor, null);
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        List<JexlNode> leaves = new ArrayList<>();
        Map<LiteralRange<?>,List<JexlNode>> ranges = JexlASTHelper.getBoundedRangesIndexAgnostic(node, leaves, false);
        
        JexlNode andNode = JexlNodes.newInstanceOfType(node);
        andNode.image = node.image;
        andNode.jjtSetParent(node.jjtGetParent());
        
        // We have a bounded range completely inside of an AND/OR
        if (!ranges.isEmpty()) {
            andNode = coalesceBoundedRanges(ranges, leaves, node, andNode, data);
        } else {
            // We have no bounded range to replace, just proceed as normal
            JexlNodes.ensureCapacity(andNode, node.jjtGetNumChildren());
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                JexlNode newChild = (JexlNode) node.jjtGetChild(i).jjtAccept(this, data);
                andNode.jjtAddChild(newChild, i);
                newChild.jjtSetParent(andNode);
            }
        }
        
        return andNode;
    }
    
    protected JexlNode coalesceBoundedRanges(Map<LiteralRange<?>,List<JexlNode>> ranges, List<JexlNode> leaves, ASTAndNode currentNode, JexlNode newNode,
                    Object data) {
        // Sanity check to ensure that we found some nodes (redundant since we couldn't have made a bounded LiteralRange in the first
        // place if we had found not range nodes)
        if (ranges.isEmpty()) {
            log.debug("Cannot find range operator nodes that encompass this query. Not proceeding with range expansion for this node.");
            return currentNode;
        }
        
        // Add all children in this AND/OR which are not a part of the range
        JexlNodes.ensureCapacity(newNode, leaves.size() + ranges.size());
        int index = 0;
        for (; index < leaves.size(); index++) {
            log.debug(leaves.get(index).image);
            // Add each child which is not a part of the bounded range, visiting them first
            JexlNode visitedChild = (JexlNode) leaves.get(index).jjtAccept(this, null);
            newNode.jjtAddChild(visitedChild, index);
            visitedChild.jjtSetParent(newNode);
        }
        
        for (Map.Entry<LiteralRange<?>,List<JexlNode>> range : ranges.entrySet()) {
            // If we have any terms that we expanded, wrap them in parens and add them to the parent
            ASTAndNode onlyRangeNodes = new ASTAndNode(ParserTreeConstants.JJTANDNODE);
            
            JexlNodes.ensureCapacity(onlyRangeNodes, range.getValue().size());
            for (int i = 0; i < range.getValue().size(); i++) {
                JexlNode node = range.getValue().get(i);
                onlyRangeNodes.jjtAddChild(node, i);
                node.jjtSetParent(onlyRangeNodes);
            }
            
            JexlNode wrappedNode = JexlNodeFactory.wrap(onlyRangeNodes);
            
            // Set the parent and child pointers accordingly
            wrappedNode.jjtSetParent(newNode);
            newNode.jjtAddChild(wrappedNode, index++);
        }
        
        // If we had no other nodes than this bounded range, we can strip out the original parent
        if (newNode.jjtGetNumChildren() == 1) {
            return newNode.jjtGetChild(0);
        }
        
        return newNode;
    }
}
