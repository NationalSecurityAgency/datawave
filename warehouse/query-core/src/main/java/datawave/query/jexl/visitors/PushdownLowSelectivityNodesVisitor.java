package datawave.query.jexl.visitors;

import datawave.data.MetadataCardinalityCounts;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.util.MetadataHelper;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.NoSuchElementException;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.DELAYED;

/**
 * Visitor meant to 'push down' predicates for expressions that reference low selectable fields.
 */
public class PushdownLowSelectivityNodesVisitor extends ShortCircuitBaseVisitor {
    
    protected MetadataHelper helper;
    protected ShardQueryConfiguration config;
    
    public PushdownLowSelectivityNodesVisitor(ShardQueryConfiguration config, MetadataHelper helper) {
        this.helper = helper;
        this.config = config;
    }
    
    private static final Logger log = Logger.getLogger(PushdownLowSelectivityNodesVisitor.class);
    
    public static <T extends JexlNode> T pushdownLowSelectiveTerms(T queryTree, ShardQueryConfiguration config, MetadataHelper helper) {
        PushdownLowSelectivityNodesVisitor visitor = new PushdownLowSelectivityNodesVisitor(config, helper);
        queryTree.jjtAccept(visitor, null);
        return queryTree;
    }
    
    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        // if not already delayed somehow
        if (!QueryPropertyMarker.findInstance(node).isAnyType()) {
            return super.visit(node, data);
        }
        return data;
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        // if not already delayed somehow
        if (!QueryPropertyMarker.findInstance(node).isAnyType()) {
            return super.visit(node, data);
        }
        return data;
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        // if this node represents a field/value that has poor selectability, then push it down
        if (hasLowSelectability(node)) {
            JexlNodes.swap(node.jjtGetParent(), node, QueryPropertyMarker.create(node, DELAYED));
        }
        return data;
    }
    
    public boolean hasLowSelectability(JexlNode node) {
        try {
            String field = JexlASTHelper.getIdentifier(node);
            Object literal = JexlASTHelper.getLiteralValue(node);
            // We can get the term counts with root auths (ignoring user auths) because this information is not
            // exposed to the user. It is only used to adjust the query planning.
            Map<String,Map<String,MetadataCardinalityCounts>> termCounts = this.helper.getTermCountsWithRootAuths();
            Map<String,MetadataCardinalityCounts> valueCounts = termCounts.get(field);
            if (valueCounts != null) {
                MetadataCardinalityCounts counts = valueCounts.get(String.valueOf(literal));
                if (counts != null) {
                    double selectivity = (double) counts.getFieldValueCount() / (double) counts.getTotalAllFieldAllValueCount();
                    if (selectivity >= config.getMinSelectivity()) {
                        return true;
                    }
                }
            }
        } catch (NoSuchElementException e) {
            log.warn("No identifier or literal found for expression", e);
        } catch (Exception e) {
            log.error("Could not retrieve counts from metadata helper", e);
        }
        return false;
    }
    
}
