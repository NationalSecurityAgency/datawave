package datawave.query.jexl.visitors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.LiteralRange;
import datawave.query.jexl.nodes.ExceededOrThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.JexlNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Visitor builds a map from all nonEvent fields contained within delayed subtrees to their respective processing nodes. This map then can be used with the
 * DelayedNonEventIndexContext to fetch these values at evaluation time. This visitor will modify/destroy the tree so a copy should always be used.
 */
public class DelayedNonEventSubTreeVisitor extends BaseVisitor {
    private Set<String> nonEventFields;
    private Multimap<String,JexlNode> delayedNonEventFieldMapNodes;
    
    public static Multimap<String,JexlNode> getDelayedNonEventFieldMap(ASTJexlScript script, Set<String> nonEventFields) {
        // create a safe copy to rip apart
        ASTJexlScript copy = TreeFlatteningRebuildingVisitor.flatten(script);
        
        // run the visitor on the copy
        DelayedNonEventSubTreeVisitor visitor = new DelayedNonEventSubTreeVisitor(nonEventFields);
        copy.jjtAccept(visitor, null);
        
        return visitor.delayedNonEventFieldMapNodes;
    }
    
    private DelayedNonEventSubTreeVisitor(Set<String> nonEventFields) {
        this.nonEventFields = nonEventFields;
        delayedNonEventFieldMapNodes = HashMultimap.create();
    }
    
    /**
     * Check if the node is an instance of a DelayedPredicate and if so capture all nonEvent delayed nodes, ranges, and node operators into
     * delayedNonEventFieldMapNodes.
     * 
     * @param node
     * @param data
     * @return
     */
    @Override
    public Object visit(ASTAndNode node, Object data) {
        if (JexlASTHelper.isDelayedPredicate(node)) {
            JexlNode candidate = QueryPropertyMarker.getQueryPropertySource(node, null);
            
            // This is to handle long list ivarators
            if (ExceededOrThresholdMarkerJexlNode.instanceOf(candidate)) {
                handleLongListIvarator(candidate);
            } else {
                handleDelayedNodes(candidate);
            }
            
            // this delayed node has been totally handled at this point there is no further processing to do
            return null;
        }
        
        // it wasn't a delayed node, process down the tree
        return super.visit(node, data);
    }
    
    /**
     * Outside of LongListIvarators which are delayed, there are two other categories of nodes, bounded ranges and everything else. Use the JexlASTHelper to
     * extract both and process. Each bounded range should have a new ASTAndNode generated to wrap them only as opposed to whatever is actually in the tree
     * since there may be other nodes included. Everything that isn't a bounded range should still be processed looking for nonEvent fields.
     * 
     * @param node
     */
    private void handleDelayedNodes(JexlNode node) {
        // special handling for bounded ranges
        List<JexlNode> nonBoundedNodes = new ArrayList<>();
        Map<LiteralRange<?>,List<JexlNode>> boundedRangeMap = JexlASTHelper.getBoundedRangesIndexAgnostic(node, nonBoundedNodes, true);
        for (LiteralRange<?> literalRange : boundedRangeMap.keySet()) {
            String fieldName = literalRange.getFieldName();
            if (nonEventFields.contains(fieldName)) {
                // create an AND node to track for delayed bounded range evaluation
                List<JexlNode> literalRangeNodes = boundedRangeMap.get(literalRange);
                JexlNode rangeNode = JexlNodeFactory.createAndNode(literalRangeNodes);
                delayedNonEventFieldMapNodes.put(fieldName, rangeNode);
            }
        }
        
        // for all nodes that are not bounded ranges process them individually
        for (JexlNode otherNode : nonBoundedNodes) {
            otherNode.jjtAccept(this, true);
        }
    }
    
    /**
     * Extract the field and id from an ExceededOrThresholdMarker and its id. If the field is nonEvent add the node to the nodeMap under the id value rather
     * than the field
     * 
     * @param node
     *            the node that was the source of the ExceededOrThresholdMarker
     */
    private void handleLongListIvarator(JexlNode node) {
        // the context lookup will be on the id, but the test should still be against the field
        String field = ExceededOrThresholdMarkerJexlNode.getField(node);
        String id = ExceededOrThresholdMarkerJexlNode.getId(node);
        if (nonEventFields.contains(field)) {
            // long list ivarators require a context lookup on their id, not the field name
            delayedNonEventFieldMapNodes.put(id, node);
        }
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        processLeaf(node, data);
        return null;
    }
    
    @Override
    public Object visit(ASTNENode node, Object data) {
        processLeaf(node, data);
        return null;
    }
    
    @Override
    public Object visit(ASTGENode node, Object data) {
        processLeaf(node, data);
        return null;
    }
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        processLeaf(node, data);
        return null;
    }
    
    @Override
    public Object visit(ASTLENode node, Object data) {
        processLeaf(node, data);
        return null;
    }
    
    @Override
    public Object visit(ASTLTNode node, Object data) {
        processLeaf(node, data);
        return null;
    }
    
    /**
     * When inside of an ASTDelayedPredicate, test the leaf operator's identifier to see if it needs to be processed. If the leaf does need to be processed at
     * this node to the map under that field name
     * 
     * @param leaf
     *            an operator node which should be tested for inclusion of delayed execution
     * @param data
     *            a marker to determine if this node is a child of an ASTDelayedPredicate or not
     */
    private void processLeaf(JexlNode leaf, Object data) {
        if (data instanceof Boolean && ((Boolean) data)) {
            String fieldName = JexlASTHelper.getIdentifier(leaf);
            if (nonEventFields.contains(fieldName)) {
                delayedNonEventFieldMapNodes.put(fieldName, leaf);
            }
        }
    }
}
