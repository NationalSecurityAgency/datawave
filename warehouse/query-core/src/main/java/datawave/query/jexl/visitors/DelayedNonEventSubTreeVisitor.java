package datawave.query.jexl.visitors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.logic.IndexIteratorBridge;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import org.apache.accumulo.core.data.Key;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.JexlNode;

import java.util.Set;

/**
 * Visitor builds a map from all nonEvent fields contained within delayed subtrees to their respective processing nodes. This map then can be used with the
 * DelayedNonEventIndexContext to fetch these values at evaluation time. This visitor will modify/destroy the tree so a copy should always be used.
 */
public class DelayedNonEventSubTreeVisitor extends BaseVisitor {
    private IteratorBuildingVisitor iteratorBuildingVisitor;
    private Set<String> nonEventFields;
    private Multimap<String,JexlNode> delayedNonEventFieldMapNodes;
    
    public static Multimap<String,JexlNode> getDelayedNonEventFieldMap(IteratorBuildingVisitor iteratorBuildingVisitor, ASTJexlScript script,
                    Set<String> nonEventFields) {
        // create a safe copy to rip apart
        ASTJexlScript copy = TreeFlatteningRebuildingVisitor.flatten(script);
        
        // run the visitor on the copy
        DelayedNonEventSubTreeVisitor visitor = new DelayedNonEventSubTreeVisitor(iteratorBuildingVisitor, nonEventFields);
        copy.jjtAccept(visitor, null);
        
        return visitor.delayedNonEventFieldMapNodes;
    }
    
    private DelayedNonEventSubTreeVisitor(IteratorBuildingVisitor iteratorBuildingVisitor, Set<String> nonEventFields) {
        this.iteratorBuildingVisitor = iteratorBuildingVisitor;
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
        if (ASTDelayedPredicate.instanceOf(node) || (data != null && (boolean) data)) {
            // strip off only the delayed predicate, leave any other markers intact since they may be necessary to properly process inside the
            // IteratorBuildingVisitor
            JexlNode candidate = QueryPropertyMarker.getQueryPropertySource(node, ASTDelayedPredicate.class);
            
            if (candidate == null) {
                // we are in a recursive situation inside an delayed predicate, evaluate the node
                candidate = node;
            }
            
            extractDelayedFields(candidate);
            
            // continue processing the tree in case the IteratorBuildingVisitor decided to discard leafs. This is not the most efficient or ideal
            // but it avoids logic problems in AndIterator/OrIterator that haven't yet been fixed.
            return super.visit(candidate, true);
        }
        
        // it wasn't a delayed node, process down the tree
        return super.visit(node, data);
    }
    
    @Override
    public Object visit(ASTOrNode node, Object data) {
        if (data != null && (boolean) data) {
            // continue processing the tree in case the IteratorBuildingVisitor decided to discard leafs. This is not the most efficient or ideal
            // but it avoids logic problems in AndIterator/OrIterator that haven't yet been fixed.
            extractDelayedFields(node);
        }
        
        return super.visit(node, data);
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        if (data != null && (boolean) data) {
            // continue processing the tree in case the IteratorBuildingVisitor decided to discard leafs. This is not the most efficient or ideal
            // but it avoids logic problems in AndIterator/OrIterator that haven't yet been fixed.
            extractDelayedFields(node);
        }
        
        return super.visit(node, data);
    }
    
    @Override
    public Object visit(ASTNENode node, Object data) {
        if (data != null && (boolean) data) {
            // continue processing the tree in case the IteratorBuildingVisitor decided to discard leafs. This is not the most efficient or ideal
            // but it avoids logic problems in AndIterator/OrIterator that haven't yet been fixed.
            extractDelayedFields(node);
        }
        
        return super.visit(node, data);
    }
    
    private void extractDelayedFields(JexlNode node) {
        // reset the root before processing a a node
        iteratorBuildingVisitor.resetRoot();
        
        // use the iterator building visitor to build a field to node map for each delayed node
        node.jjtAccept(iteratorBuildingVisitor, null);
        NestedIterator<Key> root = iteratorBuildingVisitor.root();
        if (root != null) {
            for (NestedIterator<Key> leaf : root.leaves()) {
                // only IndexIteratorBridge nodes matter, everything else can be ignored
                if (leaf instanceof IndexIteratorBridge) {
                    String fieldName = ((IndexIteratorBridge) leaf).getField();
                    if (nonEventFields.contains(fieldName)) {
                        JexlNode leafNode = ((IndexIteratorBridge) leaf).getSourceNode();
                        JexlNode targetNode = leafNode;
                        
                        delayedNonEventFieldMapNodes.put(fieldName, targetNode);
                    }
                }
            }
        }
    }
}
