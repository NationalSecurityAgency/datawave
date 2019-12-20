package datawave.query.jexl.visitors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.logic.IndexIteratorBridge;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import org.apache.accumulo.core.data.Key;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTJexlScript;
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
        if (ASTDelayedPredicate.instanceOf(node)) {
            // strip off only the delayed predicate, leave any other markers intact since they may be necessary to properly process inside the
            // IteratorBuildingVisitor
            JexlNode candidate = QueryPropertyMarker.getQueryPropertySource(node, ASTDelayedPredicate.class);
            
            if (candidate == null) {
                // something unexpected
                throw new IllegalStateException("Could not locate source node for DelayedPredicate: " + JexlStringBuildingVisitor.buildQuery(node));
            }
            
            // reset the root before processing a a node
            iteratorBuildingVisitor.resetRoot();
            
            // use the iterator building visitor to build a field to node map for each delayed node
            candidate.jjtAccept(iteratorBuildingVisitor, null);
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
            
            // this delayed node has been totally handled at this point there is no further processing to do
            return null;
        }
        
        // it wasn't a delayed node, process down the tree
        return super.visit(node, data);
    }
}
