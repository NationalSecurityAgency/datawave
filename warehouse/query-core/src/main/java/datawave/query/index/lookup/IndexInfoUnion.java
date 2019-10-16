package datawave.query.index.lookup;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.nodes.ExceededTermThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededValueThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.IndexHoleMarkerJexlNode;
import datawave.query.jexl.visitors.TreeFlatteningRebuildingVisitor;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static datawave.query.jexl.JexlASTHelper.nodeToKey;

/**
 * This class generates the union of two {@link IndexInfo} objects. It handles all combinations of document and infinite ranges and will add in any nodes that
 * were delayed.
 * <p>
 * Consider the following example, where the query 'A OR B' is executed against the index. Suppose that term A hits on documents 1 and 2, and term b hits on
 * documents 2 and 3. Logically, we can represent this as,
 * <p>
 * <code>
 * A = {doc1, doc2}
 * B = {doc2, doc3}
 * </code>
 * <p>
 * This is represented in the code as IndexInfo objects with an IndexMatch object for each hit in the index.
 * <p>
 * <code>
 * IndexInfo(A)
 * doc1 = {A}
 * doc2 = {A}
 * 
 * IndexInfo(B)
 * doc2 = {B}
 * doc3 = {B}
 * </code>
 * <p>
 * The union of IndexInfo(A) and IndexInfo(B) produce the following,
 * <p>
 * <code>
 * IndexInfo(A OR B)
 * doc 1 = {A}
 * doc 2 = {A, B}
 * doc 3 = {B}
 * </code>
 */
public class IndexInfoUnion {
    
    public static IndexInfo union(IndexInfo left, IndexInfo right) {
        return union(left, right, new ArrayList<>());
    }
    
    public static IndexInfo union(IndexInfo left, IndexInfo right, List<JexlNode> delayedNodes) {
        
        // The merged node list is constructed independently of the IndexMatch collection.
        // We do this because at this point we don't know if we are building document ranges or shard/day ranges.
        // See TupleToRange for how the IndexInfo object is used to build query ranges.
        Map<String,JexlNode> mergedNodes = new HashMap<>();
        
        // 1. Unwrap the top level Jexl node for the IndexInfo objects and merge them together.
        mergedNodes = addJexlNodesToMap(mergedNodes, left.myNode);
        mergedNodes = addJexlNodesToMap(mergedNodes, right.myNode);
        
        // 2. Add delayed nodes to the merged nodes
        mergedNodes = addJexlNodesToMap(mergedNodes, delayedNodes);
        
        // 3. Generate the inverted multimap of uids to jexl nodes.
        Set<IndexMatch> matches = mergeIndexMatches(left.uids, right.uids, delayedNodes);
        
        // 4. Build the final IndexInfo object.
        IndexInfo merged = new IndexInfo();
        
        // 5. Handle the uids & uid count
        if (left.isInfinite() || right.isInfinite()) {
            // If either side is infinite we cannot perform a document uid specific lookup.
            merged.uids = ImmutableSortedSet.of();
            merged.count = -1;
        } else {
            merged.uids = ImmutableSortedSet.copyOf(matches);
            merged.count = merged.uids.size();
        }
        
        // 6. Get the set of merged nodes
        Set<JexlNode> nodeSet = new HashSet<>(mergedNodes.values());
        
        // Put the node set into an unwrapped OR node.
        merged.myNode = JexlNodeFactory.createUnwrappedOrNode(nodeSet);
        
        return merged;
    }
    
    /**
     * Add Jexl nodes to the provided mergedNodes map, using the {@link datawave.query.jexl.JexlASTHelper#nodeToKey} method to ensure uniqueness.
     * <p>
     * If the provided Jexl node is a top level OR node, this method will add all the child nodes.
     *
     * @param mergedNodes
     *            - a map of node keys to nodes, used to ensure uniqueness
     * @param node
     *            - Jexl node to be added to the merged nodes map
     * @return - the mergedNodes map
     */
    private static Map<String,JexlNode> addJexlNodesToMap(Map<String,JexlNode> mergedNodes, JexlNode node) {
        if (null != node) {
            JexlNode sourceNode = getSourceNode(node);
            JexlNode topLevelOr = getOrNode(sourceNode);
            if (null == topLevelOr) {
                mergedNodes.put(nodeToKey(sourceNode), node);
            } else {
                for (int ii = 0; ii < topLevelOr.jjtGetNumChildren(); ii++) {
                    sourceNode = getSourceNode(topLevelOr.jjtGetChild(ii));
                    mergedNodes.put(nodeToKey(sourceNode), topLevelOr.jjtGetChild(ii));
                }
            }
        }
        return mergedNodes;
    }
    
    /**
     * Add a collection of Jexl nodes to the provided mergedNodes map.
     *
     * @param mergedNodes
     * @param nodes
     * @return
     */
    private static Map<String,JexlNode> addJexlNodesToMap(Map<String,JexlNode> mergedNodes, Collection<JexlNode> nodes) {
        if (null != nodes && !nodes.isEmpty()) {
            for (JexlNode delayedNode : nodes) {
                JexlNode sourceNode = getSourceNode(delayedNode);
                mergedNodes.put(nodeToKey(sourceNode), delayedNode);
            }
        }
        return mergedNodes;
    }
    
    public static JexlNode getSourceNode(JexlNode delayedNode) {
        
        if (ASTDelayedPredicate.instanceOf(delayedNode)) {
            
            return ASTDelayedPredicate.getQueryPropertySource(delayedNode, ASTDelayedPredicate.class);
        } else if (ExceededValueThresholdMarkerJexlNode.instanceOf(delayedNode)) {
            
            return ExceededValueThresholdMarkerJexlNode.getExceededValueThresholdSource(delayedNode);
        } else if (ExceededTermThresholdMarkerJexlNode.instanceOf(delayedNode)) {
            
            return ExceededTermThresholdMarkerJexlNode.getExceededTermThresholdSource(delayedNode);
        } else if (IndexHoleMarkerJexlNode.instanceOf(delayedNode)) {
            
            return IndexHoleMarkerJexlNode.getIndexHoleSource(delayedNode);
        } else {
            return delayedNode;
        }
    }
    
    private static JexlNode getOrNode(JexlNode node) {
        if (node instanceof ASTOrNode) {
            return node;
        } else if (node instanceof ASTReference) {
            return getOrNode(node.jjtGetChild(0));
        } else if (node instanceof ASTReferenceExpression) {
            return getOrNode(node.jjtGetChild(0));
        }
        return null;
    }
    
    /**
     * Merge the underlying IndexMatch collections from the left and right IndexInfo objects.
     *
     * @param left
     * @param right
     * @param delayedNodes
     * @return
     */
    private static Set<IndexMatch> mergeIndexMatches(ImmutableSortedSet<IndexMatch> left, ImmutableSortedSet<IndexMatch> right, List<JexlNode> delayedNodes) {
        HashMultimap<String,JexlNode> invertedNodeMap = HashMultimap.create();
        HashMultimap<String,String> invertedNodeStringMap = HashMultimap.create();
        for (IndexMatch match : Iterables.concat(left, right)) {
            if (null != match.myNodes && !match.myNodes.isEmpty()) {
                // Calling getNode() will merge the jexl nodes together under a single OR node.
                invertedNodeMap.put(match.uid, match.getNode());
                // Persist the defeat list of node strings. Useful to avoid adding duplicate delayed nodes.
                invertedNodeStringMap.putAll(match.uid, match.nodeStrings);
            }
        }
        
        // Build the new IndexMatch objects from the inverted node map.
        Set<IndexMatch> matches = new HashSet<>(invertedNodeMap.keySet().size());
        for (String uid : invertedNodeMap.keySet()) {
            
            Set<JexlNode> nodes = Sets.newHashSet(invertedNodeMap.get(uid));
            if (!nodes.isEmpty()) {
                
                // Build the new IndexMatch object.
                IndexMatch match = new IndexMatch();
                match.uid = uid;
                match.type = IndexMatchType.OR;
                for (JexlNode node : nodes) {
                    match.add(node);
                }
                
                // Prior to adding the set of delayed nodes, update the node strings in order to defeat duplicate entries.
                match.nodeStrings.addAll(invertedNodeStringMap.get(uid));
                
                // Add delayed nodes last. This prevents the same delayed node set from being added again and again.
                if (null != delayedNodes && !delayedNodes.isEmpty()) {
                    for (JexlNode delayed : delayedNodes) {
                        match.add(delayed);
                    }
                }
                
                matches.add(match);
            }
        }
        return matches;
    }
}
