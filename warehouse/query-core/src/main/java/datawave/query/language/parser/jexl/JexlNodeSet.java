package datawave.query.language.parser.jexl;

import com.google.common.collect.Sets;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.ExceededTermThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededValueThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.IndexHoleMarkerJexlNode;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static datawave.query.jexl.JexlASTHelper.nodeToKey;

/**
 * Utility class that implements the {@link Set} interface for use with a collection of Jexl nodes.
 *
 * Element uniqueness is computed using {@link JexlASTHelper#nodeToKey(JexlNode)} for keys.
 *
 * If a Jexl node and it's {@link ASTDelayedPredicate} equivalent are both added to the set, the set will accept the delayed predicate.
 *
 * The JexlNodeSet is considered invalid if any of the underlying Jexl nodes change.
 */
public class JexlNodeSet implements Set<JexlNode> {
    
    // Internal map of node keys to nodes;
    private final Map<String,JexlNode> nodeMap;
    
    private static final Logger log = Logger.getLogger(JexlNodeSet.class);
    
    public JexlNodeSet() {
        this.nodeMap = new HashMap<>();
    }
    
    /**
     * Method to access all the Jexl nodes in the set.
     *
     * @return - the underlying node map values.
     */
    public Collection<JexlNode> getNodes() {
        return nodeMap.values();
    }
    
    /**
     * Get the set of generated node keys.
     * 
     * @return - the underlying node map keySet.
     */
    public Set<String> getNodeKeys() {
        return nodeMap.keySet();
    }
    
    @Override
    public int size() {
        return nodeMap.size();
    }
    
    @Override
    public boolean isEmpty() {
        return nodeMap.isEmpty();
    }
    
    @Override
    public boolean contains(Object o) {
        if (o instanceof JexlNode) {
            String nodeKey = nodeToKey((JexlNode) o);
            return nodeMap.containsKey(nodeKey);
        }
        return false;
    }
    
    @Override
    public Iterator<JexlNode> iterator() {
        return Collections.unmodifiableCollection(nodeMap.values()).iterator();
    }
    
    @Override
    public Object[] toArray() {
        return nodeMap.entrySet().toArray();
    }
    
    @Override
    public <T> T[] toArray(T[] ts) {
        throw new UnsupportedOperationException("JexlNodeSet does not support toArray() calls to pre-allocated arrays.");
    }
    
    @Override
    public boolean add(JexlNode node) {
        if (isDelayed(node)) {
            if (log.isTraceEnabled()) {
                log.trace("Trying to add a delayed node: " + nodeToKey(node));
            }
            JexlNode sourceNode = getSourceNode(node);
            if (contains(sourceNode)) {
                if (log.isTraceEnabled()) {
                    log.trace("Source of delayed node already exists in node set.");
                }
                if (remove(sourceNode)) {
                    if (log.isTraceEnabled()) {
                        log.trace("Removing source node " + sourceNode + " from node set prior to adding delayed predicate: " + node);
                    }
                }
            }
            // Add the delayed node with a node key of just the source node.
            return add(nodeToKey(sourceNode), node);
        } else {
            return add(nodeToKey(node), node);
        }
    }
    
    /**
     * You had better know what you're doing if you use this method.
     *
     * Defeat node duplicates when the same node is wrapped in an {@link ASTDelayedPredicate} marker.
     *
     * For example "FOO == 'bar'" and "(DELAYED && FOO == 'bar'" will never be in the same set.
     *
     * @param nodeKey
     *            - a node key.
     * @param node
     *            - a Jexl node.
     * @return - true if the JexlNodeSet was modified, false if not.
     */
    protected boolean add(String nodeKey, JexlNode node) {
        if (!nodeMap.containsKey(nodeKey)) {
            nodeMap.put(nodeKey, node);
            return true;
        }
        return false;
    }
    
    /**
     * Remove the specified object from the set if it is a Jexl node.
     *
     * Builds a node key for the Jexl node and delegates to {@link #remove(String, Object)}
     *
     * @param o
     *            - object to be removed from the set of Jexl nodes.
     * @return - True if the set contained the specified element.
     */
    @Override
    public boolean remove(Object o) {
        if (o instanceof JexlNode) {
            // Remove by value
            String nodeKey = nodeToKey((JexlNode) o);
            return remove(nodeKey, o);
        } else if (o instanceof String) {
            // Remove by key
            JexlNode node = nodeMap.get(o);
            return remove((String) o, node);
        }
        return false;
    }
    
    /**
     *
     * @param nodeKey
     *            - key for the object to be removed.
     * @param o
     *            - the object to be removed.
     * @return - True, if the set contained the specified element.
     */
    private boolean remove(String nodeKey, Object o) {
        return nodeMap.remove(nodeKey, nodeMap.get(nodeKey));
    }
    
    @Override
    public boolean containsAll(Collection<?> collection) {
        if (collection != null) {
            for (Object o : collection) {
                if (!contains(o)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
    
    @Override
    public boolean addAll(Collection<? extends JexlNode> collection) {
        boolean modified = false;
        if (collection != null) {
            for (Object o : collection) {
                if (o instanceof JexlNode) {
                    if (add((JexlNode) o)) {
                        modified = true;
                    }
                }
            }
        }
        return modified;
    }
    
    @Override
    public boolean retainAll(Collection<?> collection) {
        Set<String> retainKeys = new HashSet<>();
        if (collection != null) {
            for (Object o : collection) {
                retainKeys.add(nodeToKey((JexlNode) o));
            }
        }
        
        boolean modified = false;
        for (String key : Sets.newHashSet(nodeMap.keySet())) {
            if (!retainKeys.contains(key)) {
                if (remove(key, nodeMap.get(key))) {
                    modified = true;
                }
            }
        }
        return modified;
    }
    
    @Override
    public boolean removeAll(Collection<?> collection) {
        boolean modified = false;
        if (collection != null) {
            for (Object o : collection) {
                if (remove(o)) {
                    modified = true;
                }
            }
        }
        return modified;
    }
    
    @Override
    public void clear() {
        this.nodeMap.clear();
    }
    
    /**
     * Copied from IndexInfo so that JexlNodeSet can handle adding delayed predicates.
     *
     * (FOO == 'bar') and (DELAYED && FOO == 'bar') are logically equivalent.
     *
     * @param delayedNode
     * @return
     */
    private JexlNode getSourceNode(JexlNode delayedNode) {
        
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
    
    // Is a node delayed?
    protected boolean isDelayed(JexlNode testNode) {
        if (ASTDelayedPredicate.instanceOf(testNode)) {
            return true;
        } else if (IndexHoleMarkerJexlNode.instanceOf(testNode)) {
            return true;
        } else if (ExceededValueThresholdMarkerJexlNode.instanceOf(testNode)) {
            return true;
        } else if (ExceededTermThresholdMarkerJexlNode.instanceOf(testNode)) {
            return true;
        } else {
            return false;
        }
    }
}
