package datawave.query.language.parser.jexl;

import com.google.common.collect.Sets;
import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.JexlNode;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Utility class that implements the {@link Set} interface for use with a collection of Jexl nodes.
 *
 * Element uniqueness is computed using {@link JexlASTHelper#nodeToKey(JexlNode)} for keys.
 *
 * Important Note: The set is considered invalid if any of the underlying Jexl nodes change.
 */
public class JexlNodeSet implements Set<JexlNode> {
    
    // Internal map of node keys to nodes;
    private final Map<String,JexlNode> nodeMap;
    
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
            String nodeKey = JexlASTHelper.nodeToKey((JexlNode) o);
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
        String nodeKey = JexlASTHelper.nodeToKey(node);
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
            String nodeKey = JexlASTHelper.nodeToKey((JexlNode) o);
            return remove(nodeKey, o);
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
        return nodeMap.remove(nodeKey, o);
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
                retainKeys.add(JexlASTHelper.nodeToKey((JexlNode) o));
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
}
