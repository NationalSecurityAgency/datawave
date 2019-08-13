package datawave.query.jexl.nodes;

import java.util.SortedSet;

import org.apache.commons.lang.builder.HashCodeBuilder;

import com.google.common.collect.Sets;

public class TreeHashNode implements Comparable<TreeHashNode> {
    
    private int size = 0;
    private HashCodeBuilder builder;
    private SortedSet<String> nodes;
    
    public TreeHashNode() {
        nodes = Sets.newTreeSet();
        builder = new HashCodeBuilder();
    }
    
    public TreeHashNode append(String node) {
        builder.append(node);
        nodes.add(node);
        size++;
        return this;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TreeHashNode) {
            return nodes.equals(((TreeHashNode) obj).nodes);
        }
        return false;
    }
    
    public int length() {
        return size;
    }
    
    @Override
    public int hashCode() {
        return builder.hashCode();
    }
    
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(hashCode()).append(" ").append(nodes);
        return builder.toString();
    }
    
    @Override
    public int compareTo(TreeHashNode other) {
        // Compare number of additions first
        if (size != other.size) {
            return size - other.size;
        }
        
        // Compare size of nodes
        if (nodes.size() != other.nodes.size()) {
            return nodes.size() - other.nodes.size();
        }
        
        // Compare hash codes.
        return hashCode() - other.hashCode();
    }
}
