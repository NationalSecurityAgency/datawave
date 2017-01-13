package nsa.datawave.query.rewrite.jexl.nodes;

import java.util.SortedSet;

import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.google.common.collect.Sets;

public class TreeHashNode {
    
    int size = 0;
    HashCodeBuilder builder;
    SortedSet<String> nodes;
    
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
}
