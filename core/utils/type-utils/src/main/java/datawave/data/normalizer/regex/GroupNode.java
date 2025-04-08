package datawave.data.normalizer.regex;

import java.util.List;
import java.util.Map;

import datawave.data.normalizer.regex.visitor.Visitor;

/**
 * Represents a regex group in a regex pattern encapsulated by {@code (...)}.
 */
public class GroupNode extends Node {
    
    public GroupNode() {}
    
    public GroupNode(Node child) {
        super(child);
    }
    
    public GroupNode(List<? extends Node> children) {
        super(children);
    }
    
    public GroupNode(Map<String,String> properties) {
        super(properties);
    }
    
    @Override
    public NodeType getType() {
        return NodeType.GROUP;
    }
    
    @Override
    public Object accept(Visitor visitor, Object data) {
        return visitor.visitGroup(this, data);
    }
    
    @Override
    public GroupNode shallowCopy() {
        return new GroupNode(this.properties);
    }
}
