package datawave.data.normalizer.regex;

import java.util.Map;

import datawave.data.normalizer.regex.visitor.Visitor;

/**
 * Placeholder empty node for empty groups or empty alternation branches.
 */
public class EmptyNode extends Node {
    
    public EmptyNode() {}
    
    public EmptyNode(Map<String,String> properties) {
        super(properties);
    }
    
    @Override
    public NodeType getType() {
        return NodeType.EMPTY;
    }
    
    @Override
    public Object accept(Visitor visitor, Object data) {
        return visitor.visitEmpty(this, data);
    }
    
    @Override
    public Node shallowCopy() {
        return new EmptyNode(this.properties);
    }
}
