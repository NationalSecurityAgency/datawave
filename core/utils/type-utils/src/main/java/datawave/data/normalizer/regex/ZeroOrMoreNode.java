package datawave.data.normalizer.regex;

import java.util.Map;

import datawave.data.normalizer.regex.visitor.Visitor;

/**
 * Represents the star in a regex pattern.
 */
public class ZeroOrMoreNode extends Node {
    
    public ZeroOrMoreNode() {}
    
    public ZeroOrMoreNode(Map<String,String> properties) {
        super(properties);
    }
    
    @Override
    public NodeType getType() {
        return NodeType.ZERO_OR_MORE;
    }
    
    @Override
    public Object accept(Visitor visitor, Object data) {
        return visitor.visitZeroToMany(this, data);
    }
    
    @Override
    public ZeroOrMoreNode shallowCopy() {
        return new ZeroOrMoreNode(this.properties);
    }
}
