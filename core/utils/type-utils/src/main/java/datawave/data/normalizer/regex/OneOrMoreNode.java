package datawave.data.normalizer.regex;

import java.util.Map;

import datawave.data.normalizer.regex.visitor.Visitor;

/**
 * Represents the plus sign in a regex pattern.
 */
public class OneOrMoreNode extends Node {
    
    public OneOrMoreNode() {}
    
    public OneOrMoreNode(Map<String,String> properties) {
        super(properties);
    }
    
    @Override
    public NodeType getType() {
        return NodeType.ONE_OR_MORE;
    }
    
    @Override
    public Object accept(Visitor visitor, Object data) {
        return visitor.visitOneToMany(this, data);
    }
    
    @Override
    public OneOrMoreNode shallowCopy() {
        return new OneOrMoreNode(this.properties);
    }
}
