package datawave.data.normalizer.regex;

import java.util.Map;

import datawave.data.normalizer.regex.visitor.Visitor;

/**
 * Represents a dot in a regex pattern.
 */
public class AnyCharNode extends Node {
    
    public AnyCharNode() {}
    
    public AnyCharNode(Map<String,String> properties) {
        super(properties);
    }
    
    @Override
    public NodeType getType() {
        return NodeType.ANY_CHAR;
    }
    
    @Override
    public Object accept(Visitor visitor, Object data) {
        return visitor.visitAnyChar(this, data);
    }
    
    @Override
    public AnyCharNode shallowCopy() {
        return new AnyCharNode(this.properties);
    }
}
