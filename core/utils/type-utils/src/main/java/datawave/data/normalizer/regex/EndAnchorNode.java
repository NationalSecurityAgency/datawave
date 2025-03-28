package datawave.data.normalizer.regex;

import java.util.Map;

import datawave.data.normalizer.regex.visitor.Visitor;

/**
 * Represents a regex end anchor, i.e. {@code $}.
 */
public class EndAnchorNode extends Node {
    
    public EndAnchorNode() {}
    
    public EndAnchorNode(Map<String,String> properties) {
        super(properties);
    }
    
    @Override
    public NodeType getType() {
        return NodeType.END_ANCHOR;
    }
    
    @Override
    public Object accept(Visitor visitor, Object data) {
        return visitor.visitEndAnchor(this, data);
    }
    
    @Override
    public Node shallowCopy() {
        return new EndAnchorNode(this.properties);
    }
}
