package datawave.data.normalizer.regex;

import java.util.Collection;
import java.util.Map;

import datawave.data.normalizer.regex.visitor.Visitor;

/**
 * Represents a regex alternation, i.e. {@code |}.
 */
public class AlternationNode extends Node {
    
    public AlternationNode() {}
    
    public AlternationNode(Collection<? extends Node> children) {
        super(children);
    }
    
    public AlternationNode(Map<String,String> properties) {
        super(properties);
    }
    
    @Override
    public NodeType getType() {
        return NodeType.ALTERNATION;
    }
    
    @Override
    public Object accept(Visitor visitor, Object data) {
        return visitor.visitAlternation(this, data);
    }
    
    @Override
    public AlternationNode shallowCopy() {
        return new AlternationNode(this.properties);
    }
    
}
