package datawave.data.normalizer.regex;

import java.util.Map;

import datawave.data.normalizer.regex.visitor.Visitor;

/**
 * Represents a repetition requirement in a regex pattern, e.g. {@code {3}}.
 */
public class RepetitionNode extends Node {
    
    public RepetitionNode() {}
    
    public RepetitionNode(Node child) {
        super(child);
    }
    
    public RepetitionNode(Map<String,String> properties) {
        super(properties);
    }
    
    @Override
    public NodeType getType() {
        return NodeType.REPETITION;
    }
    
    @Override
    public Object accept(Visitor visitor, Object data) {
        return visitor.visitRepetition(this, data);
    }
    
    @Override
    public RepetitionNode shallowCopy() {
        return new RepetitionNode(this.properties);
    }
}
