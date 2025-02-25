package datawave.data.normalizer.regex;

import java.util.List;
import java.util.Map;

import datawave.data.normalizer.regex.visitor.Visitor;

/**
 * Represents some subset or the full part of a regex pattern.
 */
public class ExpressionNode extends Node {
    
    public ExpressionNode() {}
    
    public ExpressionNode(Node child) {
        super(child);
    }
    
    public ExpressionNode(List<? extends Node> children) {
        super(children);
    }
    
    public ExpressionNode(Map<String,String> properties) {
        super(properties);
    }
    
    @Override
    public NodeType getType() {
        return NodeType.EXPRESSION;
    }
    
    @Override
    public Object accept(Visitor visitor, Object data) {
        return visitor.visitExpression(this, data);
    }
    
    @Override
    public ExpressionNode shallowCopy() {
        return new ExpressionNode(this.properties);
    }
}
