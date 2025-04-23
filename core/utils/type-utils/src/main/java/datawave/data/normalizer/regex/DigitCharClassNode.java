package datawave.data.normalizer.regex;

import java.util.Map;

import datawave.data.normalizer.regex.visitor.Visitor;

/**
 * Represents the digit character class {@code \d} in a regex pattern.
 */
public class DigitCharClassNode extends Node {
    
    protected DigitCharClassNode() {
        super();
    }
    
    public DigitCharClassNode(Map<String,String> properties) {
        super(properties);
    }
    
    @Override
    public NodeType getType() {
        return NodeType.DIGIT_CHAR_CLASS;
    }
    
    @Override
    public Object accept(Visitor visitor, Object data) {
        return visitor.visitDigitChar(this, data);
    }
    
    @Override
    public DigitCharClassNode shallowCopy() {
        return new DigitCharClassNode(this.properties);
    }
}
