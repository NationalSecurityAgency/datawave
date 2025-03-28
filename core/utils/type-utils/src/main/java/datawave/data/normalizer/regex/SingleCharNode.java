package datawave.data.normalizer.regex;

import java.util.Map;

import datawave.data.normalizer.regex.visitor.Visitor;

/**
 * Represents a single, non-special character in a regex pattern.
 */
public class SingleCharNode extends Node {
    
    public static final String PROPERTY_CHAR = "char";
    
    public SingleCharNode(char character) {
        setCharacter(character);
    }
    
    public char getCharacter() {
        return getProperty(PROPERTY_CHAR).charAt(0);
    }
    
    public void setCharacter(char character) {
        setProperty(PROPERTY_CHAR, String.valueOf(character));
    }
    
    public SingleCharNode() {}
    
    public SingleCharNode(Map<String,String> properties) {
        super(properties);
    }
    
    @Override
    public NodeType getType() {
        return NodeType.SINGLE_CHAR;
    }
    
    @Override
    public Object accept(Visitor visitor, Object data) {
        return visitor.visitSingleChar(this, data);
    }
    
    @Override
    public SingleCharNode shallowCopy() {
        return new SingleCharNode(this.properties);
    }
}
