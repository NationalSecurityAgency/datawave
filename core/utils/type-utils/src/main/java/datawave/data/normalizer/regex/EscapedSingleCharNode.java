package datawave.data.normalizer.regex;

import java.util.Map;

import datawave.data.normalizer.regex.visitor.Visitor;

/**
 * Represents an escaped character in a regex pattern, e.g. {@code \-}.
 */
public class EscapedSingleCharNode extends Node {
    
    public static final String PROPERTY_CHAR = "char";
    
    public EscapedSingleCharNode() {}
    
    public EscapedSingleCharNode(char character) {
        setCharacter(character);
    }
    
    public EscapedSingleCharNode(Map<String,String> properties) {
        super(properties);
    }
    
    public char getCharacter() {
        return getProperty(PROPERTY_CHAR).charAt(0);
    }
    
    public void setCharacter(char character) {
        setProperty(PROPERTY_CHAR, String.valueOf(character));
    }
    
    @Override
    public NodeType getType() {
        return NodeType.ESCAPED_SINGLE_CHAR;
    }
    
    @Override
    public Object accept(Visitor visitor, Object data) {
        return visitor.visitEscapedSingleChar(this, data);
    }
    
    @Override
    public EscapedSingleCharNode shallowCopy() {
        return new EscapedSingleCharNode(this.properties);
    }
}
