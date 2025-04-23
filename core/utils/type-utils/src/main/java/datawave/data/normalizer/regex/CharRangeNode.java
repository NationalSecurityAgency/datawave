package datawave.data.normalizer.regex;

import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

import datawave.data.normalizer.regex.visitor.Visitor;

/**
 * Represents a character range defined in a character class in a regex pattern.
 */
public class CharRangeNode extends Node {
    
    public static final String PROPERTY_START = "start";
    public static final String PROPERTY_END = "end";
    
    public CharRangeNode() {}
    
    public CharRangeNode(Map<String,String> properties) {
        super(properties);
    }
    
    public CharRangeNode(char start, char end) {
        setStart(start);
        setEnd(end);
    }
    
    public char getStart() {
        return getProperty(PROPERTY_START).charAt(0);
    }
    
    public void setStart(char start) {
        setProperty(PROPERTY_START, String.valueOf(start));
    }
    
    public char getEnd() {
        return getProperty(PROPERTY_END).charAt(0);
    }
    
    public void setEnd(char end) {
        setProperty(PROPERTY_END, String.valueOf(end));
    }
    
    @Override
    public NodeType getType() {
        return NodeType.CHAR_RANGE;
    }
    
    @Override
    public Object accept(Visitor visitor, Object data) {
        return visitor.visitCharRange(this, data);
    }
    
    @Override
    public CharRangeNode shallowCopy() {
        return new CharRangeNode(this.properties);
    }
    
}
