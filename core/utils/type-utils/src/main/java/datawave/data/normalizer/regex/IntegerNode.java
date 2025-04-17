package datawave.data.normalizer.regex;

import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

import datawave.data.normalizer.regex.visitor.Visitor;

/**
 * Represents an integer parsed in a regex repetition that did not contain a range, e.g. {@code {3}}.
 */
public class IntegerNode extends Node {
    
    public static final String PROPERTY_VALUE = "value";
    
    public IntegerNode() {}
    
    public IntegerNode(int value) {
        setValue(value);
    }
    
    public IntegerNode(Map<String,String> properties) {
        super(properties);
    }
    
    public int getValue() {
        return Integer.parseInt(getProperty(PROPERTY_VALUE));
    }
    
    public void setValue(int value) {
        setProperty(PROPERTY_VALUE, String.valueOf(value));
    }
    
    @Override
    public NodeType getType() {
        return NodeType.INTEGER;
    }
    
    @Override
    public Object accept(Visitor visitor, Object data) {
        return visitor.visitInteger(this, data);
    }
    
    @Override
    public IntegerNode shallowCopy() {
        return new IntegerNode(properties);
    }
}
