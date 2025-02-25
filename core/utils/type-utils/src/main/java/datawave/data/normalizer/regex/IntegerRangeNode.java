package datawave.data.normalizer.regex;

import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

import datawave.data.normalizer.regex.visitor.Visitor;

/**
 * Represents an integer range parsed from a regex repetition that specified a range, e.g. {@code {3,}} or {@code {3,10}}.
 */
public class IntegerRangeNode extends Node {
    
    public static final String PROPERTY_START = "start";
    public static final String PROPERTY_END = "end";
    
    public IntegerRangeNode() {}
    
    public IntegerRangeNode(int start, Integer end) {
        setStart(start);
        setEnd(end);
    }
    
    public IntegerRangeNode(Map<String,String> properties) {
        super(properties);
    }
    
    public int getStart() {
        return Integer.parseInt(getProperty(PROPERTY_START));
    }
    
    public void setStart(int start) {
        setProperty(PROPERTY_START, String.valueOf(start));
    }
    
    public Integer getEnd() {
        if (hasProperty(PROPERTY_END)) {
            return Integer.valueOf(getProperty(PROPERTY_END));
        }
        return null;
    }
    
    public void setEnd(Integer end) {
        if (end != null) {
            setProperty(PROPERTY_END, String.valueOf(end));
        }
    }
    
    public boolean isEndBounded() {
        return hasProperty(PROPERTY_END);
    }
    
    @Override
    public NodeType getType() {
        return NodeType.INTEGER_RANGE;
    }
    
    @Override
    public Object accept(Visitor visitor, Object data) {
        return visitor.visitIntegerRange(this, data);
    }
    
    @Override
    public IntegerRangeNode shallowCopy() {
        return new IntegerRangeNode(this.properties);
    }
}
