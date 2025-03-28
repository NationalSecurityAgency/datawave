package datawave.data.normalizer.regex;

import java.util.Map;
import java.util.Objects;

import datawave.data.normalizer.regex.visitor.Visitor;

/**
 * Represents a character class in a regex pattern encapsulated by {@code [...]}.
 */
public class CharClassNode extends Node {
    
    public static final String PROPERTY_NEGATED = "negated";
    private static final String TRUE = String.valueOf(true);
    
    public CharClassNode() {}
    
    public CharClassNode(boolean negated) {
        setProperty(PROPERTY_NEGATED, String.valueOf(negated));
    }
    
    public CharClassNode(Map<String,String> properties) {
        super(properties);
    }
    
    public boolean isNegated() {
        return hasProperty(PROPERTY_NEGATED) && getProperty(PROPERTY_NEGATED).equals(TRUE);
    }
    
    public void negate() {
        setProperty(PROPERTY_NEGATED, TRUE);
    }
    
    @Override
    public NodeType getType() {
        return NodeType.CHAR_CLASS;
    }
    
    @Override
    public Object accept(Visitor visitor, Object data) {
        return visitor.visitCharClass(this, data);
    }
    
    @Override
    public CharClassNode shallowCopy() {
        return new CharClassNode(this.properties);
    }
}
