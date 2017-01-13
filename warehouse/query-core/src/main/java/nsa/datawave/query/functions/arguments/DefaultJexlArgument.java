package nsa.datawave.query.functions.arguments;

import org.apache.commons.jexl2.parser.JexlNode;

@Deprecated
public abstract class DefaultJexlArgument implements JexlArgument {
    
    JexlArgumentType type;
    JexlNode node;
    String name;
    boolean contextReference;
    
    public DefaultJexlArgument(JexlArgumentType type, String name, JexlNode node, boolean contextReference) {
        this.type = type;
        this.name = name;
        this.node = node;
        this.contextReference = contextReference;
    }
    
    @Override
    public String getJexlArgumentName() {
        return name;
    }
    
    @Override
    public JexlNode getJexlArgumentNode() {
        return node;
    }
    
    @Override
    public JexlArgumentType getArgumentType() {
        return type;
    }
    
    @Override
    public boolean isContextReference() {
        return contextReference;
    }
    
    @Override
    public String toString() {
        return name;
    }
}
