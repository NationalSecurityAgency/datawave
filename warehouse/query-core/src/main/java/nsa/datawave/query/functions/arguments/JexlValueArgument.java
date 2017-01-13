package nsa.datawave.query.functions.arguments;

import org.apache.commons.jexl2.parser.JexlNode;

@Deprecated
public class JexlValueArgument extends DefaultJexlArgument {
    private String[] fieldNames = null;
    
    public JexlValueArgument(JexlNode node) {
        this("value", node);
    }
    
    public JexlValueArgument(String name, JexlNode node) {
        super(JexlArgumentType.VALUE, name, node, false);
    }
    
    public JexlValueArgument(JexlNode node, String[] fieldNames) {
        this("value", node, fieldNames);
    }
    
    public JexlValueArgument(String name, JexlNode node, String[] fieldNames) {
        this(name, node);
        this.fieldNames = fieldNames;
    }
    
    public JexlValueArgument(JexlNode node, JexlNode fieldNameNode) {
        this("value", node, fieldNameNode);
    }
    
    public JexlValueArgument(String name, JexlNode node, JexlNode fieldNameNode) {
        this(name, node);
        this.fieldNames = new String[] {fieldNameNode.image};
    }
    
    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }
    
}
