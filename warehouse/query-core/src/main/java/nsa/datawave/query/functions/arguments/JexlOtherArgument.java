package nsa.datawave.query.functions.arguments;

import org.apache.commons.jexl2.parser.JexlNode;

@Deprecated
public class JexlOtherArgument extends DefaultJexlArgument {
    private String[] fieldNames = null;
    
    public JexlOtherArgument(JexlNode node, boolean contextReference) {
        this("other", node, contextReference);
    }
    
    public JexlOtherArgument(String name, JexlNode node, boolean contextReference) {
        super(JexlArgumentType.OTHER, name, node, contextReference);
    }
    
    public JexlOtherArgument(JexlNode node, boolean contextReference, String[] fieldNames) {
        this("other", node, contextReference, fieldNames);
    }
    
    public JexlOtherArgument(String name, JexlNode node, boolean contextReference, String[] fieldNames) {
        this(name, node, contextReference);
        this.fieldNames = fieldNames;
    }
    
    public JexlOtherArgument(JexlNode node, boolean contextReference, JexlNode fieldNameNode) {
        this("other", node, contextReference, fieldNameNode);
    }
    
    public JexlOtherArgument(String name, JexlNode node, boolean contextReference, JexlNode fieldNameNode) {
        this(name, node, contextReference);
        this.fieldNames = new String[] {fieldNameNode.image};
    }
    
    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }
    
}
