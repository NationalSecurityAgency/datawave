package nsa.datawave.query.functions.arguments;

import org.apache.commons.jexl2.parser.JexlNode;

@Deprecated
public class JexlRegexArgument extends DefaultJexlArgument {
    private String[] fieldNames = null;
    
    public JexlRegexArgument(JexlNode node) {
        this("regex", node);
    }
    
    public JexlRegexArgument(String name, JexlNode node) {
        super(JexlArgumentType.REGEX, name, node, false);
    }
    
    public JexlRegexArgument(JexlNode node, String[] fieldNames) {
        this("regex", node, fieldNames);
    }
    
    public JexlRegexArgument(String name, JexlNode node, String[] fieldNames) {
        this(name, node);
        this.fieldNames = fieldNames;
    }
    
    public JexlRegexArgument(JexlNode node, JexlNode fieldNameNode) {
        this("regex", node, fieldNameNode);
    }
    
    public JexlRegexArgument(String name, JexlNode node, JexlNode fieldNameNode) {
        this(name, node);
        this.fieldNames = new String[] {fieldNameNode.image};
    }
    
    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }
    
}
