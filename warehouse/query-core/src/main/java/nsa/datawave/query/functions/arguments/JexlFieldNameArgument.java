package nsa.datawave.query.functions.arguments;

import org.apache.commons.jexl2.parser.JexlNode;

@Deprecated
public class JexlFieldNameArgument extends DefaultJexlArgument {
    
    public JexlFieldNameArgument(JexlNode node) {
        this("field name", node);
    }
    
    public JexlFieldNameArgument(String name, JexlNode node) {
        this(name, node, true);
    }
    
    public JexlFieldNameArgument(JexlNode node, boolean contextReference) {
        this("field name", node, contextReference);
    }
    
    public JexlFieldNameArgument(String name, JexlNode node, boolean contextReference) {
        super(JexlArgumentType.FIELD_NAME, name, node, contextReference);
    }
    
    @Override
    public String[] getFieldNames() {
        return new String[] {getJexlArgumentNode().image};
    }
}
