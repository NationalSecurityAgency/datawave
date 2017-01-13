package nsa.datawave.query.functions.arguments;

import org.apache.commons.jexl2.parser.JexlNode;

@Deprecated
public interface JexlArgument {
    
    public static enum JexlArgumentType {
        FIELD_NAME, // A field name argument that may be replaced with a value from the context
        VALUE, // A value supplied by the user which may require normalization
        REGEX, // A regex supplied by the user
        OTHER // something else, such as a constant or a context reference such as termOffsetMap
    }
    
    String getJexlArgumentName();
    
    JexlNode getJexlArgumentNode();
    
    JexlArgumentType getArgumentType();
    
    // for a field name argument, this will return the one field,
    // however for a regex or value this will return the fieldname that will be matched against (possible another argument or implicit)
    String[] getFieldNames();
    
    // is this value a reference to something in the context
    boolean isContextReference();
}
