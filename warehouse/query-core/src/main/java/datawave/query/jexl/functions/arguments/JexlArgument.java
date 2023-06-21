package datawave.query.jexl.functions.arguments;

import org.apache.commons.jexl2.parser.JexlNode;

public interface JexlArgument {

    enum JexlArgumentType {
        FIELD_NAME, VALUE, REGEX
    }

    String getJexlArgumentName();

    JexlNode getJexlArgumentNode();

    JexlArgumentType getArgumentType();

    // for a field name argument, this will return the one field,
    // however for a regex or value this will return the fieldname that will be matched against (possible another argument or implicit)
    String[] getFieldNames();
}
