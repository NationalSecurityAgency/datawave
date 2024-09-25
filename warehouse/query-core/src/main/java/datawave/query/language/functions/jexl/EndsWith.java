package datawave.query.language.functions.jexl;

import java.text.MessageFormat;
import java.util.ArrayList;

import org.apache.commons.jexl3.parser.ASTERNode;

import datawave.query.jexl.functions.QueryFunctions;
import datawave.query.language.functions.QueryFunction;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

/**
 * Lucene function that maps to a {@link ASTERNode} with a leading regex.
 */
public class EndsWith extends JexlQueryFunction {

    public EndsWith() {
        super(QueryFunctions.ENDS_WITH, new ArrayList<>());
    }

    @Override
    public void validate() throws IllegalArgumentException {
        // should always have two arguments, a field and a value
        if (parameterList == null || parameterList.size() != 2) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, MessageFormat.format("{0}", this.name));
            throw new IllegalArgumentException(qe);
        }
    }

    @Override
    public QueryFunction duplicate() {
        return new EndsWith();
    }

    @Override
    public String toString() {
        validate();
        return parameterList.get(0) + " =~ '.*" + parameterList.get(1) + "'";
    }
}
