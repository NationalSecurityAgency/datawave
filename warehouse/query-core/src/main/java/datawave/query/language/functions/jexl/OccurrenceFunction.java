package datawave.query.language.functions.jexl;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Iterator;

import datawave.query.language.functions.QueryFunction;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

/**
 * <pre>
 * Function arguments are a field name, an operator, and a number
 *
 * occurrence(FOO, '&gt;=', 4) will return true (non-empty set of matches) if there
 * are at least 4 values for the field FOO in a record.
 * </pre>
 */
public class OccurrenceFunction extends JexlQueryFunction {

    public OccurrenceFunction() {
        super("occurrence", new ArrayList<>());
    }

    @Override
    public void validate() throws IllegalArgumentException {
        if (this.parameterList.size() != 3) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, MessageFormat.format("{0}", this.name));
            throw new IllegalArgumentException(qe);
        }
    }

    @Override
    public String toString() {
        Iterator<String> param = getParameterList().iterator();
        StringBuilder f = new StringBuilder(64);
        String field = param.next().toUpperCase();
        String operator = param.next();
        String value = param.next();
        f.append("filter:").append("occurrence");
        f.append('(').append(field).append(", ");
        f.append(this.escapeString(operator)).append(", ");
        f.append(value);
        f.append(')');
        return f.toString();
    }

    @Override
    public QueryFunction duplicate() {
        return new OccurrenceFunction();
    }

}
