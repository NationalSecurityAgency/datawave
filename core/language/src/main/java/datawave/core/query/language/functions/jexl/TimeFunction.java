package datawave.core.query.language.functions.jexl;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import datawave.core.query.language.functions.QueryFunction;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

/**
 * function to test whether two key/value pairs match within datafields of the same (grouping context) group For example FROM_ADDRESS.1 == 1.2.3.4 and
 * DIRECTION.1 == 1
 *
 * {@code Collection<?> timeFunction(Object time1, Object time2, String operatorString, String equalityString, long goal)}
 */
public class TimeFunction extends JexlQueryFunction {

    public TimeFunction() {
        super("time_function", new ArrayList<>());
    }

    @Override
    public void validate() throws IllegalArgumentException {
        if (this.parameterList.size() != 5) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, MessageFormat.format("{0}", this.name));
            throw new IllegalArgumentException(qe);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        List<String> params = getParameterList();
        sb.append(params.get(0));
        sb.append(", ");
        sb.append(params.get(1));
        sb.append(", ");
        sb.append(this.escapeString(params.get(2)));
        sb.append(", ");
        sb.append(this.escapeString(params.get(3)));
        sb.append(", ");
        sb.append(params.get(4));
        sb.insert(0, "filter:timeFunction(");
        sb.append(")");
        return sb.toString();
    }

    @Override
    public QueryFunction duplicate() {
        return new TimeFunction();
    }

}
