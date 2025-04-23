package datawave.query.language.functions.jexl;

import java.text.MessageFormat;
import java.util.ArrayList;

import datawave.query.jexl.functions.QueryFunctions;
import datawave.query.language.functions.QueryFunction;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

/**
 * This function accepts a comma separated list of fields to be excluded from QueryModel expansion. The purpose is to provide users with an easy way to avoid
 * undesirable query model expansions. <br>
 * Note: The exclusion is only applied to the fields in the original query. An original field can be expanded into an excluded field.
 */
public class NoExpansion extends JexlQueryFunction {

    public NoExpansion() {
        super(QueryFunctions.NO_EXPANSION, new ArrayList<>());
    }

    @Override
    public void validate() throws IllegalArgumentException {
        if (this.parameterList.isEmpty()) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS,
                            MessageFormat.format("{0} requires at least one argument", this.name));
            throw new IllegalArgumentException(qe);
        }
    }

    @Override
    public QueryFunction duplicate() {
        return new NoExpansion();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(QueryFunctions.QUERY_FUNCTION_NAMESPACE).append(':').append(QueryFunctions.NO_EXPANSION);
        if (parameterList.isEmpty()) {
            sb.append("()");
        } else {
            char separator = '(';
            for (String param : parameterList) {
                sb.append(separator).append(escapeString(param));
                separator = ',';
            }
            sb.append(')');
        }
        return sb.toString();
    }
}
