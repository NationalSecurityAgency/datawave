package datawave.query.language.functions.jexl;

import java.text.MessageFormat;
import java.util.ArrayList;

import datawave.query.common.grouping.GroupFields;
import datawave.query.jexl.functions.QueryFunctions;
import datawave.query.language.functions.QueryFunction;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

public class GroupBy extends JexlQueryFunction {

    public GroupBy() {
        super(QueryFunctions.GROUPBY_FUNCTION, new ArrayList<>());
    }

    /**
     * query options is a list of fields. Cannot be the empty list.
     *
     * @throws IllegalArgumentException
     *             for illegal arguments
     */
    @Override
    public void validate() throws IllegalArgumentException {
        if (this.parameterList.isEmpty()) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS,
                            MessageFormat.format("{0} requires at least one argument", this.name));
            throw new IllegalArgumentException(qe);
        } else {
            String parameters = String.join(",", parameterList);
            try {
                GroupFields.from(parameters);
            } catch (Exception e) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, e,
                                MessageFormat.format("Unable to parse fields from arguments for function {0}", this.name));
                throw new IllegalArgumentException(qe);
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(QueryFunctions.QUERY_FUNCTION_NAMESPACE).append(':').append(QueryFunctions.GROUPBY_FUNCTION);
        if (parameterList.isEmpty()) {
            sb.append("()");
        } else {
            char separator = '(';
            for (String parm : parameterList) {
                sb.append(separator).append(escapeString(parm));
                separator = ',';
            }
            sb.append(')');
        }
        return sb.toString();
    }

    @Override
    public QueryFunction duplicate() {
        return new GroupBy();
    }
}
