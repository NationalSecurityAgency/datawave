package datawave.query.language.functions.jexl;

import java.text.MessageFormat;
import java.util.ArrayList;

import datawave.query.attributes.UniqueFields;
import datawave.query.jexl.functions.QueryFunctions;
import datawave.query.language.functions.QueryFunction;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

/**
 * Function to determine most recent uniqueness among documents given a set of fields and the levels of granularity that should be used for each fields. This
 * function accepts a list of fields with specified granularity levels in the format {@code field[ALL],dateField[DAY,HOUR,MINUTE]}. See {@link UniqueFields} for
 * additional documentation on supported formatting.
 */
public class MostRecentUnique extends JexlQueryFunction {

    public MostRecentUnique() {
        super(QueryFunctions.MOST_RECENT_PREFIX + QueryFunctions.UNIQUE_FUNCTION, new ArrayList<>());
    }

    /**
     * query options contain a list of fields. Cannot be the empty list.
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
                UniqueFields.from(parameters);
            } catch (Exception e) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS,
                                MessageFormat.format("Unable to parse unique fields from arguments for function {0}", this.name));
                throw new IllegalArgumentException(qe);
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(QueryFunctions.QUERY_FUNCTION_NAMESPACE).append(':').append(QueryFunctions.MOST_RECENT_PREFIX).append(QueryFunctions.UNIQUE_FUNCTION);
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
        return new MostRecentUnique();
    }

}
