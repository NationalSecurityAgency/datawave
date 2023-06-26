package datawave.query.language.functions.jexl;

import datawave.query.jexl.functions.QueryFunctions;
import datawave.query.language.functions.QueryFunction;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

import java.text.MessageFormat;
import java.util.ArrayList;

/**
 * Function to specify when excerpts should be included for results for any phrases that were identified as matching hits. This function accepts a list of
 * fields with a corresponding offset value in the format {@code FIELD/offset,FIELD/offset,...}. See {@link datawave.query.attributes.ExcerptFields} for
 * additional documentation on supported formatting.
 */
public class ExcerptFields extends JexlQueryFunction {

    public ExcerptFields() {
        super(QueryFunctions.EXCERPT_FIELDS_FUNCTION, new ArrayList<>());
    }

    @Override
    public void validate() throws IllegalArgumentException {
        if (this.parameterList.isEmpty()) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS,
                            MessageFormat.format("{0} requires at least one argument", this.name));
            throw new IllegalArgumentException(qe);
        } else {
            String parameters = String.join(",", parameterList);
            try {
                datawave.query.attributes.ExcerptFields.from(parameters);
            } catch (Exception e) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS,
                                MessageFormat.format("Unable to parse excerpt fields from arguments for function {0}", this.name));
                throw new IllegalArgumentException(qe);
            }
        }
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(QueryFunctions.QUERY_FUNCTION_NAMESPACE).append(':').append(QueryFunctions.EXCERPT_FIELDS_FUNCTION);
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
        return new ExcerptFields();
    }
}
