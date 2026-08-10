package datawave.query.language.functions.jexl;

import java.text.MessageFormat;
import java.util.ArrayList;

import datawave.query.jexl.functions.QueryFunctions;
import datawave.query.language.functions.QueryFunction;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

/**
 * Function to specify when summaries should be included for results for any hit documents. This function accepts a string in the format
 * {@code size/[only]/[contentName1, contentName2, ....]}. See {@link datawave.query.attributes.SummaryOptions} for additional documentation on supported
 * formatting.
 */
public class SummaryOptions extends JexlQueryFunction {

    public SummaryOptions() {
        super(QueryFunctions.SUMMARY_FUNCTION, new ArrayList<>());
    }

    @Override
    public void validate() throws IllegalArgumentException {
        // TODO: switch to commented out code after we can take no argument
        if (this.parameterList.isEmpty()) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS,
                            MessageFormat.format("{0} requires at least one argument", this.name));
            throw new IllegalArgumentException(qe);
        } else {
            String parameters = String.join(",", parameterList);
            try {
                datawave.query.attributes.SummaryOptions.from(parameters);
            } catch (Exception e) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS,
                                MessageFormat.format("Unable to parse summary options from arguments for function {0}", this.name));
                throw new IllegalArgumentException(qe);
            }
        }

        // String parameters = this.parameterList.isEmpty() ? "" : String.join(",", parameterList);
        // try {
        // datawave.query.attributes.SummarySize.from(parameters);
        // } catch (Exception e) {
        // BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS,
        // MessageFormat.format("Unable to parse summary options from arguments for function {0}", this.name));
        // throw new IllegalArgumentException(qe);
        // }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(QueryFunctions.QUERY_FUNCTION_NAMESPACE).append(':').append(QueryFunctions.SUMMARY_FUNCTION);
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
        return new SummaryOptions();
    }
}
