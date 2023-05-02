package datawave.query.language.functions.jexl;

import datawave.query.jexl.functions.QueryFunctions;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

import java.text.MessageFormat;
import java.util.List;

/**
 * Abstract class that contains common functionality for unique-by functions.
 */
public abstract class GroupByFunction extends JexlQueryFunction {

    public GroupByFunction(String functionName, List<String> parameterList) {
        super(functionName, parameterList);
    }
    
    /**
     * This function must be given at least one field parameter, and may only be given field names, and not additional granularity levels.
     *
     * @throws IllegalArgumentException
     *             if at least one parameter was not given or if additional granularity levels were specified
     */
    @Override
    public void validate() throws IllegalArgumentException {
        if (this.parameterList.isEmpty()) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, MessageFormat.format("{0}", this.name));
            throw new IllegalArgumentException(qe);
        }
        // Verify the advanced unique syntax, e.g. field[DAY,HOUR], is not used.
        for (String param : this.parameterList) {
            if (param.contains("[")) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, MessageFormat.format(
                                "{0} does not support the advanced unique syntax, only a simple comma-delimited list of fields is allowed.", this.name));
                throw new IllegalArgumentException(qe);
            }
        }
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        
        sb.append(QueryFunctions.QUERY_FUNCTION_NAMESPACE).append(':').append(this.name);
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
