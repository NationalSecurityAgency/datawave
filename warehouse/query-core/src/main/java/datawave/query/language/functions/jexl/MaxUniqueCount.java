package datawave.query.language.functions.jexl;

import java.text.MessageFormat;
import java.util.ArrayList;

import datawave.query.Constants;
import datawave.query.jexl.functions.QueryFunctions;
import datawave.query.language.functions.QueryFunction;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

public class MaxUniqueCount extends JexlQueryFunction {

    public MaxUniqueCount() {
        super(QueryFunctions.MAX_UNIQUE_COUNT, new ArrayList<>());
    }

    @Override
    public void validate() throws IllegalArgumentException {
        if (parameterList.size() == 1) {
            try {
                int value = Integer.parseInt(parameterList.get(0));
                if (value < 1) {
                    throw new IllegalArgumentException(new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS,
                                    MessageFormat.format("{0} requires an integer argument greater than 0.", this.name)));
                }
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, e,
                                MessageFormat.format("Failed to parse argument in {0} to an integer.", this)));
            }
        } else {
            throw new IllegalArgumentException(new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS,
                            MessageFormat.format("{0} requires a single integer argument greater than 0.", this.name)));
        }
    }

    @Override
    public QueryFunction duplicate() {
        return new MaxUniqueCount();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(QueryFunctions.QUERY_FUNCTION_NAMESPACE).append(Constants.COLON).append(QueryFunctions.MAX_UNIQUE_COUNT);
        String separator = Constants.LEFT_PAREN;
        for (String param : parameterList) {
            sb.append(separator).append(param);
            separator = Constants.COMMA;
        }
        sb.append(Constants.RIGHT_PAREN);
        return sb.toString();
    }
}
