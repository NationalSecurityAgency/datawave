package datawave.core.query.language.functions.jexl;

import java.text.MessageFormat;
import java.util.ArrayList;

import datawave.core.query.language.functions.QueryFunction;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

public class Options extends JexlQueryFunction {
    public static final String OPTIONS_FUNCTION = "options";

    public Options() {
        super(OPTIONS_FUNCTION, new ArrayList<>());
    }

    /**
     * query options are pairs of key/value. Ensure that the number of args is even
     *
     * @throws IllegalArgumentException
     *             for illegal arguments
     */
    @Override
    public void validate() throws IllegalArgumentException {
        if (this.parameterList.size() % 2 != 0) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, MessageFormat.format("{0}", this.name));
            throw new IllegalArgumentException(qe);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(QUERY_FUNCTION_NAMESPACE).append(':').append(OPTIONS_FUNCTION);
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
        return new Options();
    }
}
