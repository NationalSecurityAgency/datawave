package datawave.core.query.language.functions.jexl;

import java.text.MessageFormat;
import java.util.ArrayList;

import datawave.core.query.language.functions.QueryFunction;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

public class Rename extends JexlQueryFunction {
    public static final String RENAME_FUNCTION = "rename";

    public Rename() {
        super(RENAME_FUNCTION, new ArrayList<>());
    }

    @Override
    public void validate() throws IllegalArgumentException {
        if (this.parameterList.isEmpty()) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, MessageFormat.format("{0}", this.name));
            throw new IllegalArgumentException(qe);
        }
    }

    @Override
    public QueryFunction duplicate() {
        return new Rename();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(QUERY_FUNCTION_NAMESPACE).append(':').append(RENAME_FUNCTION);
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
}
