package datawave.core.query.language.functions.jexl;

import java.text.MessageFormat;
import java.util.ArrayList;

import datawave.core.query.language.functions.QueryFunction;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

public class IsNotNull extends JexlQueryFunction {
    public IsNotNull() {
        super("isnotnull", new ArrayList<>());
    }

    @Override
    public void validate() throws IllegalArgumentException {
        if (this.parameterList.size() != 1) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, MessageFormat.format("{0}", this.name));
            throw new IllegalArgumentException(qe);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        String field = parameterList.get(0);

        sb.append("filter:isNotNull(").append(field).append(")");

        return sb.toString();
    }

    @Override
    public QueryFunction duplicate() {
        return new IsNotNull();
    }
}
