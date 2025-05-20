package datawave.query.language.functions.jexl;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import datawave.query.language.functions.QueryFunction;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

/**
 * This function accepts a comma separated list of fields to be marked as lenient. The purpose is to provide users with an easy way to allow queries to when
 * model expansions are combined with normalization issues.
 */
public class Lenient extends JexlQueryFunction {

    public Lenient() {
        super("lenient", new ArrayList<>());
    }

    @Override
    public void validate() throws IllegalArgumentException {
        if (this.parameterList.size() != 1) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, MessageFormat.format("{0}", this.name));
            throw new IllegalArgumentException(qe);
        }
    }

    @Override
    public QueryFunction duplicate() {
        return new Lenient();
    }

    @Override
    public String toString() {
        List<String> params = getParameterList();
        return "f:lenient(" + String.join("", params) + ")";
    }
}
