package datawave.core.query.language.functions.jexl;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import datawave.core.query.language.functions.QueryFunction;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

/**
 * This function accepts a comma separated list of fields to be excluded from QueryModel expansion. The purpose is to provide users with an easy way to avoid
 * undesirable query model expansions.
 *
 * Note: The exclude is only applied to the fields in the original query. An original field can be expanded into an excluded field.
 */
public class NoExpansion extends JexlQueryFunction {

    public NoExpansion() {
        super("noExpansion", new ArrayList<>());
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
        return new NoExpansion();
    }

    @Override
    public String toString() {
        List<String> params = getParameterList();
        return "f:noExpansion(" + String.join("", params) + ")";
    }
}
