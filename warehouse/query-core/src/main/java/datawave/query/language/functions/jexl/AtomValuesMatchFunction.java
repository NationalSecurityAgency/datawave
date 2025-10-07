package datawave.query.language.functions.jexl;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import datawave.query.language.functions.QueryFunction;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

/**
 * Tests that for the field name arguments that are supplied, there are values that match that are also within the same grouping context
 */
public class AtomValuesMatchFunction extends JexlQueryFunction {

    public AtomValuesMatchFunction() {
        super("atom_values_match", new ArrayList<>());
    }

    @Override
    public void validate() throws IllegalArgumentException {
        if (this.parameterList.size() < 2) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, MessageFormat.format("{0}", this.name));
            throw new IllegalArgumentException(qe);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        List<String> params = getParameterList();
        for (int i = 0; i < params.size(); i++) {
            if (sb.length() > 0)
                sb.append(", ");
            sb.append(params.get(i));
        }
        sb.insert(0, "grouping:atomValuesMatch(");
        sb.append(")");
        return sb.toString();
    }

    @Override
    public QueryFunction duplicate() {
        return new AtomValuesMatchFunction();
    }

}
