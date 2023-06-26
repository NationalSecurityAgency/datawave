package datawave.query.language.functions.jexl;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import datawave.query.language.functions.QueryFunction;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

/**
 * <pre>
 * first arg is a number, next arg is a field name, following args are possible values for the field.
 * MatchesAtLeastCountOf(3, STOOGE, 'Moe', 'Larry', 'Joe', 'Shemp', 'Curley Joe') will return true
 * if at least 3 of the listed names are present for STOOGE
 * </pre>
 */
public class MatchesAtLeastCountOf extends JexlQueryFunction {

    public MatchesAtLeastCountOf() {
        super("matches_at_least_count_of", new ArrayList<>());
    }

    @Override
    public void validate() throws IllegalArgumentException {
        if (this.parameterList.size() < 3) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, MessageFormat.format("{0}", this.name));
            throw new IllegalArgumentException(qe);
        }
        String shouldBeANumber = parameterList.get(0); // get the first arg
        try {
            Integer.parseInt(shouldBeANumber);
        } catch (Exception ex) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS,
                            MessageFormat.format("{0}", ex, this.name));
            throw new IllegalArgumentException(qe);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        List<String> params = getParameterList();
        if (!params.isEmpty()) {
            sb.append(params.get(0)); // the count
        }
        if (params.size() > 1) {
            sb.append(", ");
            sb.append(params.get(1)); // the field name
        }
        for (int i = 2; i < params.size(); i++) {
            if (sb.length() > 0)
                sb.append(", ");
            sb.append(this.escapeString(params.get(i)));
        }
        sb.insert(0, "filter:matchesAtLeastCountOf(");
        sb.append(")");
        return sb.toString();
    }

    @Override
    public QueryFunction duplicate() {
        return new MatchesAtLeastCountOf();
    }

}
