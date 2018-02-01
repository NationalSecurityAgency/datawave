package datawave.query.language.functions.jexl;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import datawave.query.language.functions.QueryFunction;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

/**
 * function to test whether two key/value pairs match within datafields of the same (grouping context) group For example FROM_ADDRESS.1 == 1.2.3.4 and
 * DIRECTION.1 == 1
 */
public class MatchesAtLeastCountOf extends JexlQueryFunction {
    
    public MatchesAtLeastCountOf() {
        super("matches_at_least_count_of", new ArrayList<String>());
    }
    
    @Override
    public void validate() throws IllegalArgumentException {
        if (this.parameterList.size() < 2) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, MessageFormat.format("{0}", this.name));
            throw new IllegalArgumentException(qe);
        }
        if (this.parameterList.size() % 2 != 0) { // odd number of args
            String shouldBeANumber = parameterList.get(this.parameterList.size() - 1); // get the last arg
            try {
                Integer.parseInt(shouldBeANumber);
            } catch (Exception ex) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, MessageFormat.format("{0}", ex,
                                this.name));
                throw new IllegalArgumentException(qe);
            }
        }
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        List<String> params = getParameterList();
        if (params.size() > 0) {
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
