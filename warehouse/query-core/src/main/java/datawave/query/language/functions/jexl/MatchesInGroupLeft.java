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
public class MatchesInGroupLeft extends JexlQueryFunction {
    
    public MatchesInGroupLeft() {
        super("matches_in_group_left", new ArrayList<String>());
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
        for (int i = 0; i < params.size(); i += 2) {
            if (sb.length() > 0)
                sb.append(", ");
            sb.append(params.get(i));
            if (i + 1 == params.size())
                break; // I just added the 'offset' and there is no regex to follow
            sb.append(", ");
            sb.append(this.escapeString(params.get(i + 1)));
        }
        sb.insert(0, "grouping:matchesInGroupLeft(");
        sb.append(")");
        return sb.toString();
    }
    
    @Override
    public QueryFunction duplicate() {
        return new MatchesInGroupLeft();
    }
    
}
