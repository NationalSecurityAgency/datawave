package datawave.query.language.functions.jexl;

import datawave.query.language.functions.QueryFunction;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

public class MatchesInGroupLeftRange extends JexlQueryFunction {

    public MatchesInGroupLeftRange() {
        super("matches_in_group_left_range", new ArrayList<>());
    }

    @Override
    public void validate() throws IllegalArgumentException {
        if (this.parameterList.size() < 3) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS, MessageFormat.format("{0}", this.name));
            throw new IllegalArgumentException(qe);
        }
        if (this.parameterList.size() % 3 != 0) { // odd number of args
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
        for (int i = 0; i < params.size(); i += 3) {
            if (sb.length() > 0)
                sb.append(", ");
            sb.append(params.get(i));
            if (i + 2 == params.size())
                break; // I just added the 'offset' and there is no regex to follow
            sb.append(", ");
            sb.append(this.escapeString(params.get(i + 1)));
            sb.append(", ");
            sb.append(this.escapeString(params.get(i + 2)));
        }
        sb.insert(0, "grouping:matchesInGroupLeftRange(");
        sb.append(")");
        return sb.toString();
    }

    @Override
    public QueryFunction duplicate() {
        return new MatchesInGroupLeftRange();
    }

}
