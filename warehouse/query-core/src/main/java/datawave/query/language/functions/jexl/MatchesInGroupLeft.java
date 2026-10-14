package datawave.query.language.functions.jexl;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import datawave.query.language.functions.QueryFunction;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

/**
 * <pre>
 * Function to test whether key/value pairs match within the part of a tree (left side) formed by the field name structure that is
 * dot-delimited: NAME.FOO.BAR.BAZ
 *
 * position args are the indexed of the grouping name starting from the left to include when determining a group
 * for this field name:   NAME.grandparent_0.parent_0.child_0
 *
 * '0' means take the leftmost group after the first '.' (in other words 'NAME.grandparent_0')
 * '1' means take the up through the second group '.' (i.e. 'NAME.grandparent_0.parent_0'
 *
 *  If there is no position arg supplied, '0' is assumed.
 *
 *         "NAME.grandparent_0.parent_0.child_1,FREDO,fredo"    ==  "fredo",
 *         "NAME.grandparent_0.parent_1.child_0,SANTINO,santino" ==  "santino");
 *         (implied 0 for the position arg) means that fredo and santino have the same
 *         field name left-side: 'NAME.grandparent_0'  (they have the same grandparents so they are related)
 *
 *         "NAME.grandparent_0.parent_0.child_1,FREDO,fredo" == "fredo",
 *         "NAME.grandparent_0.parent_1.child_0,SANTINO,santino" == "santino", 1);
 *         with '1' for the position ard, function is false fredo and santino do not have the same
 *         field name left-side: 'NAME.grandparent_0.parent_X' (they have the same grandparents so they are 1st cousins)
 *
 * Supplying -1 as the position will mean to match all grouping fields must match, regardless of how many there are
 * </pre>
 */
public class MatchesInGroupLeft extends JexlQueryFunction {

    public MatchesInGroupLeft() {
        super("matches_in_group_left", new ArrayList<>());
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
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.INVALID_FUNCTION_ARGUMENTS,
                                MessageFormat.format("{0}", ex, this.name));
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
