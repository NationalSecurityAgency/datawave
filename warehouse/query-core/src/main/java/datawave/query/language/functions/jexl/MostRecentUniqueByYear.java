package datawave.query.language.functions.jexl;

import java.util.ArrayList;

import datawave.query.jexl.functions.QueryFunctions;
import datawave.query.jexl.visitors.QueryOptionsFromQueryVisitor;
import datawave.query.language.functions.QueryFunction;

/**
 * Function to return a most recent unique result for the year for a given list of fields. This function is equivalent to
 * {@code #MOST_RECENT_UNIQUE(field[YEAR])}.
 */
public class MostRecentUniqueByYear extends UniqueByFunction {

    public MostRecentUniqueByYear() {
        super(QueryFunctions.MOST_RECENT_PREFIX + QueryOptionsFromQueryVisitor.UniqueFunction.UNIQUE_BY_YEAR_FUNCTION, new ArrayList<>());
    }

    @Override
    public QueryFunction duplicate() {
        return new MostRecentUniqueByYear();
    }
}
