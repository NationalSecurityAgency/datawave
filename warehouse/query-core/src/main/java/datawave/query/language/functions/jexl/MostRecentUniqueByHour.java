package datawave.query.language.functions.jexl;

import java.util.ArrayList;

import datawave.query.jexl.functions.QueryFunctions;
import datawave.query.jexl.visitors.QueryOptionsFromQueryVisitor;
import datawave.query.language.functions.QueryFunction;

/**
 * Function to return a unique result for every hour of the day for a given list of fields. This function is equivalent to
 * {@code #MOST_RECENT_UNIQUE(field[HOUR])}.
 */
public class MostRecentUniqueByHour extends UniqueByFunction {

    public MostRecentUniqueByHour() {
        super(QueryFunctions.MOST_RECENT_PREFIX + QueryOptionsFromQueryVisitor.UniqueFunction.UNIQUE_BY_HOUR_FUNCTION, new ArrayList<>());
    }

    @Override
    public QueryFunction duplicate() {
        return new MostRecentUniqueByHour();
    }
}
