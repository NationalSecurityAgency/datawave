package datawave.query.language.functions.jexl;

import datawave.query.jexl.functions.QueryFunctions;
import datawave.query.language.functions.QueryFunction;

import java.util.ArrayList;

/**
 * Function to return a unique result for every hour of the day for a given list of fields. This function is equivalent to {@code #unique(field[DAY])}.
 */
public class UniqueByTenth extends UniqueByFunction {
    
    public UniqueByTenth() {
        super(QueryFunctions.UNIQUE_BY_TENTH_OF_HOUR_FUNCTION, new ArrayList<>());
    }
    
    @Override
    public QueryFunction duplicate() {
        return new UniqueByTenth();
    }
}
