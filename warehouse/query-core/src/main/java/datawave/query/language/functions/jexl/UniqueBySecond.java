package datawave.query.language.functions.jexl;

import datawave.query.jexl.functions.QueryFunctions;
import datawave.query.language.functions.QueryFunction;

import java.util.ArrayList;

/**
 * Function to return a unique result for every month of the year for a given list of fields. This function is equivalent to {@code #unique(field[MONTH])}.
 */
public class UniqueBySecond extends UniqueByFunction {
    
    public UniqueBySecond() {
        super(QueryFunctions.UNIQUE_BY_SECOND_FUNCTION, new ArrayList<>());
    }
    
    @Override
    public QueryFunction duplicate() {
        return new UniqueBySecond();
    }
}
