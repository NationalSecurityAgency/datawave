package datawave.query.language.functions.jexl;

import java.util.ArrayList;

import datawave.query.jexl.visitors.QueryOptionsFromQueryVisitor;
import datawave.query.language.functions.QueryFunction;

/**
 * Function to return a unique result for every tenth of an hour for a given list of fields. This function is equivalent to
 * {@code #unique(field[TENTH_OF_HOUR])}.
 */
public class UniqueByTenthOfHour extends UniqueByFunction {

    public UniqueByTenthOfHour() {
        super(QueryOptionsFromQueryVisitor.UniqueFunction.UNIQUE_BY_TENTH_OF_HOUR_FUNCTION, new ArrayList<>());
    }

    @Override
    public QueryFunction duplicate() {
        return new UniqueByTenthOfHour();
    }
}
