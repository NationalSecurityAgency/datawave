package datawave.query.language.functions.jexl;

import java.util.ArrayList;

import datawave.query.jexl.visitors.QueryOptionsFromQueryVisitor;
import datawave.query.language.functions.QueryFunction;

/**
 * Function to return a unique result for every day for a given list of fields. This function is equivalent to {@code #unique(field[DAY])}.
 */
public class UniqueByDay extends UniqueByFunction {

    public UniqueByDay() {
        super(QueryOptionsFromQueryVisitor.UniqueFunction.UNIQUE_BY_DAY_FUNCTION, new ArrayList<>());
    }

    @Override
    public QueryFunction duplicate() {
        return new UniqueByDay();
    }
}
