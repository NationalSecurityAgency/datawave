package datawave.query.language.functions.jexl;

import java.util.ArrayList;

import datawave.query.jexl.visitors.QueryOptionsFromQueryVisitor;
import datawave.query.language.functions.QueryFunction;

/**
 * Function to return a unique result for every month of the year for a given list of fields. This function is equivalent to {@code #unique(field[MONTH])}.
 */
public class UniqueByMonth extends UniqueByFunction {

    public UniqueByMonth() {
        super(QueryOptionsFromQueryVisitor.UniqueFunction.UNIQUE_BY_MONTH_FUNCTION, new ArrayList<>());
    }

    @Override
    public QueryFunction duplicate() {
        return new UniqueByMonth();
    }
}
