package datawave.query.language.functions.jexl;

import java.util.ArrayList;

import datawave.query.jexl.visitors.QueryOptionsFromQueryVisitor;
import datawave.query.language.functions.QueryFunction;

/**
 * Function to return a unique result for the year for a given list of fields. This function is equivalent to {@code #unique(field[YEAR])}.
 */
public class UniqueByYear extends UniqueByFunction {

    public UniqueByYear() {
        super(QueryOptionsFromQueryVisitor.UniqueFunction.UNIQUE_BY_YEAR_FUNCTION, new ArrayList<>());
    }

    @Override
    public QueryFunction duplicate() {
        return new UniqueByYear();
    }
}
