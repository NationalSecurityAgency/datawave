package datawave.query.language.functions.jexl;

import java.util.ArrayList;

import datawave.query.jexl.visitors.QueryOptionsFromQueryVisitor;
import datawave.query.language.functions.QueryFunction;

/**
 * Function to return a unique result for every second for a given list of fields. This function is equivalent to {@code #unique(field[SECOND])}.
 */
public class UniqueBySecond extends UniqueByFunction {

    public UniqueBySecond() {
        super(QueryOptionsFromQueryVisitor.UniqueFunction.UNIQUE_BY_SECOND_FUNCTION, new ArrayList<>());
    }

    @Override
    public QueryFunction duplicate() {
        return new UniqueBySecond();
    }
}
