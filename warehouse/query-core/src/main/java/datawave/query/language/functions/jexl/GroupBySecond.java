package datawave.query.language.functions.jexl;

import datawave.query.jexl.visitors.QueryOptionsFromQueryVisitor;
import datawave.query.language.functions.QueryFunction;

import java.util.ArrayList;

/**
 * Function to return a unique result for every second for a given list of fields. This function is equivalent to {@code #unique(field[SECOND])}.
 */
public class GroupBySecond extends GroupByFunction {

    public GroupBySecond() {
        super(QueryOptionsFromQueryVisitor.UniqueFunction.UNIQUE_BY_SECOND_FUNCTION, new ArrayList<>());
    }
    
    @Override
    public QueryFunction duplicate() {
        return new GroupBySecond();
    }
}
