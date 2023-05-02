package datawave.query.language.functions.jexl;

import datawave.query.jexl.visitors.QueryOptionsFromQueryVisitor;
import datawave.query.language.functions.QueryFunction;

import java.util.ArrayList;

/**
 * Function to return a unique result for every day for a given list of fields. This function is equivalent to {@code #unique(field[DAY])}.
 */
public class GroupByDay extends GroupByFunction {

    public GroupByDay() {
        super(QueryOptionsFromQueryVisitor.GroupbyFunction.GROUP_BY_DAY_FUNCTION, new ArrayList<>());
    }
    
    @Override
    public QueryFunction duplicate() {
        return new GroupByDay();
    }
}
