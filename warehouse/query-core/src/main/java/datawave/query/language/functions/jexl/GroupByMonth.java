package datawave.query.language.functions.jexl;

import datawave.query.jexl.visitors.QueryOptionsFromQueryVisitor;
import datawave.query.language.functions.QueryFunction;

import java.util.ArrayList;

/**
 * Function to return a unique result for every month of the year for a given list of fields. This function is equivalent to {@code #unique(field[MONTH])}.
 */
public class GroupByMonth extends GroupByFunction {

    public GroupByMonth() {
        super(QueryOptionsFromQueryVisitor.GroupbyFunction.GROUP_BY_MONTH_FUNCTION, new ArrayList<>());
    }
    
    @Override
    public QueryFunction duplicate() {
        return new GroupByMonth();
    }
}
