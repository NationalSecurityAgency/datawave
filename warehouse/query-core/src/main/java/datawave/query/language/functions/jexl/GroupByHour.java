package datawave.query.language.functions.jexl;

import datawave.query.jexl.visitors.QueryOptionsFromQueryVisitor;
import datawave.query.language.functions.QueryFunction;

import java.util.ArrayList;

/**
 * Function to return a unique result for every hour of the day for a given list of fields. This function is equivalent to {@code #unique(field[DAY])}.
 */
public class GroupByHour extends GroupByFunction {

    public GroupByHour() {
        super(QueryOptionsFromQueryVisitor.GroupbyFunction.GROUP_BY_HOUR_FUNCTION, new ArrayList<>());
    }
    
    @Override
    public QueryFunction duplicate() {
        return new GroupByHour();
    }
}
