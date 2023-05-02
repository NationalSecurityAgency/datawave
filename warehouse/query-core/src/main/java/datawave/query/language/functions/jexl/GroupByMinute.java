package datawave.query.language.functions.jexl;

import datawave.query.jexl.visitors.QueryOptionsFromQueryVisitor;
import datawave.query.language.functions.QueryFunction;

import java.util.ArrayList;

/**
 * Function to return a unique result for every minute of the hour for a given list of fields. This function is equivalent to {@code #unique(field[MINUTE])}.
 */
public class GroupByMinute extends GroupByFunction {

    public GroupByMinute() {
        super(QueryOptionsFromQueryVisitor.GroupbyFunction.GROUP_BY_MINUTE_FUNCTION, new ArrayList<>());
    }
    
    @Override
    public QueryFunction duplicate() {
        return new GroupByMinute();
    }
}
