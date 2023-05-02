package datawave.query.language.functions.jexl;

import datawave.query.jexl.visitors.QueryOptionsFromQueryVisitor;
import datawave.query.language.functions.QueryFunction;

import java.util.ArrayList;

/**
 * Function to return a unique result for every tenth of an hour for a given list of fields. This function is equivalent to
 * {@code #unique(field[TENTH_OF_HOUR])}.
 */
public class GroupByTenthOfHour extends GroupByFunction {

    public GroupByTenthOfHour() {
        super(QueryOptionsFromQueryVisitor.GroupbyFunction.GROUP_BY_TENTH_OF_HOUR_FUNCTION, new ArrayList<>());
    }
    
    @Override
    public QueryFunction duplicate() {
        return new GroupByTenthOfHour();
    }
}
