package datawave.query.language.functions.jexl;

import datawave.query.jexl.visitors.QueryOptionsFromQueryVisitor;
import datawave.query.language.functions.QueryFunction;

import java.util.ArrayList;

/**
 * Function to return a unique result for the year for a given list of fields. This function is equivalent to {@code #unique(field[YEAR])}.
 */
public class GroupByYear extends GroupByFunction {

    public GroupByYear() {
        super(QueryOptionsFromQueryVisitor.GroupbyFunction.GROUP_BY_YEAR_FUNCTION, new ArrayList<>());
    }
    
    @Override
    public QueryFunction duplicate() {
        return new GroupByYear();
    }
}
