package datawave.query.language.functions.jexl;

import java.util.ArrayList;

import datawave.query.language.functions.QueryFunction;

public class GroupByYear extends GroupByDate {

    public GroupByYear() {
        super(GroupByDate.GROUPBY_YEAR_FUNCTION, new ArrayList<>());
    }

    @Override
    public QueryFunction duplicate() {
        return new GroupByYear();
    }
}
