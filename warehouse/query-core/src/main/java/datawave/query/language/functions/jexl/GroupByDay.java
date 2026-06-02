package datawave.query.language.functions.jexl;

import java.util.ArrayList;

import datawave.query.language.functions.QueryFunction;

public class GroupByDay extends GroupByDate {

    public GroupByDay() {
        super(GroupByDate.GROUPBY_DAY_FUNCTION, new ArrayList<>());
    }

    @Override
    public QueryFunction duplicate() {
        return new GroupByDay();
    }
}
