package datawave.query.language.functions.jexl;

import java.util.ArrayList;

import datawave.query.language.functions.QueryFunction;

public class GroupByMonth extends GroupByDate {

    public GroupByMonth() {
        super(GroupByDate.GROUPBY_MONTH_FUNCTION, new ArrayList<>());
    }

    @Override
    public QueryFunction duplicate() {
        return new GroupByMonth();
    }
}
