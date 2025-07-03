package datawave.query.language.functions.jexl;

import java.util.ArrayList;

import datawave.query.language.functions.QueryFunction;

public class GroupByHour extends GroupByDate {

    public GroupByHour() {
        super(GroupByDate.GROUPBY_HOUR_FUNCTION, new ArrayList<>());
    }

    @Override
    public QueryFunction duplicate() {
        return new GroupByHour();
    }
}
