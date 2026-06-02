package datawave.query.language.functions.jexl;

import java.util.ArrayList;

import datawave.query.language.functions.QueryFunction;

public class GroupByMinute extends GroupByDate {

    public GroupByMinute() {
        super(GroupByDate.GROUPBY_MINUTE_FUNCTION, new ArrayList<>());
    }

    @Override
    public QueryFunction duplicate() {
        return new GroupByMinute();
    }
}
