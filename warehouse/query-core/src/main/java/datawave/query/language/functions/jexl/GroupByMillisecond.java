package datawave.query.language.functions.jexl;

import java.util.ArrayList;

import datawave.query.language.functions.QueryFunction;

public class GroupByMillisecond extends GroupByDate {

    public GroupByMillisecond() {
        super(GroupByDate.GROUPBY_MILLISECOND_FUNCTION, new ArrayList<>());
    }

    @Override
    public QueryFunction duplicate() {
        return new GroupByMillisecond();
    }
}
