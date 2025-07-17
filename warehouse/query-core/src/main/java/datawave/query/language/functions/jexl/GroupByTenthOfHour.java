package datawave.query.language.functions.jexl;

import java.util.ArrayList;

import datawave.query.language.functions.QueryFunction;

public class GroupByTenthOfHour extends GroupByDate {

    public GroupByTenthOfHour() {
        super(GroupByDate.GROUPBY_TENTH_OF_HOUR_FUNCTION, new ArrayList<>());
    }

    @Override
    public QueryFunction duplicate() {
        return new GroupByTenthOfHour();
    }
}
