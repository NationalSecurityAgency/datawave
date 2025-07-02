package datawave.query.language.functions.jexl;

import java.util.ArrayList;

import datawave.query.language.functions.QueryFunction;

public class GroupBySecond extends GroupByDate {

    public GroupBySecond() {
        super(GroupByDate.GROUPBY_SECOND_FUNCTION, new ArrayList<>());
    }

    @Override
    public QueryFunction duplicate() {
        return new GroupBySecond();
    }
}
