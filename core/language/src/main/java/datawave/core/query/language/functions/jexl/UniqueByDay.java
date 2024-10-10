package datawave.core.query.language.functions.jexl;

import java.util.ArrayList;

import datawave.core.query.language.functions.QueryFunction;

/**
 * Function to return a unique result for every day for a given list of fields. This function is equivalent to {@code #unique(field[DAY])}.
 */
public class UniqueByDay extends UniqueByFunction {
    public static final String UNIQUE_BY_DAY_FUNCTION = "unique_by_day";

    public UniqueByDay() {
        super(UNIQUE_BY_DAY_FUNCTION, new ArrayList<>());
    }

    @Override
    public QueryFunction duplicate() {
        return new UniqueByDay();
    }
}
