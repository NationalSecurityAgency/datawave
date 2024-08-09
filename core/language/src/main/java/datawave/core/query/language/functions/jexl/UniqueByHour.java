package datawave.core.query.language.functions.jexl;

import java.util.ArrayList;

import datawave.core.query.language.functions.QueryFunction;

/**
 * Function to return a unique result for every hour of the day for a given list of fields. This function is equivalent to {@code #unique(field[DAY])}.
 */
public class UniqueByHour extends UniqueByFunction {
    public static final String UNIQUE_BY_HOUR_FUNCTION = "unique_by_hour";

    public UniqueByHour() {
        super(UNIQUE_BY_HOUR_FUNCTION, new ArrayList<>());
    }

    @Override
    public QueryFunction duplicate() {
        return new UniqueByHour();
    }
}
