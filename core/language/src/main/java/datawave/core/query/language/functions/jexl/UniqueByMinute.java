package datawave.core.query.language.functions.jexl;

import java.util.ArrayList;

import datawave.core.query.language.functions.QueryFunction;

/**
 * Function to return a unique result for every minute of the hour for a given list of fields. This function is equivalent to {@code #unique(field[MINUTE])}.
 */
public class UniqueByMinute extends UniqueByFunction {
    public static final String UNIQUE_BY_MINUTE_FUNCTION = "unique_by_minute";

    public UniqueByMinute() {
        super(UNIQUE_BY_MINUTE_FUNCTION, new ArrayList<>());
    }

    @Override
    public QueryFunction duplicate() {
        return new UniqueByMinute();
    }
}
