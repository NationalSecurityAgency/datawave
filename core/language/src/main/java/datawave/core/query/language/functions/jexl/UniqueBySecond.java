package datawave.core.query.language.functions.jexl;

import java.util.ArrayList;

import datawave.core.query.language.functions.QueryFunction;

/**
 * Function to return a unique result for every second for a given list of fields. This function is equivalent to {@code #unique(field[SECOND])}.
 */
public class UniqueBySecond extends UniqueByFunction {
    public static final String UNIQUE_BY_SECOND_FUNCTION = "unique_by_second";

    public UniqueBySecond() {
        super(UNIQUE_BY_SECOND_FUNCTION, new ArrayList<>());
    }

    @Override
    public QueryFunction duplicate() {
        return new UniqueBySecond();
    }
}
