package datawave.core.query.language.functions.jexl;

import java.util.ArrayList;

import datawave.core.query.language.functions.QueryFunction;

/**
 * Function to return a unique result for the year for a given list of fields. This function is equivalent to {@code #unique(field[YEAR])}.
 */
public class UniqueByYear extends UniqueByFunction {
    public static final String UNIQUE_BY_YEAR_FUNCTION = "unique_by_year";

    public UniqueByYear() {
        super(UNIQUE_BY_YEAR_FUNCTION, new ArrayList<>());
    }

    @Override
    public QueryFunction duplicate() {
        return new UniqueByYear();
    }
}
