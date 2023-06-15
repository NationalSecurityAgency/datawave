package datawave.query.language.functions.jexl;

import datawave.query.jexl.functions.QueryFunctions;
import datawave.query.jexl.visitors.QueryOptionsFromQueryVisitor;
import datawave.query.language.functions.QueryFunction;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

import java.text.MessageFormat;
import java.util.ArrayList;

/**
 * Function to return a unique result for every day for a given list of fields. This function is equivalent to {@code #unique(field[DAY])}.
 */
public class UniqueByDay extends UniqueByFunction {

    public UniqueByDay() {
        super(QueryOptionsFromQueryVisitor.UniqueFunction.UNIQUE_BY_DAY_FUNCTION, new ArrayList<>());
    }

    @Override
    public QueryFunction duplicate() {
        return new UniqueByDay();
    }
}
