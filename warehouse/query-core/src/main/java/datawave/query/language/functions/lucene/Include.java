package datawave.query.language.functions.lucene;

import datawave.query.jexl.functions.QueryFunctions;
import datawave.query.language.functions.QueryFunction;

public class Include extends AbstractEvaluationPhaseFunction {
    public Include() {
        super(QueryFunctions.INCLUDE_FUNCTION, true);
    }
    
    @Override
    public QueryFunction duplicate() {
        return new Include();
    }
}
