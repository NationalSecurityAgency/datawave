package datawave.query.language.functions.lucene;

import datawave.query.jexl.functions.QueryFunctions;
import datawave.query.language.functions.QueryFunction;

public class Exclude extends AbstractEvaluationPhaseFunction {
    public Exclude() {
        super(QueryFunctions.EXCLUDE_FUNCTION, false);
    }
    
    @Override
    public QueryFunction duplicate() {
        return new Exclude();
    }
}
