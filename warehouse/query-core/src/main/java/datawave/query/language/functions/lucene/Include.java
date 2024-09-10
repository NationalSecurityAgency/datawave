package datawave.query.language.functions.lucene;

import datawave.query.language.functions.QueryFunction;

@Deprecated
public class Include extends AbstractEvaluationPhaseFunction {
    public Include() {
        super("include", true);
    }

    @Override
    public QueryFunction duplicate() {
        return new Include();
    }
}
