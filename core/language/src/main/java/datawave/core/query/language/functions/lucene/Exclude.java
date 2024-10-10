package datawave.core.query.language.functions.lucene;

import datawave.core.query.language.functions.QueryFunction;

@Deprecated
public class Exclude extends AbstractEvaluationPhaseFunction {
    public Exclude() {
        super("exclude", false);
    }

    @Override
    public QueryFunction duplicate() {
        return new Exclude();
    }
}
