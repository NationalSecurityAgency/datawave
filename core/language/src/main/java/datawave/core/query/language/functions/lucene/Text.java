package datawave.core.query.language.functions.lucene;

import datawave.core.query.language.functions.QueryFunction;

@Deprecated
public class Text extends AbstractEvaluationPhaseFunction {
    public Text() {
        super("text", true);
    }

    @Override
    public QueryFunction duplicate() {
        return new Text();
    }
}
