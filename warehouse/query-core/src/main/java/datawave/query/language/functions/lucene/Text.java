package datawave.query.language.functions.lucene;

import datawave.query.language.functions.QueryFunction;

public class Text extends AbstractEvaluationPhaseFunction {
    public Text() {
        super("text", true);
    }
    
    @Override
    public QueryFunction duplicate() {
        return new Text();
    }
}
