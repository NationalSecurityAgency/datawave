package datawave.query.language.functions.jexl;

import datawave.query.language.functions.QueryFunction;

public class Text extends AbstractEvaluationPhaseFunction {
    public Text() {
        super("text");
    }
    
    @Override
    public String toString() {
        return super.toString("filter:includeText(", ")");
    }
    
    @Override
    public QueryFunction duplicate() {
        return new Text();
    }
    
}
