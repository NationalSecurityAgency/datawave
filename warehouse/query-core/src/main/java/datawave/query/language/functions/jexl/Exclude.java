package datawave.query.language.functions.jexl;

import datawave.query.language.functions.QueryFunction;
import datawave.query.search.WildcardFieldedFilter;

public class Exclude extends AbstractEvaluationPhaseFunction {
    public Exclude() {
        super("exclude");
    }
    
    @Override
    public String toString() {
        return super.toString("not(filter:includeRegex(", "))");
    }
    
    @Override
    public QueryFunction duplicate() {
        return new Exclude();
    }
}
