package datawave.query.language.functions.jexl;

import datawave.query.language.functions.QueryFunction;
import datawave.query.search.WildcardFieldedFilter;

public class Matches extends AbstractEvaluationPhaseFunction {
    public Matches() {
        super("matches");
    }
    
    @Override
    public String toString() {
        String operation = (this.type.equals(WildcardFieldedFilter.BooleanType.AND)) ? " && " : " || ";
        return super.toString("f:matchesRegex(", ")", operation);
    }
    
    @Override
    public QueryFunction duplicate() {
        return new Matches();
    }
}
