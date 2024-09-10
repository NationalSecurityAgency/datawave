package datawave.query.language.functions.jexl;

import datawave.query.language.functions.QueryFunction;
import datawave.query.search.WildcardFieldedFilter;

public class Include extends AbstractEvaluationPhaseFunction {
    public Include() {
        super("include");
    }

    @Override
    public String toString() {
        String operation = (this.type.equals(WildcardFieldedFilter.BooleanType.AND)) ? " && " : " || ";
        return super.toString("filter:includeRegex(", ")", operation);
    }

    @Override
    public QueryFunction duplicate() {
        return new Include();
    }
}
