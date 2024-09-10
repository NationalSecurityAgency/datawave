package datawave.query.language.functions.jexl;

import datawave.query.language.functions.QueryFunction;
import datawave.query.search.WildcardFieldedFilter;

public class Exclude extends AbstractEvaluationPhaseFunction {
    public Exclude() {
        super("exclude");
    }

    @Override
    public String toString() {
        // since the negation is being distributed, we need to reverse the operation.
        String operation = (this.type.equals(WildcardFieldedFilter.BooleanType.AND)) ? " || " : " && ";
        return super.toString("not(filter:includeRegex(", "))", operation);
    }

    @Override
    public QueryFunction duplicate() {
        return new Exclude();
    }
}
