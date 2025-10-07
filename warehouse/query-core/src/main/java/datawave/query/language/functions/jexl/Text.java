package datawave.query.language.functions.jexl;

import datawave.query.language.functions.QueryFunction;
import datawave.query.search.WildcardFieldedFilter;

public class Text extends AbstractEvaluationPhaseFunction {
    public Text() {
        super("text");
    }

    @Override
    public String toString() {
        String operation = (this.type.equals(WildcardFieldedFilter.BooleanType.AND)) ? " && " : " || ";
        return super.toString("f:includeText(", ")", operation);
    }

    @Override
    public QueryFunction duplicate() {
        return new Text();
    }

}
