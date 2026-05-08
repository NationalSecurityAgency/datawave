package datawave.query.discovery;

import java.util.function.UnaryOperator;

public class DiscoveredThingValuesOnlyConditionalTransformer implements UnaryOperator<DiscoveredThing> {

    boolean valuesOnly;

    DiscoveredThingValuesOnlyConditionalTransformer(boolean valuesOnly) {
        this.valuesOnly = valuesOnly;
    }

    public DiscoveredThing apply(DiscoveredThing dt) {
        if (valuesOnly) {
            return new DiscoveredThing(dt.getTerm(), dt.getColumnVisibility());
        }
        return dt;
    }
}
