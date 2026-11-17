package datawave.query.discovery;

import java.util.function.UnaryOperator;

/**
 * Functional
 *
 * Converts DiscoveredThing to one containing only the original "Term" and "Column Visibility." Other fields are defined the DiscoveredThing constructor for
 * this scenario. The scenario here is a DiscoveredThing is created containing only "Term" and "Column Visibility."
 */
public class DiscoveredThingValuesOnlyConditionalTransformer implements UnaryOperator<DiscoveredThing> {

    private final boolean valuesOnly;

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
