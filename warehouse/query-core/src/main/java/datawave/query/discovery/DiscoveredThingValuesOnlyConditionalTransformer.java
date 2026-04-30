package datawave.query.discovery;

import java.util.function.UnaryOperator;

import org.apache.hadoop.io.MapWritable;

public class DiscoveredThingValuesOnlyConditionalTransformer implements UnaryOperator<DiscoveredThing> {

    boolean valuesOnly = false;

    DiscoveredThingValuesOnlyConditionalTransformer(boolean valuesOnly) {
        this.valuesOnly = valuesOnly;
    }

    public DiscoveredThing apply(DiscoveredThing dt) {
        // @formatter:off
        return (valuesOnly) ? new DiscoveredThing(dt.getTerm(),
                "",
                "",
                "",
                dt.getColumnVisibility(),
                0L,
                new MapWritable())
                : dt;
        // @formatter:on
    }
}
