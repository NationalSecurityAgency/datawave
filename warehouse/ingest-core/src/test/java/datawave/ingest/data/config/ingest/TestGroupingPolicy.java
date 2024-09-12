package datawave.ingest.data.config.ingest;

import java.util.Collections;
import java.util.Map;

import com.google.common.collect.Multimap;

import datawave.ingest.data.config.NormalizedContentInterface;

public class TestGroupingPolicy extends GroupingPolicy {
    private final static int RETURN_NULL = 4;

    public TestGroupingPolicy() {
        super();
    }

    public TestGroupingPolicy(String name) {
        super(name);
    }

    @Override
    protected void setPolicy(String name) {
        if (name.equals("RETURN_NULL")) {
            policy = RETURN_NULL;
        } else {
            super.setPolicy(name);
        }
    }

    @Override
    public Iterable<NormalizedContentInterface> applyPolicy(VirtualIngest.VirtualFieldNormalizer normalizer, String field,
                    VirtualIngest.VirtualFieldNormalizer.VirtualFieldGrouping grouping, Multimap<String,NormalizedContentInterface> eventFields,
                    Map<String,Multimap<VirtualIngest.VirtualFieldNormalizer.VirtualFieldGrouping,NormalizedContentInterface>> groupings) {

        switch (policy) {
            case RETURN_NULL:
                return Collections::emptyIterator;
            default:
                return super.applyPolicy(normalizer, field, grouping, eventFields, groupings);
        }
    }
}
