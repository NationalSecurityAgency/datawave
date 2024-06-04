package datawave.ingest.data.config.ingest;

import java.util.Map;

import com.google.common.collect.Multimap;

import datawave.ingest.data.config.NormalizedContentInterface;

public abstract class AbstractGroupingPolicy {

    // public abstract int valueOf(String name);
    public abstract boolean isIgnoreGroupings();

    // public abstract
    public abstract Iterable<NormalizedContentInterface> applyPolicy(VirtualIngest.VirtualFieldNormalizer normalizer, String field,
                    VirtualIngest.VirtualFieldNormalizer.VirtualFieldGrouping grouping, Multimap<String,NormalizedContentInterface> eventFields,
                    Map<String,Multimap<VirtualIngest.VirtualFieldNormalizer.VirtualFieldGrouping,NormalizedContentInterface>> groupings);

}
