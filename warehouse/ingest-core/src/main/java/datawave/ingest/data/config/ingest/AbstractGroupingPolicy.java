package datawave.ingest.data.config.ingest;

import java.util.Map;

import com.google.common.collect.Multimap;

import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.util.ObjectFactory;

public abstract class AbstractGroupingPolicy {

    // public abstract int valueOf(String name);
    protected int policy;

    protected abstract void setPolicy(String name);

    public abstract boolean isIgnoreGroupings();

    public abstract Iterable<NormalizedContentInterface> applyPolicy(VirtualIngest.VirtualFieldNormalizer normalizer, String field,
                    VirtualIngest.VirtualFieldNormalizer.VirtualFieldGrouping grouping, Multimap<String,NormalizedContentInterface> eventFields,
                    Map<String,Multimap<VirtualIngest.VirtualFieldNormalizer.VirtualFieldGrouping,NormalizedContentInterface>> groupings);

    // public static Class<? extends AbstractGroupingPolicy> getGroupingPolicy(Class<? extends AbstractGroupingPolicy> clazz) {
    // return (Class<? extends AbstractGroupingPolicy>) ObjectFactory.create(clazz.getName());
    // };
    public static AbstractGroupingPolicy getGroupingPolicy(Class<? extends AbstractGroupingPolicy> clazz) {
        return (AbstractGroupingPolicy) ObjectFactory.create(clazz.getName());
    };

    public static AbstractGroupingPolicy getGroupingPolicy(Class<? extends AbstractGroupingPolicy> clazz, String policyName) {
        return (AbstractGroupingPolicy) ObjectFactory.create(clazz.getName(), policyName);
    };

}
