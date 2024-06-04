package datawave.ingest.data.config.ingest;

import java.util.Map;

import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

import datawave.ingest.data.config.NormalizedContentInterface;

public class GroupingPolicy extends AbstractGroupingPolicy {

    private final static int SAME_GROUP_ONLY = 1;
    private final static int GROUPED_WITH_NON_GROUPED = 2;
    private final static int IGNORE_GROUPS = 3;

    private int policy = 0;

    GroupingPolicy() {
        policy = GROUPED_WITH_NON_GROUPED;
    }

    GroupingPolicy(String name) {
        if (name.equals("SAME_GROUP_ONLY")) {
            policy = SAME_GROUP_ONLY;
        } else if (name.equals("GROUPED_WITH_NON_GROUPED")) {
            policy = GROUPED_WITH_NON_GROUPED;
        } else if (name.equals("IGNORE_GROUPS")) {
            policy = IGNORE_GROUPS;
        }
    }

    // @Override
    // public int valueOf(String name) {
    // return 1;
    // }
    //
    @Override
    public boolean isIgnoreGroupings() {
        return policy == IGNORE_GROUPS;
    };

    @Override
    public Iterable<NormalizedContentInterface> applyPolicy(VirtualIngest.VirtualFieldNormalizer normalizer, String field,
                    VirtualIngest.VirtualFieldNormalizer.VirtualFieldGrouping grouping, Multimap<String,NormalizedContentInterface> eventFields,
                    Map<String,Multimap<VirtualIngest.VirtualFieldNormalizer.VirtualFieldGrouping,NormalizedContentInterface>> groupings) {

        switch (policy) {
            case GROUPED_WITH_NON_GROUPED:
                normalizer.updateGroupedEventFields(eventFields, field, groupings);
                if (grouping == null) {
                    // if this grouping is null, then we can match with anything
                    return eventFields.get(field);
                } else {
                    // if we have a grouping, then we can match those with the same grouping or no grouping
                    return Iterables.concat(groupings.get(field).get(null), groupings.get(field).get(grouping));
                }
            case GroupingPolicy.SAME_GROUP_ONLY:
                normalizer.updateGroupedEventFields(eventFields, field, groupings);
                // only return those with the same grouping
                return groupings.get(field).get(grouping);
            default:
                // by default grouping does not matter
                return eventFields.get(field);
        }
    }

}
