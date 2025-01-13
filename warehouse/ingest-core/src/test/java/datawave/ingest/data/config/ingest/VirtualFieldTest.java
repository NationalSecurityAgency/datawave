package datawave.ingest.data.config.ingest;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.ingest.data.Type;
import datawave.ingest.data.config.GroupedNormalizedContentInterface;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;

public class VirtualFieldTest {

    protected Multimap<String,NormalizedContentInterface> eventFields = HashMultimap.create();

    @Before
    public void setup() {
        eventFields.put("GROUPED_1", new NormalizedFieldAndValue("GROUPED_1", "value1", "group1", "subgroup1"));
        eventFields.put("GROUPED_1", new NormalizedFieldAndValue("GROUPED_1", "value2", "group2", "subgroup1"));
        eventFields.put("GROUPED_1", new NormalizedFieldAndValue("GROUPED_1", "value3", "group3", "subgroup1"));
        eventFields.put("GROUPED_1", new NormalizedFieldAndValue("GROUPED_1", "value4", "group4", "subgroup1"));
        eventFields.put("GROUPED_1", new NormalizedFieldAndValue("GROUPED_1", "value5", "group5", "subgroup1"));

        eventFields.put("GROUPED_2", new NormalizedFieldAndValue("GROUPED_2", "value1", "group1", "subgroup1"));
        eventFields.put("GROUPED_2", new NormalizedFieldAndValue("GROUPED_2", "value2", "group2", "subgroup1"));
        eventFields.put("GROUPED_2", new NormalizedFieldAndValue("GROUPED_2", "value3", "group3", "subgroup1"));
        eventFields.put("GROUPED_2", new NormalizedFieldAndValue("GROUPED_2", "value4", "group4", "subgroup1"));
        eventFields.put("GROUPED_2", new NormalizedFieldAndValue("GROUPED_2", "value5", "group5", "subgroup1"));

        eventFields.put("GROUPED_3", new NormalizedFieldAndValue("GROUPED_3", "value1", "group1-1", "subgroup1"));
        eventFields.put("GROUPED_3", new NormalizedFieldAndValue("GROUPED_3", "value2", "group2-1", "subgroup2"));
        eventFields.put("GROUPED_3", new NormalizedFieldAndValue("GROUPED_3", "value3", "group3-1", "subgroup3"));
        eventFields.put("GROUPED_3", new NormalizedFieldAndValue("GROUPED_3", "value4", "group4-1", "subgroup4"));
        eventFields.put("GROUPED_3", new NormalizedFieldAndValue("GROUPED_3", "value5", "group5-1", "subgroup5"));

        eventFields.put("GROUPED_4", new NormalizedFieldAndValue("GROUPED_4", "value1", "group1-2", "subgroup1"));
        eventFields.put("GROUPED_4", new NormalizedFieldAndValue("GROUPED_4", "value2", "group2-2", "subgroup2"));
        eventFields.put("GROUPED_4", new NormalizedFieldAndValue("GROUPED_4", "value3", "group3-2", "subgroup3"));
        eventFields.put("GROUPED_4", new NormalizedFieldAndValue("GROUPED_4", "value4", "group4-2", "subgroup4"));
        eventFields.put("GROUPED_4", new NormalizedFieldAndValue("GROUPED_4", "value5", "group5-2", "subgroup5"));

        eventFields.put("UNGROUPED_1", new NormalizedFieldAndValue("UNGROUPED_1", "value1"));
        eventFields.put("UNGROUPED_1", new NormalizedFieldAndValue("UNGROUPED_1", "value2"));
        eventFields.put("UNGROUPED_1", new NormalizedFieldAndValue("UNGROUPED_1", "value3"));
        eventFields.put("UNGROUPED_1", new NormalizedFieldAndValue("UNGROUPED_1", "value4"));
        eventFields.put("UNGROUPED_1", new NormalizedFieldAndValue("UNGROUPED_1", "value5"));

        eventFields.put("UNGROUPED_2", new NormalizedFieldAndValue("UNGROUPED_2", "value1"));
        eventFields.put("UNGROUPED_2", new NormalizedFieldAndValue("UNGROUPED_2", "value2"));
        eventFields.put("UNGROUPED_2", new NormalizedFieldAndValue("UNGROUPED_2", "value3"));
        eventFields.put("UNGROUPED_2", new NormalizedFieldAndValue("UNGROUPED_2", "value4"));
        eventFields.put("UNGROUPED_2", new NormalizedFieldAndValue("UNGROUPED_2", "value5"));

        eventFields.put("PARTIAL_1", new NormalizedFieldAndValue("PARTIAL_1", "value1", "group1", "subgroup1"));
        eventFields.put("PARTIAL_1", new NormalizedFieldAndValue("PARTIAL_1", "value2", "group2", "subgroup1"));
        eventFields.put("PARTIAL_1", new NormalizedFieldAndValue("PARTIAL_1", "value3", "group3", "subgroup1"));
        eventFields.put("PARTIAL_1", new NormalizedFieldAndValue("PARTIAL_1", "value4"));
        eventFields.put("PARTIAL_1", new NormalizedFieldAndValue("PARTIAL_1", "value5"));

        eventFields.put("PARTIAL_2", new NormalizedFieldAndValue("PARTIAL_2", "value1"));
        eventFields.put("PARTIAL_2", new NormalizedFieldAndValue("PARTIAL_2", "value2"));
        eventFields.put("PARTIAL_2", new NormalizedFieldAndValue("PARTIAL_2", "value3", "group3", "subgroup1"));
        eventFields.put("PARTIAL_2", new NormalizedFieldAndValue("PARTIAL_2", "value4", "group4", "subgroup1"));
        eventFields.put("PARTIAL_2", new NormalizedFieldAndValue("PARTIAL_2", "value5", "group5", "subgroup1"));
    }

    protected VirtualFieldIngestHelper getIngestHelper(VirtualIngest.GroupingPolicy policy, boolean allowMissing) {
        VirtualFieldIngestHelper helper = new VirtualFieldTestIngestHelper(new Type("test", null, null, null, 1, null));
        setConfig(helper, policy, allowMissing);
        return helper;
    }

    protected VirtualFieldTestIngestHelper getTestIngestHelper(VirtualIngest.GroupingPolicy policy, boolean allowMissing) {
        VirtualFieldTestIngestHelper helper = new VirtualFieldTestIngestHelper(new Type("test", null, null, null, 1, null));
        setConfig(helper, policy, allowMissing);
        return helper;
    }

    private void setConfig(VirtualFieldIngestHelper helper, VirtualIngest.GroupingPolicy policy, boolean allowMissing) {
        Configuration config = new Configuration();
        config.set("test" + VirtualIngest.VIRTUAL_FIELD_NAMES,
                        "group1partial*,partial*group1,group1group2,group1ungroup1,ungroup1group1,ungroup1ungroup2,partial1partial2,ungroup1empty,emptypartial1,group3group4");
        config.set("test" + VirtualIngest.VIRTUAL_FIELD_MEMBERS,
                        "GROUPED_1.PARTIAL_*,PARTIAL_*.GROUPED_1,GROUPED_1.GROUPED_2,GROUPED_1.UNGROUPED_1,UNGROUPED_1.GROUPED_1,UNGROUPED_1.UNGROUPED_2,PARTIAL_1.PARTIAL_2,UNGROUPED_1.EMPTY,EMPTY.PARTIAL_1,GROUPED_3.GROUPED_4");
        // config.set("test" + VirtualIngest.VIRTUAL_FIELD_NAMES, "group3group4");
        // config.set("test" + VirtualIngest.VIRTUAL_FIELD_MEMBERS, "GROUPED_3.GROUPED_4");
        config.set("test" + VirtualIngest.VIRTUAL_FIELD_GROUPING_POLICY, policy.name());
        config.set("test" + VirtualIngest.VIRTUAL_FIELD_ALLOW_MISSING, Boolean.toString(allowMissing));
        helper.setup(config);
    }

    @Test
    public void testSameGroupOnlyVirtualFieldGrouping() {
        VirtualFieldIngestHelper helper = getIngestHelper(VirtualIngest.GroupingPolicy.SAME_GROUP_ONLY, false);
        Multimap<String,NormalizedContentInterface> virtualFields = helper.getVirtualFields(eventFields);
        assertEquals(7, virtualFields.keySet().size());
        // assertEquals(5, virtualFields.get("partial1partial2").size());
        validateGroupOnlyResults(virtualFields);
    }

    @Test
    public void testSameGroupOnlyAlternateGroupFalseSetVirtualFieldGrouping() {
        VirtualFieldTestIngestHelper helper = getTestIngestHelper(VirtualIngest.GroupingPolicy.SAME_GROUP_ONLY, false);
        Multimap<String,NormalizedContentInterface> virtualFields = helper.getVirtualFields(eventFields);
        assertEquals(7, virtualFields.keySet().size());
        validateGroupOnlyResults(virtualFields);
    }

    @Test
    public void testSameGroupOnlyAlternateGroupTrueSetVirtualFieldGrouping() {
        VirtualFieldTestIngestHelper helper = getTestIngestHelper(VirtualIngest.GroupingPolicy.SAME_GROUP_OR_ALTERNATE_METHOD, false);

        Multimap<String,NormalizedContentInterface> virtualFields = helper.getVirtualFields(eventFields);
        assertEquals(8, virtualFields.keySet().size());
        validateGroupOnlyResults(virtualFields);

        Set<String> expectedGroups = new HashSet<>();
        expectedGroups.add("group1-1.group1-2");
        expectedGroups.add("group2-1.group2-2");
        expectedGroups.add("group3-1.group3-2");
        expectedGroups.add("group4-1.group4-2");
        expectedGroups.add("group5-1.group5-2");

        assertEquals(5, virtualFields.get("group3group4").size());

        Set<String> actualGroups = new HashSet<>();
        for (NormalizedContentInterface value : virtualFields.get("group3group4")) {
            NormalizedFieldAndValue v = (NormalizedFieldAndValue) value;
            actualGroups.add(v.getGroup());
        }
        assertEquals(expectedGroups, actualGroups);
    }

    private void validateGroupOnlyResults(Multimap<String,NormalizedContentInterface> virtualFields) {

        // 3 groups match for the patterned virtual fields
        assertEquals(3, virtualFields.get("group1partial1").size());
        assertEquals(3, virtualFields.get("group1partial2").size());
        assertEquals(3, virtualFields.get("partial1group1").size());
        assertEquals(3, virtualFields.get("partial2group1").size());
        // 5 * 5 matches between ungrouped fields
        assertEquals(25, virtualFields.get("ungroup1ungroup2").size());
        // 5 groups matching the same 5 groups
        assertEquals(5, virtualFields.get("group1group2").size());
        assertEquals(5, virtualFields.get("partial1partial2").size());
    }

    @Test
    public void testGroupedWithNonGroupedVirtualFieldGrouping() {
        VirtualFieldIngestHelper helper = getIngestHelper(VirtualIngest.GroupingPolicy.GROUPED_WITH_NON_GROUPED, false);
        Multimap<String,NormalizedContentInterface> virtualFields = helper.getVirtualFields(eventFields);

        assertEquals(9, virtualFields.keySet().size());
        // the 3 matching groups plus all group1 groups against the partial fields with null groups for a total of 13
        assertEquals(13, virtualFields.get("group1partial1").size());
        assertEquals(13, virtualFields.get("group1partial2").size());
        assertEquals(13, virtualFields.get("partial1group1").size());
        assertEquals(13, virtualFields.get("partial2group1").size());
        // 5 groups matching the same 5 groups
        assertEquals(5, virtualFields.get("group1group2").size());
        // 5 * 5 matches between ungrouped fields or between grouped and ungrouped fields
        assertEquals(25, virtualFields.get("ungroup1ungroup2").size());
        assertEquals(25, virtualFields.get("ungroup1group1").size());
        assertEquals(25, virtualFields.get("group1ungroup1").size());
        // The first two groups of partial 1 match the null groups of partial 2 (total 4)
        // The third group of partial 1 matches the null groups of partial 2 and the matching groups (total 3)
        // The two null groups of partial 1 match all of fields in partial 2 (total 10)
        // for a grand total of 17
        assertEquals(17, virtualFields.get("partial1partial2").size());
    }

    @Test
    public void testIgnoreGroupsVirtualFieldGrouping() {
        VirtualFieldIngestHelper helper = getIngestHelper(VirtualIngest.GroupingPolicy.IGNORE_GROUPS, false);
        Multimap<String,NormalizedContentInterface> virtualFields = helper.getVirtualFields(eventFields);

        assertEquals(10, virtualFields.keySet().size());
        // all 5 of one side match all 5 of the other side for a total of 25
        assertEquals(25, virtualFields.get("group1partial1").size());
        assertEquals(25, virtualFields.get("group1partial2").size());
        assertEquals(25, virtualFields.get("partial1group1").size());
        assertEquals(25, virtualFields.get("partial2group1").size());
        assertEquals(25, virtualFields.get("ungroup1ungroup2").size());
        assertEquals(25, virtualFields.get("group1group2").size());
        assertEquals(25, virtualFields.get("ungroup1group1").size());
        assertEquals(25, virtualFields.get("group1ungroup1").size());
        assertEquals(25, virtualFields.get("partial1partial2").size());
        assertEquals(25, virtualFields.get("group3group4").size());
    }

    @Test
    public void testAllowMissing() {
        VirtualFieldIngestHelper helper = getIngestHelper(VirtualIngest.GroupingPolicy.GROUPED_WITH_NON_GROUPED, true);
        Multimap<String,NormalizedContentInterface> virtualFields = helper.getVirtualFields(eventFields);

        assertEquals(11, virtualFields.keySet().size());
        // the 3 matching groups plus all group1 groups against the partial fields with null groups for a total of 13
        assertEquals(13, virtualFields.get("group1partial1").size());
        assertEquals(13, virtualFields.get("group1partial2").size());
        assertEquals(13, virtualFields.get("partial1group1").size());
        assertEquals(13, virtualFields.get("partial2group1").size());
        // 5 groups matching the same 5 groups
        assertEquals(5, virtualFields.get("group1group2").size());
        // 5 * 5 matches between ungrouped fields or between grouped and ungrouped fields
        assertEquals(25, virtualFields.get("ungroup1ungroup2").size());
        assertEquals(25, virtualFields.get("ungroup1group1").size());
        assertEquals(25, virtualFields.get("group1ungroup1").size());
        // The first two groups of partial 1 match the null groups of partial 2 (total 4)
        // The third group of partial 1 matches the null groups of partial 2 and the matching groups (total 3)
        // The two null groups of partial 1 match all of fields in partial 2 (total 10)
        // for a grand total of 17
        assertEquals(17, virtualFields.get("partial1partial2").size());
        // The empty fields now produce a virtual field for each of the 5 ungroup1 or partial1 values
        assertEquals(5, virtualFields.get("ungroup1empty").size());
        assertEquals(5, virtualFields.get("emptypartial1").size());
    }

    public class VirtualFieldTestIngestHelper extends VirtualFieldIngestHelper {
        public VirtualFieldTestIngestHelper(Type type) {
            super(type);
            virtualFieldNormalizer = new VirtualFieldTestNormalizer();
        }

        class VirtualFieldTestNormalizer extends VirtualFieldNormalizer {

            @Override
            public VirtualFieldGrouping getAlternateGrouping(NormalizedContentInterface value) {
                if (value instanceof GroupedNormalizedContentInterface && ((GroupedNormalizedContentInterface) value).isGrouped()) {
                    NormalizedFieldAndValue groupedNCI = (NormalizedFieldAndValue) value;
                    String family = groupedNCI.getEventFieldName().split("\\.")[0].split("_")[0];
                    return new VirtualFieldGrouping(family, groupedNCI.getSubGroup(), groupedNCI.getGroup());
                }
                return null;
            }
        }
    }

}
