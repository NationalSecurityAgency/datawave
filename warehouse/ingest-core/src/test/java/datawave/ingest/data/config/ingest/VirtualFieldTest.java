package datawave.ingest.data.config.ingest;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.ingest.data.Type;
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

    protected VirtualFieldIngestHelper getHelper(String policy) {
        return getHelper(policy, false);
    }

    protected VirtualFieldIngestHelper getHelper(String policy, boolean allowMissing) {
        VirtualFieldIngestHelper helper = new VirtualFieldIngestHelper(new Type("test", null, null, null, 1, null));
        Configuration config = new Configuration();
        config.set("test" + VirtualIngest.VIRTUAL_FIELD_NAMES,
                        "group1partial*,partial*group1,group1group2,group1ungroup1,ungroup1group1,ungroup1ungroup2,partial1partial2,ungroup1empty,emptypartial1");
        config.set("test" + VirtualIngest.VIRTUAL_FIELD_MEMBERS,
                        "GROUPED_1.PARTIAL_*,PARTIAL_*.GROUPED_1,GROUPED_1.GROUPED_2,GROUPED_1.UNGROUPED_1,UNGROUPED_1.GROUPED_1,UNGROUPED_1.UNGROUPED_2,PARTIAL_1.PARTIAL_2,UNGROUPED_1.EMPTY,EMPTY.PARTIAL_1");
        config.set("test" + VirtualIngest.VIRTUAL_FIELD_GROUPING_POLICY, policy);
        config.set("test" + VirtualIngest.VIRTUAL_FIELD_ALLOW_MISSING, Boolean.toString(allowMissing));
        helper.setup(config);
        return helper;
    }

    @Test
    public void testSameGroupOnlyVirtualFieldGrouping() {
        VirtualFieldIngestHelper helper = getHelper("SAME_GROUP_ONLY");
        Multimap<String,NormalizedContentInterface> virtualFields = helper.getVirtualFields(eventFields);

        assertEquals(7, virtualFields.keySet().size());
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
        VirtualFieldIngestHelper helper = getHelper("GROUPED_WITH_NON_GROUPED");
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
        VirtualFieldIngestHelper helper = getHelper("IGNORE_GROUPS");
        Multimap<String,NormalizedContentInterface> virtualFields = helper.getVirtualFields(eventFields);

        assertEquals(9, virtualFields.keySet().size());
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
    }

    @Test
    public void testAllowMissing() {
        VirtualFieldIngestHelper helper = getHelper("GROUPED_WITH_NON_GROUPED", true);
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

}
