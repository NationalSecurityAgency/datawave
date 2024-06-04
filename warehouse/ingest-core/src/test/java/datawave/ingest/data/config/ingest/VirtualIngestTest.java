package datawave.ingest.data.config.ingest;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.google.common.collect.Multimap;

import datawave.ingest.data.Type;
import datawave.ingest.data.config.NormalizedContentInterface;

public class VirtualIngestTest extends VirtualFieldTest {

    @Override
    protected VirtualFieldIngestHelper getHelper(String policy, boolean allowMissing) {
        VirtualFieldIngestHelper helper = new TestVirtualFieldIngestHelper(new Type("test", null, null, null, 1, null));
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
    public void testNullVirtualFieldGrouping() {
        VirtualFieldIngestHelper helper = getHelper("RETURN_NULL");
        Multimap<String,NormalizedContentInterface> virtualFields = helper.getVirtualFields(eventFields);

        assertEquals(0, virtualFields.keySet().size());
        // all 5 of one side match all 5 of the other side for a total of 25
        assertEquals(0, virtualFields.get("group1partial1").size());
        assertEquals(0, virtualFields.get("group1partial2").size());
        assertEquals(0, virtualFields.get("partial1group1").size());
        assertEquals(0, virtualFields.get("partial2group1").size());
        assertEquals(0, virtualFields.get("ungroup1ungroup2").size());
        assertEquals(0, virtualFields.get("group1group2").size());
        assertEquals(0, virtualFields.get("ungroup1group1").size());
        assertEquals(0, virtualFields.get("group1ungroup1").size());
        assertEquals(0, virtualFields.get("partial1partial2").size());
    }

    public class TestVirtualFieldIngestHelper extends VirtualFieldIngestHelper {

        public TestVirtualFieldIngestHelper(Type type) {
            super(type);
        }

        @Override
        public void setup(Configuration config) throws IllegalArgumentException {
            virtualFieldNormalizer.setGroupingPolicyClass(TestGroupingPolicy.class);
            super.setup(config);
        }

    }

}
