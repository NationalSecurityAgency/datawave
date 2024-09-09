package datawave.ingest.data.config.ingest;

import datawave.ingest.data.Type;
import org.apache.hadoop.conf.Configuration;

public class VirtualIngestTest extends VirtualFieldTest {

    @Override
    protected VirtualFieldIngestHelper getHelper(String policy, boolean allowMissing) {
        VirtualFieldIngestHelper helper = new TestVirtualIngest(new Type("test", null, null, null, 1, null));
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

    public class TestVirtualIngest extends VirtualFieldIngestHelper {

        public TestVirtualIngest(Type type) {
            super(type);
        }

        @Override
        public void setup(Configuration config) throws IllegalArgumentException {
            virtualFieldNormalizer.setGroupingPolicyClass(TestGroupingPolicy.class);
            super.setup(config);
        }
    }
}

