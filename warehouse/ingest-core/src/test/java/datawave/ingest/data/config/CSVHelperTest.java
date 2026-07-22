package datawave.ingest.data.config;

import static java.util.Objects.requireNonNull;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import datawave.ingest.data.TypeRegistry;
import datawave.policy.IngestPolicyEnforcer;

public class CSVHelperTest {
    @Test
    public void testLoadsFieldAllowlistAndDisallowlistIndependently() {
        // Verify allowlist and disallowlist configuration populate separate CSVHelper fields.
        Configuration conf = new Configuration();
        conf.addResource(requireNonNull(getClass().getResourceAsStream("/fake-datatype-config.xml")));
        conf.set("all" + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS, IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
        conf.setStrings("fake" + CSVHelper.DATA_HEADER, "FIELD");
        conf.setStrings("fake" + CSVHelper.FIELD_ALLOWLIST, "KEEP");
        conf.setStrings("fake" + CSVHelper.FIELD_DISALLOWLIST, "DROP");

        try {
            TypeRegistry.reset();
            TypeRegistry.getInstance(conf);
            CSVHelper helper = new CSVHelper();
            helper.setup(conf);

            assertEquals(Set.of("KEEP"), helper.getFieldAllowlist());
            assertEquals(Set.of("DROP"), helper.getFieldDisallowlist());
        } finally {
            TypeRegistry.reset();
        }
    }
}
