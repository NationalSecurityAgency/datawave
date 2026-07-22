package datawave.ingest.data.config;

import static java.util.Objects.requireNonNull;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Map;
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

    @Test
    public void testTreatsMultivalueSeparatorAsLiteralText() {
        // Verify regex characters in multivalue separators are split and cleaned as literal text.
        Configuration conf = new Configuration();
        conf.addResource(requireNonNull(getClass().getResourceAsStream("/fake-datatype-config.xml")));
        conf.set("all" + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS, IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
        conf.setStrings("fake" + CSVHelper.DATA_HEADER, "FIELD");
        conf.set("fake" + CSVHelper.MULTI_VALUED_SEPARATOR, ".");

        try {
            TypeRegistry.reset();
            TypeRegistry.getInstance(conf);
            CSVHelper helper = new CSVHelper();
            helper.setup(conf);

            assertArrayEquals(new String[] {"left", "right\\.middle", "tail"},
                            "left.right\\.middle.tail".split(helper.getEscapeSafeMultiValueSeparatorPattern()));
            assertEquals("right.middle\\+kept", helper.cleanEscapedMultivalueSeparators("right\\.middle\\+kept"));
        } finally {
            TypeRegistry.reset();
        }
    }

    @Test
    public void testSetupResetsStateWhenHelperIsReused() {
        // Verify a second setup call does not keep optional state from an earlier configuration.
        Configuration first = new Configuration();
        first.addResource(requireNonNull(getClass().getResourceAsStream("/fake-datatype-config.xml")));
        first.set("all" + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS, IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
        first.setStrings("fake" + CSVHelper.DATA_HEADER, "FIELD");
        first.setStrings("fake" + CSVHelper.FIELD_ALLOWLIST, "KEEP");
        first.setStrings("fake" + CSVHelper.FIELD_DISALLOWLIST, "DROP");
        first.setStrings("fake" + CSVHelper.REQUIRED_FIELDS, "REQUIRED");
        first.setStrings("fake" + CSVHelper.MULTI_VALUED_FIELDS_DISALLOWLIST, "OLD:RENAMED");
        first.setInt("fake" + CSVHelper.FIELD_SIZE_THRESHOLD, 10);
        first.set("fake" + CSVHelper.THRESHOLD_ACTION, CSVHelper.ThresholdAction.DROP.name());
        first.setInt("fake" + CSVHelper.MULTI_VALUED_THRESHOLD, 2);
        first.set("fake" + CSVHelper.MULTI_VALUED_THRESHOLD_ACTION, CSVHelper.ThresholdAction.TRUNCATE.name());

        Configuration second = new Configuration();
        second.addResource(requireNonNull(getClass().getResourceAsStream("/fake-datatype-config.xml")));
        second.set("all" + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS, IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
        second.setStrings("fake" + CSVHelper.DATA_HEADER, "FIELD");

        try {
            TypeRegistry.reset();
            TypeRegistry.getInstance(first);
            CSVHelper helper = new CSVHelper();
            helper.setup(first);

            assertEquals(Set.of("KEEP"), helper.getFieldAllowlist());
            assertEquals(CSVHelper.ThresholdAction.DROP, helper.getThresholdAction());
            assertEquals(Map.of("OLD", "RENAMED"), helper.getMultiValuedFieldsDisallowlist());

            TypeRegistry.reset();
            TypeRegistry.getInstance(second);
            helper.setup(second);

            assertNull(helper.getFieldAllowlist());
            assertNull(helper.getFieldDisallowlist());
            assertFalse(helper.hasRequiredFields());
            assertFalse(helper.usingMultiValuedFieldsDisallowlist());
            assertEquals(Map.of(), helper.getMultiValuedFields());
            assertEquals(Map.of(), helper.getMultiValuedFieldsDisallowlist());
            assertEquals(Integer.MAX_VALUE, helper.getFieldSizeThreshold());
            assertEquals(Integer.MAX_VALUE, helper.getMultiFieldSizeThreshold());
            assertEquals(CSVHelper.ThresholdAction.FAIL, helper.getThresholdAction());
            assertEquals(CSVHelper.ThresholdAction.FAIL, helper.getMultiValuedThresholdAction());
        } finally {
            TypeRegistry.reset();
        }
    }
}
