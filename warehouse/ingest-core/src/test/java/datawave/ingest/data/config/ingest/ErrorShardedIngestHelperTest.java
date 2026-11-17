package datawave.ingest.data.config.ingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import datawave.TestBaseIngestHelper;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.policy.IngestPolicyEnforcer;

class ErrorShardedIngestHelperTest {

    private static final String DATA_TYPE_NAME = "error";

    /**
     * Verify that when indexed and reversed indexed fields are provided, that they are correctly parsed and are not treated as disallowed fields.
     */
    @Test
    void testSetupGivenIndexedFieldLists() {
        Configuration config = getBaseConfig();
        config.set(DATA_TYPE_NAME + BaseIngestHelper.INDEX_FIELDS, "FOO,BAR,HAT");
        config.set(DATA_TYPE_NAME + BaseIngestHelper.REVERSE_INDEX_FIELDS, "APPLE,BANANA,KIWI");

        ErrorShardedIngestHelper helper = new ErrorShardedIngestHelper();
        helper.setup(config);

        assertEquals(Set.of("FOO", "BAR", "HAT"), helper.getIndexedFields());
        assertFalse(helper.hasIndexDisallowlist());

        // The fields FOO, BAR, and HAT should be considered indexed fields.
        assertTrue(helper.isIndexedField("FOO"));
        assertTrue(helper.isIndexedField("BAR"));
        assertTrue(helper.isIndexedField("HAT"));

        assertEquals(Set.of("APPLE", "BANANA", "KIWI"), helper.getReverseIndexedFields());
        assertFalse(helper.hasReverseIndexDisallowlist());

        // The fields APPLE, BANANA, and KIWI should be considered reverse indexed fields.
        assertTrue(helper.isReverseIndexedField("APPLE"));
        assertTrue(helper.isReverseIndexedField("BANANA"));
        assertTrue(helper.isReverseIndexedField("KIWI"));
    }

    /**
     * Verify that when disallowed indexed and reversed indexed fields are provided, that they are correctly parsed and are treated as disallowed fields.
     */
    @Test
    void testSetupGivenDisallowedIndexedFieldLists() {
        Configuration config = getBaseConfig();
        config.set(DATA_TYPE_NAME + BaseIngestHelper.DISALLOWLIST_INDEX_FIELDS, "FOO,BAR,HAT");
        config.set(DATA_TYPE_NAME + BaseIngestHelper.DISALLOWLIST_REVERSE_INDEX_FIELDS, "APPLE,BANANA,KIWI");

        ErrorShardedIngestHelper helper = new ErrorShardedIngestHelper();
        helper.setup(config);

        assertEquals(Set.of("FOO", "BAR", "HAT"), helper.getIndexedFields());
        assertTrue(helper.hasIndexDisallowlist());

        // Although getIndexedFields() will return FOO, BAR, and HAT, they should not be considered indexed fields due to the disallow list. Verify this as a
        // sanity check.
        assertFalse(helper.isIndexedField("FOO"));
        assertFalse(helper.isIndexedField("BAR"));
        assertFalse(helper.isIndexedField("HAT"));

        assertEquals(Set.of("APPLE", "BANANA", "KIWI"), helper.getReverseIndexedFields());
        assertTrue(helper.hasReverseIndexDisallowlist());

        // Repeat the sanity check for APPLE, BANANA, and KIWI as reverse indexed fields.
        assertFalse(helper.isReverseIndexedField("APPLE"));
        assertFalse(helper.isReverseIndexedField("BANANA"));
        assertFalse(helper.isReverseIndexedField("KIWI"));
    }

    private Configuration getBaseConfig() {
        Configuration config = new Configuration();
        config.set(DataTypeHelper.Properties.DATA_NAME, DATA_TYPE_NAME);
        config.set(DATA_TYPE_NAME + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS, IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
        config.set(TypeRegistry.INGEST_DATA_TYPES, DATA_TYPE_NAME);
        config.set(DATA_TYPE_NAME + TypeRegistry.INGEST_HELPER, TestBaseIngestHelper.class.getName());
        return config;
    }
}
