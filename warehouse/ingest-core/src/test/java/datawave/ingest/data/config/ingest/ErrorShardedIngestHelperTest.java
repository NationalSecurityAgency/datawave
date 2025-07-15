package datawave.ingest.data.config.ingest;

import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import datawave.TestBaseIngestHelper;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.policy.IngestPolicyEnforcer;

class ErrorShardedIngestHelperTest {

/* SETH NOTE

    tests for the setup method
    https://github.com/NationalSecurityAgency/datawave/pull/2864/files#diff-86d9d0c6cfcff5b686be27e01ab927c38f6c347f59faeebdedd2ccbf96834d70
    1. Verify if no global or datatype specific i/ri configs given, setup does not throw exception.
    2. Verify if global i/ri given, setup does not throw exception. Verify datatype specific is still parsed.
    3. Verify if global i/ri given, but not datatype specific, setup does not throw exception.
    4. Verify that if both allow list and disallow list given for datatype specific, error is thrown.
 */

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

        Assertions.assertEquals(Set.of("FOO", "BAR", "HAT"), helper.getIndexedFields());
        Assertions.assertFalse(helper.hasIndexDisallowlist());

        Assertions.assertEquals(Set.of("APPLE", "BANANA", "KIWI"), helper.getReverseIndexedFields());
        Assertions.assertFalse(helper.hasReverseIndexDisallowlist());
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

        Assertions.assertEquals(Set.of("FOO", "BAR", "HAT"), helper.getIndexedFields());
        Assertions.assertTrue(helper.hasIndexDisallowlist());

        Assertions.assertEquals(Set.of("APPLE", "BANANA", "KIWI"), helper.getReverseIndexedFields());
        Assertions.assertTrue(helper.hasReverseIndexDisallowlist());
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