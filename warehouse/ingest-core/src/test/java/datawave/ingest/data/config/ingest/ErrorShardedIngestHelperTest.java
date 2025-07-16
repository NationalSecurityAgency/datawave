package datawave.ingest.data.config.ingest;

import java.util.Set;

import datawave.ingest.data.config.ConfigurationHelper;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import datawave.TestBaseIngestHelper;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.policy.IngestPolicyEnforcer;

import static datawave.ingest.data.config.ingest.BaseIngestHelper.DISALLOWLIST_INDEX_FIELDS;

class ErrorShardedIngestHelperTest {

/* SETH NOTE

    tests for the setup method
    https://github.com/NationalSecurityAgency/datawave/pull/2864/files#diff-86d9d0c6cfcff5b686be27e01ab927c38f6c347f59faeebdedd2ccbf96834d70
    1. Verify if no global or datatype specific i/ri configs given, setup does not throw exception.
    2. Verify if global i/ri given, setup does not throw exception. Verify datatype specific is still parsed.
    3. Verify if global i/ri given, but not datatype specific, setup does not throw exception.
    4. Verify that if both allow list and disallow list given for datatype specific, error is thrown.
 */

    private static final String DATA_TYPE = "error";

    /**
     * Verify that when indexed and reversed indexed fields are provided, that they are correctly parsed and are not treated as disallowed fields.
     */
    @Test
    void testSetupGivenIndexedFieldLists() {
        Configuration config = getBaseConfig();

        String errorFunnyDataType = DATA_TYPE + ".funny" + BaseIngestHelper.INDEX_FIELDS;
        String errorFruitDataType = DATA_TYPE + ".fruit" + BaseIngestHelper.INDEX_FIELDS;

        config.set(errorFunnyDataType, "FOO,BAR,HAT"); //need to include dt
        config.set(errorFruitDataType, "APPLE,BANANA,KIWI");
        config.set(DATA_TYPE + ".funny" + DISALLOWLIST_INDEX_FIELDS, "FOO");
        config.set(DATA_TYPE + ".fruit" + DISALLOWLIST_INDEX_FIELDS, "KIWI");
        ErrorShardedIngestHelper helper = new ErrorShardedIngestHelper();
        helper.setup(config);

        ConfigurationHelper.isNull(config, errorFunnyDataType, String.class);
        ConfigurationHelper.isNull(config, errorFruitDataType, String.class);
        TypeRegistry.getInstance(config);

        Assertions.assertEquals(Set.of("FOO", "BAR", "HAT"), helper.getIndexedFields(TypeRegistry.getType(errorFunnyDataType))); // need to include dt
        Assertions.assertFalse(helper.hasIndexDisallowlist());

        Assertions.assertEquals(Set.of("APPLE", "BANANA", "KIWI"), helper.getReverseIndexedFields(TypeRegistry.getType(errorFruitDataType)));
        Assertions.assertFalse(helper.hasReverseIndexDisallowlist());
    }

    /**
     * Verify that when disallowed indexed and reversed indexed fields are provided, that they are correctly parsed and are treated as disallowed fields.
     */
    @Test
    void testSetupGivenDisallowedIndexedFieldLists() {
        Configuration config = getBaseConfig();
        config.set(DATA_TYPE + DISALLOWLIST_INDEX_FIELDS, "FOO,BAR,HAT");
        config.set(DATA_TYPE + BaseIngestHelper.DISALLOWLIST_REVERSE_INDEX_FIELDS, "APPLE,BANANA,KIWI");

        ErrorShardedIngestHelper helper = new ErrorShardedIngestHelper();
        helper.setup(config);

        Assertions.assertEquals(Set.of("FOO", "BAR", "HAT"), helper.getIndexedFields());
        Assertions.assertTrue(helper.hasIndexDisallowlist());

        Assertions.assertEquals(Set.of("APPLE", "BANANA", "KIWI"), helper.getReverseIndexedFields());
        Assertions.assertTrue(helper.hasReverseIndexDisallowlist());
    }

    private Configuration getBaseConfig() {
        Configuration config = new Configuration();
        config.set(DataTypeHelper.Properties.DATA_NAME, DATA_TYPE);
        config.set(DATA_TYPE + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS, IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
        config.set(TypeRegistry.INGEST_DATA_TYPES, DATA_TYPE);
        config.set(DATA_TYPE + TypeRegistry.INGEST_HELPER, TestBaseIngestHelper.class.getName());
        return config;
    }
}