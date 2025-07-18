package datawave.ingest.data.config.ingest;

import static datawave.ingest.data.config.ingest.BaseIngestHelper.DISALLOWLIST_INDEX_FIELDS;
import static datawave.ingest.data.config.ingest.BaseIngestHelper.INDEX_FIELDS;

import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import datawave.TestBaseIngestHelper;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.policy.IngestPolicyEnforcer;

class ErrorShardedIngestHelperTest {

    /*
     * SETH NOTE
     *
     * tests for the setup method
     * https://github.com/NationalSecurityAgency/datawave/pull/2864/files#diff-86d9d0c6cfcff5b686be27e01ab927c38f6c347f59faeebdedd2ccbf96834d70 1. Verify if no
     * global or datatype specific i/ri configs given, setup does not throw exception. 2. Verify if global i/ri given, setup does not throw exception. Verify
     * datatype specific is still parsed. 3. Verify if global i/ri given, but not datatype specific, setup does not throw exception. 4. Verify that if both
     * allow list and disallow list given for datatype specific, error is thrown.
     *
     *
     *
     * 1. I think get/setActiveDatatype() should be put in ErrorShardedIngestHelper, not BaseIngestHelper. 2. You need to set the config property that will add
     * each datatype you're using to the TypeRegistry. This can be done via the following:
     * config.set(TypeRegistry.INGEST_DATA_TYPES,"<datatype1>,<datatype2>,<datatype3>,etc"); 3. We should expect the data-type specific error index
     * configurations to follow the format error.<datatype>.etc, not <datatype>.error.data.etc, since we're parsing them from the error configuration. I'm
     * basing that off your conversation with Ivan. 4. In ErrorShardedIngestHelper.setup(), configProperty should not be a Type. Refer back to the
     * BaseIngestHelper.setup method where the configProperty is a String. To get all properties in the configuration that start with 'error.', you can use
     * config.getPropsWithPrefix("error."), which will return a map of properties to values. You can then iterate through the property keys of that map, and
     * identify which keys match the format error.<datatype>.data.index.etc for the various properties that contain the various error index configurations, and
     * then build your maps of datatypes to IndexFields from there.
     */

    private static final String DATA_TYPE = "error";

    /**
     * Verify that when indexed and reversed indexed fields are provided, that they are correctly parsed and are not treated as disallowed fields.
     */
    @Test
    void testSetupGivenIndexedFieldLists() {
        Configuration config = getBaseConfig();

//        String errorFunnyDataTypeIndexedFields = TypeRegistry.ERROR_PREFIX + ".funny" + ".index";
//        String errorFruitDataTypeIndexedFields = TypeRegistry.ERROR_PREFIX + ".fruit" + ".index";

//            config.set(DataTypeHelper.Properties.DATA_NAME, "error");
            config.set(TypeRegistry.INGEST_DATA_TYPES, "csv");

//        conf.set(DATA_TYPE_NAME + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS, IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
//        conf.set(DataTypeHelper.Properties.DATA_NAME, DATA_TYPE_NAME);
//        conf.set(TypeRegistry.INGEST_DATA_TYPES, DATA_TYPE_NAME);
//        conf.set(DATA_TYPE_NAME + TypeRegistry.INGEST_HELPER, INGEST_HELPER_CLASS);

        config.set("error.csv" + INDEX_FIELDS, "FOO,BAR,HATT");

//        TypeRegistry.getInstance(config);

        ErrorShardedIngestHelper helper = new ErrorShardedIngestHelper();
        helper.setup(config);

        Assertions.assertEquals(Set.of("FOO", "BAR", "HAT"), helper.getIndexedFields(TypeRegistry.getType("error"))); // need to include dt
        Assertions.assertFalse(helper.hasIndexDisallowlist());

        Assertions.assertEquals(Set.of("APPLE", "BANANA", "KIWI"), helper.getReverseIndexedFields(TypeRegistry.getType("error.fruit")));
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
