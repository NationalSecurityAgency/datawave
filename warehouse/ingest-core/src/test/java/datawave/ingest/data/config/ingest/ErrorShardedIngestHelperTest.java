package datawave.ingest.data.config.ingest;

import static datawave.ingest.data.config.ingest.BaseIngestHelper.INDEX_FIELDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.TestBaseIngestHelper;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.policy.IngestPolicyEnforcer;

class ErrorShardedIngestHelperTest {

    private static final String DATA_TYPE_NAME = "error";

    @BeforeEach
    void setUp() {
        TypeRegistry.reset();
    }

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

    @Test
    public void testErrorIndexFieldsCreated() {

        Configuration config = new Configuration();

        config.set(TypeRegistry.INGEST_DATA_TYPES, "xyz, error");

        config.set("xyz" + TypeRegistry.INGEST_HELPER, BaseIngestHelper.class.getName());
        config.set("xyz" + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS, IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());

        config.set("error" + TypeRegistry.INGEST_HELPER, ErrorShardedIngestHelper.class.getName());
        config.set("error" + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS, IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());

        config.set("xyz" + INDEX_FIELDS, "XYZ_A, XYZ_B, XYZ_C");

        config.set("error" + "." + "xyz" + TypeRegistry.INGEST_HELPER, ErrorShardedIngestHelper.class.getName());
        config.set("error" + "." + "xyz" + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS,
                        IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
        config.set("error" + "." + "xyz" + INDEX_FIELDS, "XYZ_ERROR_A, XYZ_A");

        ErrorShardedIngestHelper errorHelper = new ErrorShardedIngestHelper();
        errorHelper.setup(config);

        // With null activeDataType, checks union of ALL per-datatype error configs as a safe fallback.
        // error.xyz config has {XYZ_ERROR_A, XYZ_A}, so both should match.
        errorHelper.setActiveDataType(null);
        assertFalse(errorHelper.isIndexedField("UNINDEXED_FIELD"));
        assertTrue(errorHelper.isIndexedField("XYZ_A")); // in error.xyz config
        assertTrue(errorHelper.isIndexedField("XYZ_ERROR_A")); // in error.xyz config

        errorHelper.setActiveDataType(TypeRegistry.getType("xyz"));
        assertFalse(errorHelper.isIndexedField("UNINDEXED_FIELD"));
        assertTrue(errorHelper.isIndexedField("XYZ_A"));
        assertTrue(errorHelper.isIndexedField("XYZ_ERROR_A"));

    }

    @Test
    public void testIsIndexedField() {

        /*
         *
         * Data Types: CSV, JSON /note: JSON also uses the CSV ingest helper/
         *
         * CSV Indexed Fields: CSV_A, CSV_B, CSV_C JSON Indexed Fields: JSON_A, JSON_B, JSON_C
         *
         * ERROR CSV Indexed Fields: CSV_ERROR_A, CSV_A ERROR JSON Indexed Fields: JSON_ERROR_A, JSON_A
         *
         */

        Configuration config = new Configuration();

        // --- CSV ---

        config.set(TypeRegistry.INGEST_DATA_TYPES, "csv, json, error");
        config.set(DataTypeHelper.Properties.DATA_NAME, "error");
        config.set("csv" + TypeRegistry.INGEST_HELPER, BaseIngestHelper.class.getName());
        config.set("csv" + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS, IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
        config.set("csv" + INDEX_FIELDS, "CSV_A, CSV_B, CSV_C");

        config.set("csv.data.header", "okay");
        config.set("csv.data.separator", ",");

        config.set("error" + "." + "csv" + TypeRegistry.INGEST_HELPER, ErrorShardedIngestHelper.class.getName());
        config.set("error" + "." + "csv" + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS,
                        IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
        config.set("error" + "." + "csv" + INDEX_FIELDS, "CSV_ERROR_A, CSV_A");

        // --- JSON ---

        config.set("json" + TypeRegistry.INGEST_HELPER, BaseIngestHelper.class.getName());
        config.set("json" + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS, IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
        config.set("json" + INDEX_FIELDS, "JSON_A, JSON_B, JSON_C");

        config.set("json.data.header", "no-way");
        config.set("json.data.separator", ",");

        config.set("error" + "." + "json" + TypeRegistry.INGEST_HELPER, ErrorShardedIngestHelper.class.getName());
        config.set("error" + "." + "json" + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS,
                        IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
        config.set("error" + "." + "json" + INDEX_FIELDS, "JSON_ERROR_A, JSON_A");

        config.set("error" + TypeRegistry.INGEST_HELPER, ErrorShardedIngestHelper.class.getName());
        config.set("error" + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS, IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
        config.set("error" + INDEX_FIELDS, "GLOB_ERROR_A, GLOB_A");

        ErrorShardedIngestHelper errorHelper = new ErrorShardedIngestHelper();

        errorHelper.setup(config);

        // --- NULL DATA TYPE ---
        // With null activeDataType, checks union of ALL per-datatype error configs plus base error fields.
        // error.csv config has {CSV_ERROR_A, CSV_A}, error.json config has {JSON_ERROR_A, JSON_A},
        // base error config has {GLOB_ERROR_A, GLOB_A}.
        errorHelper.setActiveDataType(null);
        assertFalse(errorHelper.isIndexedField("UNINDEXED_FIELD"));

        // Fields from any per-datatype error config should be indexed
        assertTrue(errorHelper.isIndexedField("CSV_A")); // in error.csv config
        assertTrue(errorHelper.isIndexedField("JSON_A")); // in error.json config
        assertTrue(errorHelper.isIndexedField("CSV_ERROR_A")); // in error.csv config
        assertTrue(errorHelper.isIndexedField("JSON_ERROR_A")); // in error.json config

        // Base error index fields should also be indexed
        assertTrue(errorHelper.isIndexedField("GLOB_A"));
        assertTrue(errorHelper.isIndexedField("GLOB_ERROR_A"));

        // --- CSV vs JSON DATA TYPE ---
        errorHelper.setActiveDataType(TypeRegistry.getType("csv"));
        // dt HAS been set, field IS NOT indexed on any datatype
        assertFalse(errorHelper.isIndexedField("UNINDEXED_FIELD"));

        // The following should use the ErrorShardedIngestHelper.isIndexedField() method instead of the general one in BaseIngestHelper.

        // dt HAS been set, field IS indexed as normal indexed field
        assertTrue(errorHelper.isIndexedField("CSV_A"));
        assertFalse(errorHelper.isIndexedField("JSON_A"));

        // dt HAS been set, field IS NOT indexed as normal indexed field, only as error indexed field
        assertTrue(errorHelper.isIndexedField("CSV_ERROR_A"));
        assertFalse(errorHelper.isIndexedField("JSON_ERROR_A"));

        // --- JSON DATA TYPE ---
        errorHelper.setActiveDataType(TypeRegistry.getType("json"));
        // dt HAS been set, field IS NOT indexed on any datatype
        assertFalse(errorHelper.isIndexedField("UNINDEXED_FIELD"));

        // The following should use the ErrorShardedIngestHelper.isIndexedField() method instead of the general one in BaseIngestHelper.

        // dt HAS been set, field IS indexed as normal indexed field
        assertFalse(errorHelper.isIndexedField("CSV_A"));
        assertTrue(errorHelper.isIndexedField("JSON_A"));

        // dt HAS been set, field IS NOT indexed as normal indexed field, only as error indexed field
        assertFalse(errorHelper.isIndexedField("CSV_ERROR_A"));
        assertTrue(errorHelper.isIndexedField("JSON_ERROR_A"));

    }

    // @Test
    // public void testIsReverseIndexedField() {
    //
    // Configuration config = new Configuration();
    //
    // config.set(TypeRegistry.INGEST_DATA_TYPES, "csv");
    // config.set(DataTypeHelper.Properties.DATA_NAME, "csv");
    // config.set("csv" + TypeRegistry.INGEST_HELPER, CSVIngestHelper.class.getName());
    // config.set("csv" + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS, IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
    // config.set("csv" + REVERSE_INDEX_FIELDS, "DT_A, DT_B, DT_C");
    //
    // config.set("csv.data.header", "okay");
    // config.set("csv.data.separator", ",");
    //
    // config.set("error" + "." + "csv" + TypeRegistry.INGEST_HELPER, ErrorShardedIngestHelper.class.getName());
    // config.set("error" + "." + "csv" + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS,
    // IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
    // config.set("error" + "." + "csv" + REVERSE_INDEX_FIELDS, "DT_B");
    //
    // ErrorShardedIngestHelper errorHelper = new ErrorShardedIngestHelper();
    //
    // errorHelper.setup(config);
    //
    // // NULL dt
    // errorHelper.setActiveDataType(null);
    // // dt HAS NOT been set, index DOES NOT exist
    // assertFalse(errorHelper.isReverseIndexedField("UNINDEXED_FIELD"));
    //
    // // CSV dt
    // errorHelper.setActiveDataType(TypeRegistry.getType("csv"));
    // // dt HAS been set, index DOES NOT exist
    // assertFalse(errorHelper.isReverseIndexedField("UNINDEXED_FIELD"));
    // // dt HAS been set, dt index DOES exist
    // assertTrue(errorHelper.isReverseIndexedField("DT_B"));
    //
    // }

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
