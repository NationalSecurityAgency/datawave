package datawave.ingest.data.config.ingest;

import static datawave.ingest.data.config.CSVHelper.DATA_HEADER;
import static datawave.ingest.data.config.CSVHelper.DATA_HEADER_ENABLED;
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

    /**
     * Verify that when indexed and reversed indexed fields are provided, that they are correctly parsed and are not treated as disallowed fields.
     */
    @Test
    void testSetupGivenIndexedFieldLists() {

        Configuration csvConfig = new Configuration();

        csvConfig.set(TypeRegistry.INGEST_DATA_TYPES, "csv");
        csvConfig.set(DataTypeHelper.Properties.DATA_NAME, "csv");
        csvConfig.set("csv" + TypeRegistry.INGEST_HELPER, CSVIngestHelper.class.getName());
        csvConfig.set("csv" + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS, IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
        csvConfig.set("csv" + INDEX_FIELDS, "FOO,BAR,HAT");
        csvConfig.set("csv.data.header", "okay");
        csvConfig.set("csv.data.separator", ",");

        CSVIngestHelper csvHelper = new CSVIngestHelper();
        csvHelper.setup(csvConfig);

        // --- ERROR CONFIG ---

        Configuration errorConfig = new Configuration();

        errorConfig.set(TypeRegistry.INGEST_DATA_TYPES, "csv");
        errorConfig.set(DataTypeHelper.Properties.DATA_NAME, "csv");
        errorConfig.set("csv" + TypeRegistry.INGEST_HELPER, CSVIngestHelper.class.getName());
        errorConfig.set("csv" + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS, IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
        errorConfig.set("csv" + INDEX_FIELDS, "FOO,BAR");

        errorConfig.set("error" + TypeRegistry.INGEST_HELPER, ErrorShardedIngestHelper.class.getName());
        errorConfig.set("error" + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS, IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
        errorConfig.set("error" + INDEX_FIELDS, "FOO");

        ErrorShardedIngestHelper errorHelper = new ErrorShardedIngestHelper();
        errorHelper.setActiveDataType(TypeRegistry.getType("csv"));
        errorHelper.setup(errorConfig);

        Assertions.assertEquals(Set.of("FOO", "BAR", "HAT"), csvHelper.getIndexedFields());
        Assertions.assertEquals(Set.of("FOO", "BAR", "HAT"), errorHelper.getIndexedFields(TypeRegistry.getType("csv"))); // need to include dt
        Assertions.assertFalse(errorHelper.hasIndexDisallowlist());

        Assertions.assertEquals(Set.of("APPLE", "BANANA", "KIWI"), errorHelper.getReverseIndexedFields(TypeRegistry.getType("error.fruit")));
        Assertions.assertFalse(errorHelper.hasReverseIndexDisallowlist());
    }


}
