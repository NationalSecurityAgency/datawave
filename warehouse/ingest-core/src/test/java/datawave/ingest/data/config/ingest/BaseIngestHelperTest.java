package datawave.ingest.data.config.ingest;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.Function;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import datawave.TestBaseIngestHelper;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.policy.IngestPolicyEnforcer;

class BaseIngestHelperTest {

    private static final String FIELD_CONFIG_FILE = "datawave/ingest/data/config/ingest/BaseIngestHelperTest_IsIndexedFieldTests_field-config.xml";

    /**
     * Executes tests for {@link BaseIngestHelper#isIndexedField(String)}.
     */
    @Nested
    class IsIndexedFieldTests extends BaseIsIndexedFieldTests {

        private static final String DATA_TYPE_NAME = "test";

        @Override
        String getDataTypeName() {
            return DATA_TYPE_NAME;
        }

        @Override
        String getFieldListProperty() {
            return BaseIngestHelper.INDEX_FIELDS;
        }

        @Override
        String getDisallowListProperty() {
            return BaseIngestHelper.DISALLOWLIST_INDEX_FIELDS;
        }

        /**
         * Return {@link BaseIngestHelper#isIndexedField(String)} as the function under test.
         */
        @Override
        Function<String,Boolean> getFunctionUnderTest() {
            TestBaseIngestHelper ingestHelper = new TestBaseIngestHelper();
            ingestHelper.setup(config);
            return ingestHelper::isIndexedField;
        }
    }

    /**
     * Executes tests for {@link BaseIngestHelper#isReverseIndexedField(String)}.
     */
    @Nested
    class IsReversedIndexedFieldTests extends BaseIsIndexedFieldTests {

        private static final String DATA_TYPE_NAME = "test";

        @Override
        String getDataTypeName() {
            return DATA_TYPE_NAME;
        }

        @Override
        String getFieldListProperty() {
            return BaseIngestHelper.REVERSE_INDEX_FIELDS;
        }

        @Override
        String getDisallowListProperty() {
            return BaseIngestHelper.DISALLOWLIST_REVERSE_INDEX_FIELDS;
        }

        /**
         * Return {@link BaseIngestHelper#isReverseIndexedField(String)} as the function under test.
         */
        @Override
        Function<String,Boolean> getFunctionUnderTest() {
            TestBaseIngestHelper ingestHelper = new TestBaseIngestHelper();
            ingestHelper.setup(config);
            return ingestHelper::isReverseIndexedField;
        }
    }

    /**
     * Test cases for {@link BaseIngestHelper#isIndexedField(String)} and {@link BaseIngestHelper#isReverseIndexedField(String)}. The test cases for both
     * functions are effectively identical, so we can use the same base class to test both.
     */
    abstract static class BaseIsIndexedFieldTests {

        protected Configuration config;

        @BeforeEach
        void setUp() {
            initBaseConfig();
        }

        private void initBaseConfig() {
            config = new Configuration();
            String dataTypeName = getDataTypeName();
            config.set(DataTypeHelper.Properties.DATA_NAME, dataTypeName);
            config.set(dataTypeName + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS, IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
            config.set(TypeRegistry.INGEST_DATA_TYPES, dataTypeName);
            config.set(dataTypeName + TypeRegistry.INGEST_HELPER, TestBaseIngestHelper.class.getName());
        }

        /**
         * Verify that when a field config helper is provided, and the field is to be indexed, that we receive a result of true.
         */
        @Test
        void givenFieldConfigHelperWithFieldIndexed() {
            config.set(getDataTypeName() + BaseIngestHelper.FIELD_CONFIG_FILE, FIELD_CONFIG_FILE);
            assertTrue(getFunctionUnderTest().apply("FOO"));
        }

        /**
         * Verify that when a field config helper is provided, and the field is not to be indexed, that we receive a result of false.
         */
        @Test
        void givenFieldConfigHelperWithFieldNotIndexed() {
            config.set(getDataTypeName() + BaseIngestHelper.FIELD_CONFIG_FILE, FIELD_CONFIG_FILE);
            assertFalse(getFunctionUnderTest().apply("HAT"));
        }

        /**
         * Verify that when a field config helper is provided, that it overrides any fields provided via the field list property.
         */
        @Test
        void givenFieldConfigThenIndexFieldListIsOverridden() {
            config.set(getDataTypeName() + BaseIngestHelper.FIELD_CONFIG_FILE, FIELD_CONFIG_FILE);
            config.set(getDataTypeName() + getFieldListProperty(), "FOO,BAR,HAT");
            assertFalse(getFunctionUnderTest().apply("HAT")); // index="false" for "HAT" in the field config.
        }

        /**
         * Verify that when a field config helper is provided, that it overrides any fields provided via the disallow list fields property.
         */
        @Test
        void givenFieldConfigThenDisallowIndexFieldListIsOverridden() {
            config.set(getDataTypeName() + BaseIngestHelper.FIELD_CONFIG_FILE, FIELD_CONFIG_FILE);
            config.set(getDataTypeName() + getDisallowListProperty(), "FOO,BAR,HAT");
            assertTrue(getFunctionUnderTest().apply("FOO")); // index="true" for "FOO" in the field config.
        }

        /**
         * Verify that fields are provided via the field list property, and it contains the field, that we receive a result of true.
         */
        @Test
        void givenIndexFieldsWithMatch() {
            config.set(getDataTypeName() + getFieldListProperty(), "FOO,BAR,HAT");
            assertTrue(getFunctionUnderTest().apply("FOO"));
        }

        /**
         * Verify that fields are provided via the field list property, and it does not contain the field, that we receive a result of false.
         */
        @Test
        void giveFieldsWithNoMatch() {
            config.set(getDataTypeName() + getFieldListProperty(), "FOO,BAR,HAT");
            assertFalse(getFunctionUnderTest().apply("BAZ"));
        }

        /**
         * Verify that fields are provided via the disallow list fields property, and it contains the field, that we receive a result of false.
         */
        @Test
        void givenDisallowIndexFieldsWithMatch() {
            config.set(getDataTypeName() + getDisallowListProperty(), "FOO,BAR,HAT");
            assertFalse(getFunctionUnderTest().apply("FOO"));
        }

        /**
         * Verify that fields are provided via the disallow list fields property, and does not contain the field, that we receive a result of true.
         */
        @Test
        void givenDisallowIndexFieldsWithNoMatch() {
            config.set(getDataTypeName() + getDisallowListProperty(), "FOO,BAR,HAT");
            assertTrue(getFunctionUnderTest().apply("BAZ"));
        }

        abstract String getDataTypeName();

        abstract String getFieldListProperty();

        abstract String getDisallowListProperty();

        abstract Function<String,Boolean> getFunctionUnderTest();
    }
}
