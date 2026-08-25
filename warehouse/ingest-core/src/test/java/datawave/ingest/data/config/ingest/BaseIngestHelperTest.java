package datawave.ingest.data.config.ingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import com.google.common.collect.Multimap;

import datawave.TestBaseIngestHelper;
import datawave.data.normalizer.Normalizer;
import datawave.data.type.BaseType;
import datawave.data.type.DateType;
import datawave.data.type.HexStringType;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NoOpType;
import datawave.data.type.NumberType;
import datawave.data.type.Type;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.ingest.data.config.FieldLookupCache;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.data.config.XMLFieldConfigHelper;
import datawave.policy.IngestPolicyEnforcer;

class BaseIngestHelperTest {

    private static final String FIELD_CONFIG_FILE = "datawave/ingest/data/config/ingest/BaseIngestHelperTest_IsIndexedFieldTests_field-config.xml";

    private static final String DATA_TYPE_NAME = "test";

    /**
     * Declares {@code FOO} with its own type, {@code BAR} with no type of its own, a {@code *_DATE} pattern, a {@code default} type applied to fields that
     * declare no {@code indexType}, and a {@code nomatch} type used for fields the config says nothing about.
     */
    private static final String FIELD_TYPE_CONFIG_FILE = "datawave/ingest/data/config/ingest/BaseIngestHelperTest_FieldTypeTests_field-config.xml";

    /**
     * The configuration shared by the field type tests: the {@code test} datatype wired to {@link TypeTestIngestHelper}, with no field config file declared
     * yet.
     */
    private static Configuration typeTestConfig() {
        Configuration config = new Configuration();
        config.set(DataTypeHelper.Properties.DATA_NAME, DATA_TYPE_NAME);
        config.set(DATA_TYPE_NAME + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS, IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
        config.set(TypeRegistry.INGEST_DATA_TYPES, DATA_TYPE_NAME);
        config.set(DATA_TYPE_NAME + TypeRegistry.INGEST_HELPER, TypeTestIngestHelper.class.getName());
        TypeRegistry.reset();
        return config;
    }

    private static void assertSoleType(Class<?> expected, List<Type<?>> types) {
        assertEquals(1, types.size(), "Expected a single type, but was: " + types);
        assertInstanceOf(expected, types.get(0));
    }

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

    /**
     * Executes tests for {@link BaseIngestHelper#normalizeFieldValue(NormalizedContentInterface, datawave.data.type.Type)}.
     */
    @Nested
    class NormalizeFieldValueTests {

        /** Counts normalize invocations. */
        private class CountingType extends BaseType<String> {

            private static final long serialVersionUID = 1L;

            private final AtomicInteger calls;

            CountingType(AtomicInteger calls) {
                super(Normalizer.LC_NO_DIACRITICS_NORMALIZER);
                this.calls = calls;
            }

            @Override
            public String normalize(String in) {
                calls.incrementAndGet();
                return super.normalize(in);
            }
        }

        /**
         * Verify that a value is normalized exactly once and both fields carry the result.
         */
        @Test
        void givenAValueThenNormalizesExactlyOnce() {
            AtomicInteger calls = new AtomicInteger();
            TestBaseIngestHelper helper = new TestBaseIngestHelper();

            NormalizedContentInterface result = helper.normalizeFieldValue(new NormalizedFieldAndValue("FIELD", "MixedCase Value"), new CountingType(calls));

            assertEquals(1, calls.get(), "normalize should be invoked once per value");
            assertEquals(result.getEventFieldValue(), result.getIndexedFieldValue(), "both fields should carry the same normalized text");
        }

        /**
         * Verify that an unparseable value records the error rather than propagating it.
         */
        @Test
        void givenAnUnparseableValueThenRecordsTheError() {
            AtomicInteger calls = new AtomicInteger();
            TestBaseIngestHelper helper = new TestBaseIngestHelper();

            NormalizedContentInterface result = helper.normalizeFieldValue(new NormalizedFieldAndValue("FIELD", "not a date"),
                            new datawave.data.type.DateType() {

                                private static final long serialVersionUID = 1L;

                                @Override
                                public String normalize(String in) {
                                    calls.incrementAndGet();
                                    return super.normalize(in);
                                }
                            });

            assertEquals(1, calls.get(), "a failing normalize should not be retried");
            assertTrue(result.getError() != null, "the failure should be recorded on the result");
        }
    }

    /**
     * Executes tests for {@link BaseIngestHelper#getDataTypes(String)} and {@link BaseIngestHelper#isDataTypeField(String)}.
     */
    @Nested
    class FieldTypeTests {

        private Configuration config;
        private TypeTestIngestHelper ingestHelper;

        @BeforeEach
        void setUp() {
            config = typeTestConfig();
            ingestHelper = new TypeTestIngestHelper();
        }

        /**
         * Set up the helper against {@link #FIELD_TYPE_CONFIG_FILE}.
         */
        private TypeTestIngestHelper helperWithFieldConfig() {
            config.set(DATA_TYPE_NAME + BaseIngestHelper.FIELD_CONFIG_FILE, FIELD_TYPE_CONFIG_FILE);
            ingestHelper.setup(config);
            return ingestHelper;
        }

        /**
         * Set up the helper and then apply the given field config XML to it.
         */
        private TypeTestIngestHelper helperWithFieldConfig(String xml) throws Exception {
            ingestHelper.setup(config);
            // constructing the config helper is what registers the declared types with the ingest helper
            new XMLFieldConfigHelper(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)), ingestHelper);
            return ingestHelper;
        }

        /**
         * Verify that a field declaring its own type resolves to that type.
         */
        @Test
        void givenFieldWithDeclaredTypeThenDeclaredTypeIsUsed() {
            assertSoleType(HexStringType.class, helperWithFieldConfig().getDataTypes("FOO"));
        }

        /**
         * Verify that a declared field with no type of its own resolves to the type from the {@code default} tag.
         */
        @Test
        void givenFieldWithNoDeclaredTypeThenDefaultTagTypeIsUsed() {
            assertSoleType(LcNoDiacriticsType.class, helperWithFieldConfig().getDataTypes("BAR"));
        }

        /**
         * Verify that a field matching a declared pattern resolves to the pattern's type.
         */
        @Test
        void givenFieldMatchingPatternThenPatternTypeIsUsed() {
            assertSoleType(DateType.class, helperWithFieldConfig().getDataTypes("SOME_DATE"));
        }

        /**
         * Verify that a field the config says nothing about resolves to the {@code nomatch} type, and keeps resolving to it on repeat calls. Prior to memoizing
         * the default-type fallback, this path re-walked the pattern list on every call.
         */
        @Test
        void givenUnknownFieldThenNoMatchTypeIsUsed() {
            TypeTestIngestHelper helper = helperWithFieldConfig();
            assertSoleType(NumberType.class, helper.getDataTypes("UNKNOWN"));
            assertSoleType(NumberType.class, helper.getDataTypes("UNKNOWN"));
        }

        /**
         * Verify that repeated lookups return the memoized result rather than resolving anew.
         */
        @Test
        void givenRepeatedLookupsThenMemoizedResultIsReturned() {
            TypeTestIngestHelper helper = helperWithFieldConfig();
            assertSame(helper.getDataTypes("FOO"), helper.getDataTypes("FOO"));
            assertSame(helper.getDataTypes("SOME_DATE"), helper.getDataTypes("SOME_DATE"));
            assertSame(helper.getDataTypes("UNKNOWN"), helper.getDataTypes("UNKNOWN"));
        }

        /**
         * Verify that a lower cased field name resolves to the same types as its upper cased form, for both an exact entry and a pattern match.
         */
        @Test
        void givenLowerCaseFieldNameThenSameTypesResolve() {
            TypeTestIngestHelper helper = helperWithFieldConfig();
            assertSoleType(HexStringType.class, helper.getDataTypes("foo"));
            assertSoleType(HexStringType.class, helper.getDataTypes("FOO"));
            assertSoleType(DateType.class, helper.getDataTypes("some_date"));
            assertSoleType(DateType.class, helper.getDataTypes("SOME_DATE"));
        }

        /**
         * Verify that lower cased names and patterns in a field config are reachable, i.e. that the types they declare are not silently ignored.
         */
        @Test
        void givenLowerCaseFieldConfigEntriesThenTypesResolve() throws Exception {
            String xml = "<?xml version=\"1.0\"?>\n" + "<fieldConfig>\n"
                            + "    <default stored=\"true\" indexed=\"true\" reverseIndexed=\"false\" tokenized=\"false\" reverseTokenized=\"false\" indexType=\"datawave.data.type.LcNoDiacriticsType\"/>\n"
                            + "    <nomatch stored=\"true\" indexed=\"false\" reverseIndexed=\"false\" tokenized=\"false\" reverseTokenized=\"false\" indexType=\"datawave.data.type.NumberType\"/>\n"
                            + "    <field name=\"lower\" indexType=\"datawave.data.type.HexStringType\"/>\n"
                            + "    <fieldPattern pattern=\"*_lowerdate\" indexType=\"datawave.data.type.DateType\"/>\n" + "</fieldConfig>";

            TypeTestIngestHelper helper = helperWithFieldConfig(xml);
            assertSoleType(HexStringType.class, helper.getDataTypes("LOWER"));
            assertSoleType(HexStringType.class, helper.getDataTypes("lower"));
            assertSoleType(DateType.class, helper.getDataTypes("SOME_LOWERDATE"));
            assertSoleType(DateType.class, helper.getDataTypes("some_lowerdate"));
        }

        /**
         * Verify that regex metacharacters in a field pattern survive registration. Upper casing the pattern text would invert character classes, e.g.
         * {@code \d} would become {@code \D}, and the pattern would match the wrong fields.
         */
        @Test
        void givenPatternWithCharacterClassThenClassIsNotInverted() {
            TypeTestIngestHelper helper = helperWithFieldConfig();
            helper.updateDatawaveTypes("FOO_\\d+", HexStringType.class.getName());

            assertSoleType(HexStringType.class, helper.getDataTypes("FOO_123"));
            // a non-numeric suffix must fall through to the nomatch type, not match an inverted \D+
            assertSoleType(NumberType.class, helper.getDataTypes("FOO_ABC"));
        }

        /**
         * Verify that a pattern carrying an inline flag still compiles. Upper casing the pattern text would turn {@code (?i)} into {@code (?I)}, which throws a
         * {@link java.util.regex.PatternSyntaxException} when the patterns are compiled.
         */
        @Test
        void givenPatternWithInlineFlagThenPatternCompiles() {
            TypeTestIngestHelper helper = helperWithFieldConfig();
            helper.updateDatawaveTypes("(?i)baz.*", HexStringType.class.getName());

            assertSoleType(HexStringType.class, helper.getDataTypes("BAZ_ONE"));
        }

        /**
         * Verify that a field with a configured type is reported as a data type field, whether the type came from an exact entry or a pattern, and regardless
         * of whether the types have already been resolved for that field.
         */
        @Test
        void givenConfiguredFieldThenIsDataTypeField() {
            TypeTestIngestHelper helper = helperWithFieldConfig();

            assertTrue(helper.isDataTypeField("FOO"));
            assertTrue(helper.isDataTypeField("SOME_DATE"));
            assertTrue(helper.isDataTypeField("some_date"));

            helper.getDataTypes("FOO");
            helper.getDataTypes("SOME_DATE");
            helper.getDataTypes("some_date");

            assertTrue(helper.isDataTypeField("FOO"));
            assertTrue(helper.isDataTypeField("SOME_DATE"));
            assertTrue(helper.isDataTypeField("some_date"));
        }

        /**
         * Verify that a field falling back to the default type is not reported as a data type field, before or after its types are resolved.
         */
        @Test
        void givenUnconfiguredFieldThenIsNotDataTypeField() {
            TypeTestIngestHelper helper = helperWithFieldConfig();

            assertFalse(helper.isDataTypeField("UNKNOWN"));
            helper.getDataTypes("UNKNOWN");
            assertFalse(helper.isDataTypeField("UNKNOWN"));
        }

        /**
         * Verify that types registered after a field has already been resolved take effect, rather than the stale resolution being served from the memo.
         */
        @Test
        void givenTypesRegisteredAfterResolutionThenMemoIsInvalidated() {
            TypeTestIngestHelper helper = helperWithFieldConfig();
            assertSoleType(NumberType.class, helper.getDataTypes("LATE"));
            assertFalse(helper.isDataTypeField("LATE"));

            helper.updateDatawaveTypes("LATE", HexStringType.class.getName());

            assertSoleType(HexStringType.class, helper.getDataTypes("LATE"));
            assertTrue(helper.isDataTypeField("LATE"));
        }

        /**
         * Verify that a second setup discards the types and resolutions from the first.
         */
        @Test
        void givenSecondSetupThenPriorResolutionsAreDiscarded() {
            TypeTestIngestHelper helper = helperWithFieldConfig();
            assertSoleType(HexStringType.class, helper.getDataTypes("FOO"));

            config.unset(DATA_TYPE_NAME + BaseIngestHelper.FIELD_CONFIG_FILE);
            helper.setup(config);

            // with the field config gone, FOO has no configured type and falls back to the NoOpType default
            assertFalse(helper.isDataTypeField("FOO"));
            assertSoleType(NoOpType.class, helper.getDataTypes("FOO"));
        }
    }

    /**
     * Covers the settings that choose the cache backing the resolved field types.
     */
    @Nested
    class FieldTypeCacheTests {

        private Configuration config;
        private TypeTestIngestHelper ingestHelper;

        @BeforeEach
        void setUp() {
            config = typeTestConfig();
            config.set(DATA_TYPE_NAME + BaseIngestHelper.FIELD_CONFIG_FILE, FIELD_TYPE_CONFIG_FILE);
            ingestHelper = new TypeTestIngestHelper();
        }

        /**
         * Verify that an unconfigured datatype keeps the historical unbounded cache.
         */
        @Test
        void givenNoSettingsThenCacheIsUnbounded() {
            ingestHelper.setup(config);
            assertEquals(FieldLookupCache.UNBOUNDED, ingestHelper.getTypeResolvedCache().getMaxSize());
        }

        /**
         * Verify that a bounded cache stays within its bound under either overflow policy while still resolving types correctly for the fields it did not keep
         * -- the ones a {@code BYPASS} cache never got to store, and the ones a {@code CLEAR} cache has since cleared away.
         */
        @ParameterizedTest
        @EnumSource(FieldLookupCache.OverflowPolicy.class)
        void givenBoundedSettingsThenCacheIsBounded(FieldLookupCache.OverflowPolicy policy) {
            config.set(DATA_TYPE_NAME + BaseIngestHelper.FIELD_TYPE_CACHE_MAX_SIZE, "2");
            config.set(DATA_TYPE_NAME + BaseIngestHelper.FIELD_TYPE_CACHE_OVERFLOW_POLICY, policy.name());
            ingestHelper.setup(config);

            assertEquals(2, ingestHelper.getTypeResolvedCache().getMaxSize());
            assertEquals(policy, ingestHelper.getTypeResolvedCache().getOverflowPolicy());

            for (String field : new String[] {"FOO", "BAR", "SOME_DATE", "UNKNOWN"}) {
                ingestHelper.getDataTypes(field);
                assertTrue(ingestHelper.getTypeResolvedCache().size() <= 2, "cache grew past its bound");
            }

            assertSoleType(HexStringType.class, ingestHelper.getDataTypes("FOO"));
            assertSoleType(LcNoDiacriticsType.class, ingestHelper.getDataTypes("BAR"));
            assertSoleType(DateType.class, ingestHelper.getDataTypes("SOME_DATE"));
            assertSoleType(NumberType.class, ingestHelper.getDataTypes("UNKNOWN"));
        }

        /**
         * Verify that the settings are read under the datatype this helper was set up for, so an {@code all} setting reaches it. The full precedence matrix
         * belongs to {@code FieldLookupCacheTest}; this only checks the wiring.
         */
        @Test
        void givenAllSettingThenItReachesTheCache() {
            config.set(TypeRegistry.ALL_PREFIX + BaseIngestHelper.FIELD_TYPE_CACHE_MAX_SIZE, "2");
            config.set(TypeRegistry.ALL_PREFIX + BaseIngestHelper.FIELD_TYPE_CACHE_OVERFLOW_POLICY, "CLEAR");
            ingestHelper.setup(config);

            assertEquals(2, ingestHelper.getTypeResolvedCache().getMaxSize());
            assertEquals(FieldLookupCache.OverflowPolicy.CLEAR, ingestHelper.getTypeResolvedCache().getOverflowPolicy());
        }

        /**
         * Verify that a second setup rebuilds the cache with the new bound rather than merely emptying the first one.
         */
        @Test
        void givenSecondSetupThenCacheImplementationIsSwapped() {
            ingestHelper.setup(config);
            assertEquals(FieldLookupCache.UNBOUNDED, ingestHelper.getTypeResolvedCache().getMaxSize());

            config.set(DATA_TYPE_NAME + BaseIngestHelper.FIELD_TYPE_CACHE_MAX_SIZE, "2");
            ingestHelper.setup(config);

            assertEquals(2, ingestHelper.getTypeResolvedCache().getMaxSize());
        }

        /**
         * Verify that an unusable cache setting fails setup rather than being silently ignored.
         */
        @Test
        void givenInvalidSettingsThenSetupThrows() {
            config.set(DATA_TYPE_NAME + BaseIngestHelper.FIELD_TYPE_CACHE_MAX_SIZE, "2");
            config.set(DATA_TYPE_NAME + BaseIngestHelper.FIELD_TYPE_CACHE_OVERFLOW_POLICY, "BOGUS");
            assertThrows(IllegalArgumentException.class, () -> ingestHelper.setup(config));

            config.set(DATA_TYPE_NAME + BaseIngestHelper.FIELD_TYPE_CACHE_OVERFLOW_POLICY, "BYPASS");
            config.set(DATA_TYPE_NAME + BaseIngestHelper.FIELD_TYPE_CACHE_MAX_SIZE, "0");
            assertThrows(IllegalArgumentException.class, () -> ingestHelper.setup(config));
        }
    }

    /**
     * A minimal concrete helper. Unlike {@link TestBaseIngestHelper}, it does not override {@link BaseIngestHelper#isDataTypeField(String)}.
     */
    public static class TypeTestIngestHelper extends BaseIngestHelper {

        @Override
        public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer event) {
            return null;
        }
    }
}
