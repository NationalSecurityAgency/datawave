package datawave.query.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import datawave.data.type.DateType;
import datawave.data.type.LcNoDiacriticsListType;
import datawave.ingest.mapreduce.handler.dateindex.DateIndexUtil;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.QueryParameters;
import datawave.query.attributes.TemporalGranularity;
import datawave.query.attributes.UniqueFields;
import datawave.query.common.grouping.GroupFields;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.exceptions.DatawaveQueryException;
import datawave.query.exceptions.NoResultsException;
import datawave.query.iterator.QueryOptions;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MockDateIndexHelper;
import datawave.query.util.TypeMetadata;
import datawave.query.util.TypeMetadataSerializer;
import datawave.test.JexlNodeAssert;
import datawave.util.time.DateHelper;

class DefaultQueryPlannerTest {

    /**
     * Contains tests for
     * {@link DefaultQueryPlanner#addDateFilters(ASTJexlScript, ScannerFactory, MetadataHelper, DateIndexHelper, ShardQueryConfiguration, Query)}
     */
    @Nested
    class DateFilterTests {

        private final SimpleDateFormat filterFormat = new SimpleDateFormat("yyyyMMdd:HH:mm:ss:SSSZ");

        private DefaultQueryPlanner planner;
        private ShardQueryConfiguration config;
        private QueryImpl settings;
        private MockDateIndexHelper dateIndexHelper;
        private ASTJexlScript queryTree;

        @BeforeEach
        void setUp() {
            planner = new DefaultQueryPlanner();
            config = new ShardQueryConfiguration();
            settings = new QueryImpl();
            dateIndexHelper = new MockDateIndexHelper();
        }

        /**
         * Verify that when the date type is the default date type, and is part of the noExpansionIfCurrentDateTypes types, and the query's end date is the
         * current date, that no date filters are added and SHARDS_AND_DAYS hints are forbidden.
         */
        @Test
        void testDefaultDateTypeMarkedForNoExpansionAndEndDateIsCurrDate() throws Exception {
            queryTree = JexlASTHelper.parseJexlQuery("FOO == 'bar'");
            config.setDefaultDateTypeName("EVENT");
            config.setNoExpansionIfCurrentDateTypes(Set.of("EVENT"));

            Date beginDate = DateHelper.parse("20241001");
            config.setBeginDate(beginDate);
            Date endDate = new Date();
            config.setEndDate(endDate);

            ASTJexlScript actual = addDateFilters();

            // no hints or date filter required in this case
            JexlNodeAssert.assertThat(actual).isEqualTo("FOO == 'bar'");
            assertEquals(beginDate, config.getBeginDate());
            assertEquals(endDate, config.getEndDate());
        }

        /**
         * Verify that when a date type is given via parameters that is part of the noExpansionIfCurrentDateTypes types, and the query's end date is the current
         * date, that no date filters are added and SHARDS_AND_DAYS hints are forbidden.
         */
        @Test
        void testParamDateTypeMarkedForNoExpansionAndEndDateIsCurrDate() throws Exception {
            queryTree = JexlASTHelper.parseJexlQuery("FOO == 'bar'");
            config.setDefaultDateTypeName("EVENT");
            config.setNoExpansionIfCurrentDateTypes(Set.of("SPECIAL_EVENT"));

            Date beginDate = DateHelper.parse("20241001");
            config.setBeginDate(beginDate);
            Date endDate = new Date();
            config.setEndDate(endDate);

            settings.addParameter(QueryParameters.DATE_RANGE_TYPE, "SPECIAL_EVENT");
            dateIndexHelper.addEntry("20241201", "SPECIAL_EVENT", "wiki", "FOO", "20240101_shard");
            dateIndexHelper.addEntry("20250101", "SPECIAL_EVENT", "wiki", "FOO", "20250101_shard");

            ASTJexlScript actual = addDateFilters();

            // no hints but the date filter is still used
            JexlNodeAssert.assertThat(actual).isEqualTo(
                            "(FOO == 'bar') && filter:betweenDates(FOO, '" + filterFormat.format(beginDate) + "', '" + filterFormat.format(endDate) + "')");
            // begin date is not pushed farther back
            assertEquals(DateIndexUtil.getBeginDate("20241001"), config.getBeginDate());
            // end date not pushed farther back either
            assertEquals(endDate, config.getEndDate());
        }

        /**
         * Verify that when the date type is the default date type, and is part of the noExpansionIfCurrentDateTypes types, but the query's end date is not the
         * current date, that no date filters are added and SHARDS_AND_DAYS hints are allowed.
         */
        @Test
        void testDefaultDateTypeMarkedForNoExpansionAndEndDateIsNotCurrDate() throws Exception {
            queryTree = JexlASTHelper.parseJexlQuery("FOO == 'bar'");
            config.setDefaultDateTypeName("EVENT");
            config.setNoExpansionIfCurrentDateTypes(Set.of("EVENT"));

            Date beginDate = DateHelper.parse("20241001");
            config.setBeginDate(beginDate);
            Date endDate = DateHelper.parse("20241010");
            config.setEndDate(endDate);

            ASTJexlScript actual = addDateFilters();

            // no hints or date filter required in this case
            JexlNodeAssert.assertThat(actual).isEqualTo("FOO == 'bar'");
            assertEquals(beginDate, config.getBeginDate());
            assertEquals(endDate, config.getEndDate());
        }

        /**
         * Verify that when a date type is given via parameters that is part of the noExpansionIfCurrentDateTypes types, but the query's end date is not the
         * current date, that date filters are added and SHARDS_AND_DAYS hints are allowed.
         */
        @Test
        void testParamDateTypeMarkedForNoExpansionAndEndDateIsNotCurrDate() throws Exception {
            queryTree = JexlASTHelper.parseJexlQuery("FOO == 'bar'");
            config.setDefaultDateTypeName("EVENT");
            config.setNoExpansionIfCurrentDateTypes(Set.of("SPECIAL_EVENT"));
            Date beginDate = DateHelper.parse("20241009");
            config.setBeginDate(beginDate);
            Date endDate = DateHelper.parse("20241011");
            config.setEndDate(endDate);
            settings.addParameter(QueryParameters.DATE_RANGE_TYPE, "SPECIAL_EVENT");
            dateIndexHelper.addEntry("20241010", "SPECIAL_EVENT", "wiki", "FOO", "20241010_shard");

            ASTJexlScript actual = addDateFilters();

            // hints and date filter used in this case
            JexlNodeAssert.assertThat(actual).hasExactQueryString(
                            "(FOO == 'bar') && filter:betweenDates(FOO, '" + filterFormat.format(beginDate) + "', '" + filterFormat.format(endDate) + "')");
            // only the end date is adjusted
            assertEquals(beginDate, config.getBeginDate());
            assertEquals(DateIndexUtil.getEndDate("20241010"), config.getEndDate());
        }

        /**
         * Verify that when the date type is the default date type, and is not part of the noExpansionIfCurrentDateTypes types, and the query's end date is the
         * current date, that no date filters are added and SHARDS_AND_DAYS hints are allowed.
         */
        @Test
        void testDefaultDateTypeIsNotMarkedForNoExpansionAndEndDateNotCurrDate() throws Exception {
            queryTree = JexlASTHelper.parseJexlQuery("FOO == 'bar'");
            config.setDefaultDateTypeName("EVENT");
            config.setNoExpansionIfCurrentDateTypes(Set.of("OTHER_EVENT"));

            Date beginDate = DateHelper.parse("20241001");
            config.setBeginDate(beginDate);
            Date endDate = DateHelper.parse("20241010");
            config.setEndDate(endDate);

            ASTJexlScript actual = addDateFilters();

            // no hints or date filter required in this case
            JexlNodeAssert.assertThat(actual).isEqualTo("FOO == 'bar'");
            assertEquals(beginDate, config.getBeginDate());
            assertEquals(endDate, config.getEndDate());
        }

        /**
         * Verify that when a date type is given via parameters that is not part of the noExpansionIfCurrentDateTypes types, and the query's end date is the
         * current date, that date filters are added and SHARDS_AND_DAYS hints are allowed.
         */
        @Test
        void testParamDateTypeIsNotMarkedForNoExpansionAndEndDateIsCurrDate() throws Exception {
            queryTree = JexlASTHelper.parseJexlQuery("FOO == 'bar'");
            config.setDefaultDateTypeName("EVENT");
            config.setNoExpansionIfCurrentDateTypes(Set.of("OTHER_EVENT"));
            config.setBeginDate(DateHelper.parse("20241009"));
            Date beginDate = DateHelper.parse("20241001");
            config.setBeginDate(beginDate);
            Date endDate = new Date();
            config.setEndDate(endDate);

            settings.addParameter(QueryParameters.DATE_RANGE_TYPE, "SPECIAL_EVENT");
            dateIndexHelper.addEntry("20241010", "SPECIAL_EVENT", "wiki", "FOO", "20241010_shard");

            ASTJexlScript actual = addDateFilters();

            // hints and date filter used in this case
            JexlNodeAssert.assertThat(actual).hasExactQueryString(
                            "(FOO == 'bar') && filter:betweenDates(FOO, '" + filterFormat.format(beginDate) + "', '" + filterFormat.format(endDate) + "')");
            assertEquals(DateIndexUtil.getBeginDate("20241010"), config.getBeginDate());
            assertEquals(DateIndexUtil.getEndDate("20241010"), config.getEndDate());
        }

        /**
         * Verify that when the date type is in noExpansionIfCurrentDateTypes and the query end date is not today, but the remapped shard end date falls before
         * the query begin date, a NoResultsException is thrown rather than creating an invalid date range.
         */
        @Test
        void testParamDateTypeMarkedForNoExpansionRemappedEndDateBeforeBeginDate() throws Exception {
            queryTree = JexlASTHelper.parseJexlQuery("FOO == 'bar'");
            config.setDefaultDateTypeName("EVENT");
            config.setNoExpansionIfCurrentDateTypes(Set.of("SPECIAL_EVENT"));
            // Query begin date is 2024-01-01; end date is not today
            Date beginDate = DateHelper.parse("20240101");
            config.setBeginDate(beginDate);
            config.setEndDate(DateHelper.parse("20240131"));
            settings.addParameter(QueryParameters.DATE_RANGE_TYPE, "SPECIAL_EVENT");
            // Date index shard dates are all in December 2023 — entirely before the query begin date
            dateIndexHelper.addEntry("20231201", "SPECIAL_EVENT", "wiki", "FOO", "20231201_shard");
            dateIndexHelper.addEntry("20231215", "SPECIAL_EVENT", "wiki", "FOO", "20231215_shard");

            assertThrows(NoResultsException.class, this::addDateFilters);
        }

        private ASTJexlScript addDateFilters() throws TableNotFoundException, DatawaveQueryException {
            return planner.addDateFilters(queryTree, null, null, dateIndexHelper, config, settings);
        }

        /**
         * Verify that no exception is thrown when validating a {@link UniqueFields} instance with temporal granularities for datetime fields.
         */
        @Test
        void testValidateUniqueFieldsGivenValidFields() {
            TypeMetadata metadata = new TypeMetadata();
            metadata.put("NAME", "hr", LcNoDiacriticsListType.class.getName());
            metadata.put("ROLE", "hr", LcNoDiacriticsListType.class.getName());
            metadata.put("HIRE_DATE", "hr", DateType.class.getName());
            planner.setTypeMetadata(metadata);

            UniqueFields uniqueFields = new UniqueFields();
            uniqueFields.put("NAME", TemporalGranularity.ALL);
            uniqueFields.put("ROLE", TemporalGranularity.ALL);
            uniqueFields.put("HIRE_DATE", TemporalGranularity.TRUNCATE_TEMPORAL_TO_DAY);

            planner.validateUniqueFields(uniqueFields);
        }

        /**
         * Verify that an exception is thrown when validating a {@link UniqueFields} instance with temporal granularities for non-datetime fields.
         */
        @Test
        void testValidateUniqueFieldsGivenInvalidFields() {
            TypeMetadata metadata = new TypeMetadata();
            metadata.put("NAME", "hr", LcNoDiacriticsListType.class.getName());
            metadata.put("ROLE", "hr", LcNoDiacriticsListType.class.getName());
            metadata.put("HIRE_DATE", "hr", DateType.class.getName());
            planner.setTypeMetadata(metadata);

            UniqueFields uniqueFields = new UniqueFields();
            uniqueFields.put("NAME", TemporalGranularity.ALL);
            uniqueFields.put("ROLE", TemporalGranularity.TRUNCATE_TEMPORAL_TO_DAY);
            uniqueFields.put("HIRE_DATE", TemporalGranularity.TRUNCATE_TEMPORAL_TO_DAY);

            assertThrows(DatawaveFatalQueryException.class, () -> planner.validateUniqueFields(uniqueFields),
                            "The following unique fields are not date fields and cannot be used with UNIQUE_BY_X: ROLE");
        }

        /**
         * Verify that no exception is thrown when validating a {@link GroupFields} instance with temporal granularities for datetime fields.
         */
        @Test
        void testValidateGroupFieldsGivenValidFields() {
            TypeMetadata metadata = new TypeMetadata();
            metadata.put("NAME", "hr", LcNoDiacriticsListType.class.getName());
            metadata.put("ROLE", "hr", LcNoDiacriticsListType.class.getName());
            metadata.put("HIRE_DATE", "hr", DateType.class.getName());
            planner.setTypeMetadata(metadata);

            GroupFields groupFields = GroupFields.from("NAME,ROLE,HIRE_DATE[DAY]");

            planner.validateGroupFields(groupFields);
        }

        /**
         * Verify that an exception is thrown when validating a {@link GroupFields} instance with temporal granularities for non-datetime fields.
         */
        @Test
        void testValidateGroupFieldsGivenInvalidFields() {
            TypeMetadata metadata = new TypeMetadata();
            metadata.put("NAME", "hr", LcNoDiacriticsListType.class.getName());
            metadata.put("ROLE", "hr", LcNoDiacriticsListType.class.getName());
            metadata.put("HIRE_DATE", "hr", DateType.class.getName());
            planner.setTypeMetadata(metadata);

            GroupFields groupFields = GroupFields.from("NAME,ROLE[DAY],HIRE_DATE[DAY]");

            assertThrows(DatawaveFatalQueryException.class, () -> planner.validateGroupFields(groupFields),
                            "The following group-by fields are not date fields and cannot be used with temporal truncation: ROLE");
        }
    }

    /**
     * Contains tests for {@link DefaultQueryPlanner#capDateRange(ShardQueryConfiguration)}
     */
    @Nested
    class CapDateRangeTests {

        private static final long THIRTY_DAYS_MS = 30L * 24 * 60 * 60 * 1000;

        private DefaultQueryPlanner planner;
        private ShardQueryConfiguration config;

        @BeforeEach
        void setUp() {
            planner = new DefaultQueryPlanner();
            config = new ShardQueryConfiguration();
        }

        /**
         * Verify that when both begin and end dates are before the cap and failOutsideValidDateRange is false, a NoResultsException is thrown rather than
         * setting endDate before beginDate.
         */
        @Test
        void testBothDatesBeforeCapSoftFailThrowsNoResults() {
            config.setBeginDateCap(THIRTY_DAYS_MS);
            config.setFailOutsideValidDateRange(false);
            config.setBeginDate(new Date(System.currentTimeMillis() - 60 * 24 * 60 * 60 * 1000L)); // 60 days ago
            config.setEndDate(new Date(System.currentTimeMillis() - 45 * 24 * 60 * 60 * 1000L)); // 45 days ago

            assertThrows(NoResultsException.class, () -> planner.capDateRange(config));
        }

        /**
         * Verify that when both begin and end dates are before the cap and failOutsideValidDateRange is true, a DatawaveQueryException is thrown.
         */
        @Test
        void testBothDatesBeforeCapHardFailThrowsQueryException() {
            config.setBeginDateCap(THIRTY_DAYS_MS);
            config.setFailOutsideValidDateRange(true);
            config.setBeginDate(new Date(System.currentTimeMillis() - 60 * 24 * 60 * 60 * 1000L));
            config.setEndDate(new Date(System.currentTimeMillis() - 45 * 24 * 60 * 60 * 1000L));

            assertThrows(DatawaveQueryException.class, () -> planner.capDateRange(config));
        }

        /**
         * Verify that when only the begin date is before the cap (end date is within range), beginDate is capped and endDate is unchanged.
         */
        @Test
        void testOnlyBeginDateBeforeCapUpdatesBeginDate() throws DatawaveQueryException {
            config.setBeginDateCap(THIRTY_DAYS_MS);
            config.setFailOutsideValidDateRange(false);
            long minStartTime = System.currentTimeMillis() - THIRTY_DAYS_MS;
            config.setBeginDate(new Date(minStartTime - 1000)); // just before cap
            Date endDate = new Date(); // now — well within cap
            config.setEndDate(endDate);

            planner.capDateRange(config);

            assertTrue(!config.getEndDate().before(config.getBeginDate()), "endDate must not be before beginDate after cap");
            assertEquals(endDate, config.getEndDate(), "endDate should be unchanged");
        }

        /**
         * Verify that when both dates are within the cap, neither date is modified.
         */
        @Test
        void testDatesWithinCapAreUnchanged() throws DatawaveQueryException {
            config.setBeginDateCap(THIRTY_DAYS_MS);
            Date beginDate = new Date(System.currentTimeMillis() - 10 * 24 * 60 * 60 * 1000L); // 10 days ago
            Date endDate = new Date();
            config.setBeginDate(beginDate);
            config.setEndDate(endDate);

            planner.capDateRange(config);

            assertEquals(beginDate, config.getBeginDate());
            assertEquals(endDate, config.getEndDate());
        }
    }

    /**
     * Contains tests for {@link DefaultQueryPlanner#configureTypeMappings(ShardQueryConfiguration, IteratorSetting, MetadataHelper, boolean, boolean)}
     */
    @Nested
    class ConfigureTypeMappingsTests {

        private DefaultQueryPlanner planner;
        private ShardQueryConfiguration config;
        private MetadataHelper metadataHelper;
        private TypeMetadata typeMetadata;

        @BeforeEach
        void setUp() {
            planner = new DefaultQueryPlanner();
            config = new ShardQueryConfiguration();
            metadataHelper = Mockito.mock(MetadataHelper.class);
            Mockito.when(metadataHelper.getUsersMetadataAuthorizationSubset()).thenReturn("");

            typeMetadata = new TypeMetadata();
            typeMetadata.put("FIELD1", "ingestA", "LcNoDiacriticsType");
            typeMetadata.put("FIELD2", "ingestA", "NumberType");
            planner.setTypeMetadata(typeMetadata);
        }

        /**
         * Verify that when {@code kryoTypeMetadata} is disabled (the default), TYPE_METADATA is serialized via {@link TypeMetadata#toString()} and the
         * TYPE_METADATA_KRYO marker option is false.
         */
        @Test
        void testKryoTypeMetadataDisabledUsesNativeStringFormat() throws Exception {
            IteratorSetting cfg = new IteratorSetting(100, "test", "test");

            planner.configureTypeMappings(config, cfg, metadataHelper, false);

            Map<String,String> options = cfg.getOptions();
            assertFalse(Boolean.parseBoolean(options.get(QueryOptions.TYPE_METADATA_KRYO)));
            assertEquals(typeMetadata.toString(), options.get(QueryOptions.TYPE_METADATA));
        }

        /**
         * Verify that when {@code kryoTypeMetadata} is enabled, TYPE_METADATA is serialized via {@link TypeMetadataSerializer} and the TYPE_METADATA_KRYO
         * marker option is true, and that the result decodes back to the original TypeMetadata.
         */
        @Test
        void testKryoTypeMetadataEnabledUsesKryoFormat() throws Exception {
            config.setKryoTypeMetadata(true);
            IteratorSetting cfg = new IteratorSetting(100, "test", "test");

            planner.configureTypeMappings(config, cfg, metadataHelper, false);

            Map<String,String> options = cfg.getOptions();
            assertTrue(Boolean.parseBoolean(options.get(QueryOptions.TYPE_METADATA_KRYO)));
            assertEquals(typeMetadata, new TypeMetadataSerializer().deserialize(options.get(QueryOptions.TYPE_METADATA)));
        }

        /**
         * Verify that {@code kryoTypeMetadata} is ignored (falls back to the native string format) when per-shard type metadata reduction is enabled, since
         * VisitorFunction re-serializes TypeMetadata via toString() after reducing it per shard and does not understand the Kryo format.
         */
        @Test
        void testKryoTypeMetadataIgnoredWhenReducingPerShard() throws Exception {
            config.setKryoTypeMetadata(true);
            // canReduceTypeMetadata requires a non-empty project/disallowlist field set, otherwise configureTypeMappings
            // forces reduceTypeMetadataPerShard back to false before it ever reaches the serialization branch below
            config.setProjectFields(Set.of("FIELD1"));
            config.setQueryTree(JexlASTHelper.parseJexlQuery("FIELD1 == 'bar'"));
            config.setReduceTypeMetadataPerShard(true);
            IteratorSetting cfg = new IteratorSetting(100, "test", "test");

            planner.configureTypeMappings(config, cfg, metadataHelper, false);

            // the planner still performs a first-pass reduction to FIELD1 regardless of the kryo flag; only the
            // serialization format (native toString(), not Kryo) is under test here
            Map<String,String> options = cfg.getOptions();
            assertFalse(Boolean.parseBoolean(options.get(QueryOptions.TYPE_METADATA_KRYO)));
            assertEquals(typeMetadata.reduce(Set.of("FIELD1")).toString(), options.get(QueryOptions.TYPE_METADATA));
        }
    }
}
