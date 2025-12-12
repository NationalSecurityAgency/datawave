package datawave.ingest.mapreduce.handler.shard;

import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import datawave.data.hash.UID;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.RawRecordContainerImplTest;
import datawave.ingest.data.Type;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.data.config.ingest.CSVIngestHelper;
import datawave.ingest.data.hash.StringUID;
import datawave.util.time.DateHelper;

class FieldDateShardIdGeneratorTest {

    private static final String CSV = "csv";
    private static final UID AUID = new StringUID("12345.12345.12345");

    /**
     * Contains common tests that should be executed against the result of the constructor called in {@link #init()}.
     */
    abstract static class ConstructorTests {

        protected int baseNumShards;
        protected Date begin;
        protected Date end;
        protected List<FieldDateShardIdGenerator.FieldMapping> mappings;

        /**
         * Call the underlying target constructor.
         *
         * @return the initialized {@link FieldDateShardIdGenerator}
         */
        protected abstract FieldDateShardIdGenerator init();

        @AfterEach
        void tearDown() {
            baseNumShards = -1;
            begin = null;
            end = null;
            mappings = null;
        }

        /**
         * Test initializing a {@link FieldDateShardIdGenerator} with all null attributes.
         */
        @Test
        void testAllNullAttributes() {
            IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class, this::init);
            Assertions.assertEquals("Must configure at least one field mapping", exception.getMessage());
        }

        /**
         * Test initializing a {@link FieldDateShardIdGenerator} with no null attributes.
         */
        @Test
        void testAllNonNullAttributes() {
            this.baseNumShards = 10;
            this.begin = DateHelper.parse("20240115");
            this.end = DateHelper.parseTimeExactToSeconds("20240120235959");
            FieldDateShardIdGenerator.FieldMapping mapping1 = new FieldDateShardIdGenerator.FieldMapping(CSV, "field", "value", 10);
            this.mappings = Lists.newArrayList(mapping1);

            FieldDateShardIdGenerator generator = init();

            Assertions.assertEquals(mappings, generator.getMappings());
            Assertions.assertEquals(DateHelper.parse("20240115"), generator.getBegin());
            Assertions.assertEquals(DateHelper.parseTimeExactToSeconds("20240120235959"), generator.getEnd());
        }

        /**
         * Verify that when the begin date is after the end date, that an exception is thrown.
         */
        @Test
        void testBeginDateAfterEndDate() {
            this.begin = DateHelper.parse("20240115");
            this.end = DateHelper.parse("19990120");

            IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class, this::init);
            Assertions.assertTrue(exception.getMessage().startsWith("End date must be after begin date"));
        }
    }

    /**
     * Tests for {@link FieldDateShardIdGenerator#FieldDateShardIdGenerator(int, Date, Date, List)}.
     */
    @Nested
    class ExplicitArgsConstructorTests extends ConstructorTests {

        @Override
        protected FieldDateShardIdGenerator init() {
            return new FieldDateShardIdGenerator(baseNumShards, begin, end, mappings);
        }
    }

    /**
     * Tests for {@link FieldDateShardIdGenerator#FieldDateShardIdGenerator(Configuration, String)}.
     */
    @Nested
    class ConfigurationConstructorTests extends ConstructorTests {

        private final Joiner joiner = Joiner.on(',');

        @Override
        protected FieldDateShardIdGenerator init() {
            Configuration configuration = new Configuration();

            if (this.baseNumShards != -1) {
                configuration.set("generator.1." + FieldDateShardIdGenerator.BASENUMSHARDS, String.valueOf(baseNumShards));
            }
            if (this.begin != null) {
                configuration.set("generator.1." + FieldDateShardIdGenerator.BEGIN, DateHelper.formatToTimeExactToSeconds(this.begin));
            }
            if (this.end != null) {
                configuration.set("generator.1." + FieldDateShardIdGenerator.END, DateHelper.formatToTimeExactToSeconds(this.end));
            }
            if (this.mappings != null) {
                int index = 0;
                for (FieldDateShardIdGenerator.FieldMapping mapping : this.mappings) {
                    index++;
                    configuration.set("generator.1." + FieldDateShardIdGenerator.DATATYPE + '.' + index, mapping.getDatatype());
                    configuration.set("generator.1." + FieldDateShardIdGenerator.FIELD + '.' + index, mapping.getField());
                    configuration.set("generator.1." + FieldDateShardIdGenerator.REGEX + '.' + index, mapping.getRegex());
                    configuration.set("generator.1." + FieldDateShardIdGenerator.SHARDS + '.' + index, String.valueOf(mapping.getShards()));
                }
            }

            return new FieldDateShardIdGenerator(configuration, "generator.1");
        }

    }

    /**
     * Verify that for a {@link FieldDateShardIdGenerator} with all null attributes,
     * {@link FieldDateShardIdGenerator#isApplicable(RawRecordContainer, Multimap)} always returns true.
     */
    @Test
    void testFailsConstructionWithAllNullAttributes() {
        RawRecordContainer record = createRecord("20200101");

        Assertions.assertThrows(IllegalArgumentException.class, () -> new FieldDateShardIdGenerator(-1, null, null, null));
    }

    /**
     * Verify that {@link FieldDateShardIdGenerator#isApplicable(RawRecordContainer, Multimap)} returns false if it has a non-empty set of data types, and null
     * dates, and the record does not have a matching data type.
     */
    @Test
    void testIsApplicableWithNullDates() {
        RawRecordContainer record = createRecord("20200101");

        FieldDateShardIdGenerator.FieldMapping mapping1 = new FieldDateShardIdGenerator.FieldMapping(CSV, "field", "value", 10);
        FieldDateShardIdGenerator generator = new FieldDateShardIdGenerator(100, null, null, Lists.newArrayList(mapping1));

        Assertions.assertTrue(generator.isApplicable(record, createMultimap("field:value", "f2:v2")));
    }

    /**
     * Verify that {@link FieldDateShardIdGenerator#isApplicable(RawRecordContainer, Multimap)} returns true if it has an empty set of data types, a non-null
     * begin date, and a null end dates, and the record has a date equal to or after the begin date.
     */
    @Test
    void testIsApplicableWithDateWithinRangeWithNonNullBeginDate() {
        RawRecordContainer record = createRecord("20200115");

        FieldDateShardIdGenerator.FieldMapping mapping1 = new FieldDateShardIdGenerator.FieldMapping(CSV, "field", "value", 10);
        FieldDateShardIdGenerator generator = new FieldDateShardIdGenerator(100, DateHelper.parse("20200101"), null, Lists.newArrayList(mapping1));

        Assertions.assertTrue(generator.isApplicable(record, createMultimap("field:value", "f2:v2")));
    }

    /**
     * Verify that {@link FieldDateShardIdGenerator#isApplicable(RawRecordContainer, Multimap)} returns false if it has an empty set of data types, a non-null
     * begin date, and a null end dates, and the record has a date before the begin date.
     */
    @Test
    void testIsApplicableWithDateOutsideRangeWithNonNullBeginDate() {
        RawRecordContainer record = createRecord("19990110");

        FieldDateShardIdGenerator.FieldMapping mapping1 = new FieldDateShardIdGenerator.FieldMapping(CSV, "field", "value", 10);
        FieldDateShardIdGenerator generator = new FieldDateShardIdGenerator(100, DateHelper.parse("20200101"), null, Lists.newArrayList(mapping1));

        Assertions.assertFalse(generator.isApplicable(record, createMultimap("field:value", "f2:v2")));
    }

    /**
     * Verify that {@link FieldDateShardIdGenerator#isApplicable(RawRecordContainer, Multimap)} returns true if it has an empty set of data types, a null begin
     * date, and a non-null end dates, and the record has a date equal to or before the end date.
     */
    @Test
    void testIsApplicableWithDateWithinRangeWithNonNullEndDate() {
        RawRecordContainer record = createRecord("20200115");

        FieldDateShardIdGenerator.FieldMapping mapping1 = new FieldDateShardIdGenerator.FieldMapping(CSV, "field", "value", 10);
        FieldDateShardIdGenerator generator = new FieldDateShardIdGenerator(100, null, DateHelper.parse("20240111"), Lists.newArrayList(mapping1));

        Assertions.assertTrue(generator.isApplicable(record, createMultimap("field:value", "f2:v2")));
    }

    /**
     * Verify that {@link FieldDateShardIdGenerator#isApplicable(RawRecordContainer, Multimap)} returns false if it has an empty set of data types, a null begin
     * date, and a non-null end dates, and the record has a date after the end date.
     */
    @Test
    void testIsApplicableWithDateOutsideRangeWithNonNullEndDate() {
        RawRecordContainer record = createRecord("20240111");

        FieldDateShardIdGenerator.FieldMapping mapping1 = new FieldDateShardIdGenerator.FieldMapping(CSV, "field", "value", 10);
        FieldDateShardIdGenerator generator = new FieldDateShardIdGenerator(100, null, DateHelper.parse("20200101"), Lists.newArrayList(mapping1));

        Assertions.assertFalse(generator.isApplicable(record, createMultimap("field:value", "f2:v2")));
    }

    /**
     * Verify that {@link FieldDateShardIdGenerator#isApplicable(RawRecordContainer, Multimap)} returns true if it has an empty set of data types, a non-null
     * begin date, and a non-null end dates, and the record has a date that falls within the date range.
     */
    @Test
    void testIsApplicableWithDateWithinRangeWithNonNullDates() {
        RawRecordContainer record = createRecord("20220111");

        FieldDateShardIdGenerator.FieldMapping mapping1 = new FieldDateShardIdGenerator.FieldMapping(CSV, "field", "value", 10);
        FieldDateShardIdGenerator generator = new FieldDateShardIdGenerator(100, DateHelper.parse("20200101"), DateHelper.parse("20240101"),
                        Lists.newArrayList(mapping1));

        Assertions.assertTrue(generator.isApplicable(record, createMultimap("field:value", "f2:v2")));
    }

    /**
     * Verify that the begin of the date range is inclusive.
     */
    @Test
    void testIsApplicableBeginDateInclusive() {
        RawRecordContainer record = createRecord("20200101");

        FieldDateShardIdGenerator.FieldMapping mapping1 = new FieldDateShardIdGenerator.FieldMapping(CSV, "field", "value", 10);
        FieldDateShardIdGenerator generator = new FieldDateShardIdGenerator(100, DateHelper.parse("20200101"), DateHelper.parse("20240101"),
                        Lists.newArrayList(mapping1));

        Assertions.assertTrue(generator.isApplicable(record, createMultimap("field:value", "f2:v2")));
    }

    /**
     * Verify that the end of the date range is inclusive.
     */
    @Test
    void testIsApplicableEndDateInclusive() {
        RawRecordContainer record = createRecord("20240101");

        FieldDateShardIdGenerator.FieldMapping mapping1 = new FieldDateShardIdGenerator.FieldMapping(CSV, "field", "value", 10);
        FieldDateShardIdGenerator generator = new FieldDateShardIdGenerator(100, DateHelper.parse("20200101"), DateHelper.parse("20240101"),
                        Lists.newArrayList(mapping1));

        Assertions.assertTrue(generator.isApplicable(record, createMultimap("field:value", "f2:v2")));
    }

    /**
     * Verify the behavior of {@link FieldDateShardIdGenerator#getShardId(RawRecordContainer, Multimap, String, int)}.
     */
    @Test
    void testGetShardId() {
        RawRecordContainer record = createRecord("20240101");
        FieldDateShardIdGenerator.FieldMapping mapping1 = new FieldDateShardIdGenerator.FieldMapping(CSV, "field", "value", 10);
        FieldDateShardIdGenerator generator = new FieldDateShardIdGenerator(100, DateHelper.parse("20200101"), DateHelper.parse("20240101"),
                        Lists.newArrayList(mapping1));
        String baseShardId = ShardIdFactory.getBaseShardId(record, 100);
        String shardId = generator.getShardId(record, createMultimap("field:value", "f2:v2"), baseShardId, 10);
        Assertions.assertEquals("20240101_103", shardId);
    }

    /**
     * Verify the behavior of {@link FieldDateShardIdGenerator#getShardId(RawRecordContainer, Multimap, String, int)}.
     */
    @Test
    void testGetBaseShardId() {
        RawRecordContainer record = createRecord("20240101");
        FieldDateShardIdGenerator.FieldMapping mapping1 = new FieldDateShardIdGenerator.FieldMapping(CSV, "field", "wrongvalue", 10);
        FieldDateShardIdGenerator generator = new FieldDateShardIdGenerator(100, DateHelper.parse("20200101"), DateHelper.parse("20240101"),
                        Lists.newArrayList(mapping1));
        String baseShardId = ShardIdFactory.getBaseShardId(record, 100);
        String shardId = generator.getShardId(record, createMultimap("field:value", "f2:v2"), baseShardId, 10);
        Assertions.assertEquals(baseShardId, shardId);
    }

    private RawRecordContainer createRecord(String dateStr) {
        Type dataType = new Type(CSV, CSVIngestHelper.class, null, null, 0, null);
        Date date = DateHelper.parse(dateStr);
        RawRecordContainerImplTest.ValidatingRawRecordContainerImpl event = new RawRecordContainerImplTest.ValidatingRawRecordContainerImpl();
        event.setTimestamp(date.getTime());
        event.setDataType(dataType);
        event.setId(AUID);
        return event;
    }

    private Multimap<String,NormalizedContentInterface> createMultimap(String... fieldValues) {
        Multimap<String,NormalizedContentInterface> map = HashMultimap.create();
        for (String fieldValue : fieldValues) {
            String[] parts = fieldValue.split(":");
            map.put(parts[0], new NormalizedFieldAndValue(parts[0], parts[1]));
        }
        return map;
    }

}
