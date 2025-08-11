package datawave.ingest.mapreduce.handler.shard;

import java.util.Date;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.google.common.base.Joiner;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.RawRecordContainerImplTest;
import datawave.ingest.data.Type;
import datawave.ingest.data.config.ingest.CSVIngestHelper;
import datawave.util.time.DateHelper;

class ShiftOnDayTest {

    private static final String CSV = "csv";

    /**
     * Contains common tests that should be executed against the result of the constructor called in {@link #init()}.
     */
    abstract static class ConstructorTests {

        protected Set<String> dataTypes;
        protected Date begin;
        protected Date end;

        /**
         * Call the underlying target constructor.
         *
         * @return the initialized {@link ShiftOnDay}
         */
        protected abstract ShiftOnDay init();

        @AfterEach
        void tearDown() {
            dataTypes = null;
            begin = null;
            end = null;
        }

        /**
         * Test initializing a {@link ShiftOnDay} with all null attributes.
         */
        @Test
        void testAllNullAttributes() {
            ShiftOnDay generator = init();

            Assertions.assertTrue(generator.getDataTypes().isEmpty());
            Assertions.assertNull(generator.getBegin());
            Assertions.assertNull(generator.getEnd());
        }

        /**
         * Test initializing a {@link ShiftOnDay} with no null attributes.
         */
        @Test
        void testAllNonNullAttributes() {
            this.dataTypes = Set.of("a", "b", "c");
            this.begin = DateHelper.parse("20240115");
            this.end = DateHelper.parse("20240120");

            ShiftOnDay generator = init();

            Assertions.assertEquals(Set.of("a", "b", "c"), generator.getDataTypes());
            Assertions.assertEquals(DateHelper.parse("20240115"), generator.getBegin());
            Assertions.assertEquals(DateHelper.parse("20240120"), generator.getEnd());
        }

        /**
         * Verify that when the begin date is after the end date, that an exception is thrown.
         */
        @Test
        void testBeginDateAfterEndDate() {
            this.begin = DateHelper.parse("20240115");
            this.end = DateHelper.parse("19990120");

            IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class, this::init);
            Assertions.assertEquals("End date must be after begin date", exception.getMessage());
        }
    }

    /**
     * Tests for {@link ShiftOnDay#ShiftOnDay(Set, Date, Date)}.
     */
    @Nested
    class ExplicitArgsConstructorTests extends ConstructorTests {

        @Override
        protected ShiftOnDay init() {
            return new ShiftOnDay(dataTypes, begin, end);
        }
    }

    /**
     * Tests for {@link ShiftOnDay#ShiftOnDay(Configuration, String)}.
     */
    @Nested
    class ConfigurationConstructorTests extends ConstructorTests {

        private final Joiner joiner = Joiner.on(',');

        @Override
        protected ShiftOnDay init() {
            Configuration configuration = new Configuration();
            if (this.dataTypes != null) {
                String dataTypesStr = joiner.join(this.dataTypes);
                configuration.set("generator." + ShiftOnDay.DATATYPES, dataTypesStr);
            }
            if (this.begin != null) {
                configuration.set("generator." + ShiftOnDay.BEGIN, DateHelper.format(this.begin));
            }
            if (this.end != null) {
                configuration.set("generator." + ShiftOnDay.END, DateHelper.format(this.end));
            }

            return new ShiftOnDay(configuration, "generator");
        }

    }

    /**
     * Verify that for a {@link ShiftOnDay} with all null attributes, {@link ShiftOnDay#isApplicable(RawRecordContainer)} always returns true.
     */
    @Test
    void testIsApplicableWithAllNullAttributes() {
        RawRecordContainer record = createRecord("20200101");

        ShiftOnDay generator = new ShiftOnDay(null, null, null);

        Assertions.assertTrue(generator.isApplicable(record));
    }

    /**
     * Verify that {@link ShiftOnDay#isApplicable(RawRecordContainer)} returns false if it has a non-empty set of data types, and null dates, and the record
     * does not have a matching data type.
     */
    @Test
    void testIsApplicableWithNonMatchingDataTypeWithNullDates() {
        RawRecordContainer record = createRecord("20200101");

        ShiftOnDay generator = new ShiftOnDay(Set.of("wiki", "tv", "text"), null, null);

        Assertions.assertFalse(generator.isApplicable(record));
    }

    /**
     * Verify that {@link ShiftOnDay#isApplicable(RawRecordContainer)} returns true if it has a non-empty set of data types, and null dates, and the record has
     * a matching data type.
     */
    @Test
    void testIsApplicableWithMatchingDataTypeWithNullDates() {
        RawRecordContainer record = createRecord("20200101");

        ShiftOnDay generator = new ShiftOnDay(Set.of("wiki", "tv", CSV), null, null);

        Assertions.assertTrue(generator.isApplicable(record));
    }

    /**
     * Verify that {@link ShiftOnDay#isApplicable(RawRecordContainer)} returns true if it has an empty set of data types, a non-null begin date, and a null end
     * dates, and the record has a date equal to or after the begin date.
     */
    @Test
    void testIsApplicableWithDateWithinRangeWithNonNullBeginDate() {
        RawRecordContainer record = createRecord("20200115");

        ShiftOnDay generator = new ShiftOnDay(null, DateHelper.parse("20200101"), null);

        Assertions.assertTrue(generator.isApplicable(record));
    }

    /**
     * Verify that {@link ShiftOnDay#isApplicable(RawRecordContainer)} returns false if it has an empty set of data types, a non-null begin date, and a null end
     * dates, and the record has a date before the begin date.
     */
    @Test
    void testIsApplicableWithDateOutsideRangeWithNonNullBeginDate() {
        RawRecordContainer record = createRecord("19990110");

        ShiftOnDay generator = new ShiftOnDay(null, DateHelper.parse("20200101"), null);

        Assertions.assertFalse(generator.isApplicable(record));
    }

    /**
     * Verify that {@link ShiftOnDay#isApplicable(RawRecordContainer)} returns true if it has an empty set of data types, a null begin date, and a non-null end
     * dates, and the record has a date equal to or before the end date.
     */
    @Test
    void testIsApplicableWithDateWithinRangeWithNonNullEndDate() {
        RawRecordContainer record = createRecord("20200115");

        ShiftOnDay generator = new ShiftOnDay(null, null, DateHelper.parse("20240111"));

        Assertions.assertTrue(generator.isApplicable(record));
    }

    /**
     * Verify that {@link ShiftOnDay#isApplicable(RawRecordContainer)} returns false if it has an empty set of data types, a null begin date, and a non-null end
     * dates, and the record has a date after the end date.
     */
    @Test
    void testIsApplicableWithDateOutsideRangeWithNonNullEndDate() {
        RawRecordContainer record = createRecord("20240111");

        ShiftOnDay generator = new ShiftOnDay(null, null, DateHelper.parse("20200101"));

        Assertions.assertFalse(generator.isApplicable(record));
    }

    /**
     * Verify that {@link ShiftOnDay#isApplicable(RawRecordContainer)} returns true if it has an empty set of data types, a non-null begin date, and a non-null
     * end dates, and the record has a date that falls within the date range.
     */
    @Test
    void testIsApplicableWithDateWithinRangeWithNonNullDates() {
        RawRecordContainer record = createRecord("20220111");

        ShiftOnDay generator = new ShiftOnDay(null, DateHelper.parse("20200101"), DateHelper.parse("20240101"));

        Assertions.assertTrue(generator.isApplicable(record));
    }

    /**
     * Verify that the begin of the date range is inclusive.
     */
    @Test
    void testIsApplicableBeginDateInclusive() {
        RawRecordContainer record = createRecord("20200101");

        ShiftOnDay generator = new ShiftOnDay(null, DateHelper.parse("20200101"), DateHelper.parse("20240101"));

        Assertions.assertTrue(generator.isApplicable(record));
    }

    /**
     * Verify that the end of the date range is inclusive.
     */
    @Test
    void testIsApplicableEndDateInclusive() {
        RawRecordContainer record = createRecord("20240101");

        ShiftOnDay generator = new ShiftOnDay(null, DateHelper.parse("20200101"), DateHelper.parse("20240101"));

        Assertions.assertTrue(generator.isApplicable(record));
    }

    /**
     * Verify the behavior of {@link ShiftOnDay#getShardId(RawRecordContainer, String, int)}.
     */
    @Test
    void testGetShardId() {
        RawRecordContainer record = createRecord("20240101");
        ShiftOnDay shiftOnDay = new ShiftOnDay(null, null, null);
        String shardId = shiftOnDay.getShardId(record, "20240101_2", 10);
        Assertions.assertEquals("20240101_12", shardId);
    }

    private RawRecordContainer createRecord(String dateStr) {
        Type dataType = new Type(CSV, CSVIngestHelper.class, null, null, 0, null);
        Date date = DateHelper.parse(dateStr);
        RawRecordContainerImplTest.ValidatingRawRecordContainerImpl event = new RawRecordContainerImplTest.ValidatingRawRecordContainerImpl();
        event.setTimestamp(date.getTime());
        event.setDataType(dataType);
        return event;
    }
}
