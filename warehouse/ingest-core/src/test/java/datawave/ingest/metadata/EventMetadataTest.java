package datawave.ingest.metadata;

import static datawave.ingest.mapreduce.handler.DataTypeHandler.NULL_VALUE;
import static datawave.ingest.metadata.RawRecordMetadata.DELIMITER;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.niceMock;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.IdentityDataType;
import datawave.TestAbstractContentIngestHelper;
import datawave.TestBaseIngestHelper;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NumberType;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.mapreduce.job.BulkIngestKey;

public class EventMetadataTest {

    private static final Text SHARD_TABLE_NAME = new Text("shard");
    private static final Text SHARD_INDEX_TABLE_NAME = new Text("shardIndex");
    private static final Text SHARD_REVERSE_INDEX_TABLE_NAME = new Text("shardReverseIndex");
    private static final Text METADATA_TABLE_NAME = new Text("metadata");
    private static final Text LOAD_DATE_TABLE_NAME = new Text("loadDate");
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd", Locale.US);

    private final Multimap<String,NormalizedContentInterface> fieldValues = HashMultimap.create();
    private EventMetadata eventMetadata;
    private Multimap<BulkIngestKey,Value> bulkMetadata;

    @After
    public void tearDown() throws Exception {
        this.fieldValues.clear();
        this.eventMetadata = null;
        this.bulkMetadata = null;
    }

    /**
     * Test ingesting a single event.
     */
    @Test
    public void testSingleEvent() {
        // Configure the field values.
        givenFieldValue("FIELD_1", "HEY HO HEY HO");
        long loadDate = getMillis("20140404");
        givenFieldValue("LOAD_DATE", String.valueOf(loadDate));

        // Configure the helper interface.
        IngestHelper helper = createIngestHelper();
        helper.addDataType("FIELD_1", new IdentityDataType());

        // Init the event metadata and add the event.
        initEventMetadata();
        long eventDate = getMillis("20140402");
        eventMetadata.addEvent(helper, createMockEvent("xyzabc", eventDate, helper), fieldValues, loadDate);

        // Validate the resulting bulk entries.
        collectBulkEntries();
        assertTotalBulkEntries(4);
        assertContainsMetadataTableEntry("FIELD_1", "e", "xyzabc", eventDate, NULL_VALUE);
        assertContainsMetadataTableEntry("FIELD_1", "f", "xyzabc" + DELIMITER + "20140402", eventDate, encodeCount(1L));
        assertContainsMetadataTableEntry("LOAD_DATE", "e", "xyzabc", eventDate, NULL_VALUE);
        assertContainsMetadataTableEntry("LOAD_DATE", "f", "xyzabc" + DELIMITER + "20140402", eventDate, encodeCount(1L));
    }

    /**
     * Test ingesting an event for an excluded field.
     */
    @Test
    public void testExcludedField() {
        // Configure the field values.
        givenFieldValue("FIELD_1", "HEY HO HEY HO");
        long loadDate = getMillis("20140404");
        givenFieldValue("LOAD_DATE", String.valueOf(loadDate));

        // Configure the helper interface.
        IngestHelper helper = createIngestHelper();
        helper.addDataType("FIELD_1", new IdentityDataType());

        // Mark the field as excluded.
        helper.addShardExclusionField("FIELD_1");

        // Init the event metadata and add the event.
        initEventMetadata();
        long eventDate = getMillis("20140402");
        eventMetadata.addEvent(helper, createMockEvent("xyzabc", eventDate, helper), fieldValues, loadDate);

        // Validate the resulting bulk entries.
        collectBulkEntries();
        assertTotalBulkEntries(2);
        assertContainsMetadataTableEntry("LOAD_DATE", "e", "xyzabc", eventDate, NULL_VALUE);
        assertContainsMetadataTableEntry("LOAD_DATE", "f", "xyzabc" + DELIMITER + "20140402", eventDate, encodeCount(1L));
    }

    /**
     * Test ingesting an event for an indexed field.
     */
    @Test
    public void testIndexedField() {
        // Configure the field values.
        givenFieldValue("FIELD_1", "HEY HO HEY HO");
        long loadDate = getMillis("20140404");
        givenFieldValue("LOAD_DATE", String.valueOf(loadDate));

        // Configure the helper interface.
        IngestHelper helper = createIngestHelper();
        helper.addDataType("FIELD_1", new IdentityDataType());

        // Mark the field as indexed.
        helper.addIndexedField("FIELD_1");

        // Init the event metadata and add the event.
        initEventMetadata();
        long eventDate = getMillis("20140402");
        eventMetadata.addEvent(helper, createMockEvent("xyzabc", eventDate, helper), fieldValues, loadDate);

        // Validate the resulting bulk entries.
        collectBulkEntries();
        assertTotalBulkEntries(7);
        assertContainsMetadataTableEntry("FIELD_1", "e", "xyzabc", eventDate, NULL_VALUE);
        assertContainsMetadataTableEntry("FIELD_1", "i", "xyzabc" + DELIMITER + "20140402", eventDate, NULL_VALUE);
        assertContainsMetadataTableEntry("FIELD_1", "f", "xyzabc" + DELIMITER + "20140402", eventDate, encodeCount(1L));
        assertContainsMetadataTableEntry("FIELD_1", "t", "xyzabc" + DELIMITER + "datawave.IdentityDataType", eventDate, NULL_VALUE);
        assertContainsMetadataTableEntry("LOAD_DATE", "e", "xyzabc", eventDate, NULL_VALUE);
        assertContainsMetadataTableEntry("LOAD_DATE", "f", "xyzabc" + DELIMITER + "20140402", eventDate, encodeCount(1L));
        assertContainsLoadDateTableEntry("FIELD_1", "FIELD_NAME" + DELIMITER + "shardIndex", "20140404" + DELIMITER + "xyzabc", encodeCount(1L));
    }

    /**
     * Test ingesting an event for a reverse indexed field.
     */
    @Test
    public void testReverseIndexedField() {
        // Configure the field values.
        givenFieldValue("FIELD_1", "HEY HO HEY HO");
        long loadDate = getMillis("20140404");
        givenFieldValue("LOAD_DATE", String.valueOf(loadDate));

        // Configure the helper interface.
        IngestHelper helper = createIngestHelper();
        helper.addDataType("FIELD_1", new IdentityDataType());

        // Mark the field as reverse indexed.
        helper.addReverseIndexedField("FIELD_1");

        // Init the event metadata and add the event.
        initEventMetadata();
        long eventDate = getMillis("20140402");
        eventMetadata.addEvent(helper, createMockEvent("xyzabc", eventDate, helper), fieldValues, loadDate);

        // Validate the resulting bulk entries.
        collectBulkEntries();
        assertTotalBulkEntries(7);
        assertContainsMetadataTableEntry("FIELD_1", "e", "xyzabc", eventDate, NULL_VALUE);
        assertContainsMetadataTableEntry("FIELD_1", "ri", "xyzabc" + DELIMITER + "20140402", eventDate, NULL_VALUE);
        assertContainsMetadataTableEntry("FIELD_1", "f", "xyzabc" + DELIMITER + "20140402", eventDate, encodeCount(1L));
        assertContainsMetadataTableEntry("FIELD_1", "t", "xyzabc" + DELIMITER + "datawave.IdentityDataType", eventDate, NULL_VALUE);
        assertContainsMetadataTableEntry("LOAD_DATE", "e", "xyzabc", eventDate, NULL_VALUE);
        assertContainsMetadataTableEntry("LOAD_DATE", "f", "xyzabc" + DELIMITER + "20140402", eventDate, encodeCount(1L));
        assertContainsLoadDateTableEntry("FIELD_1", "FIELD_NAME" + DELIMITER + "shardReverseIndex", "20140404" + DELIMITER + "xyzabc", encodeCount(1L));
    }

    /**
     * Test ingesting an event for a field that is both indexed and reverse indexed.
     */
    @Test
    public void testIndexedAndReverseIndexedField() {
        // Configure the field values.
        givenFieldValue("FIELD_1", "HEY HO HEY HO");
        long loadDate = getMillis("20140404");
        givenFieldValue("LOAD_DATE", String.valueOf(loadDate));

        // Configure the helper interface.
        IngestHelper helper = createIngestHelper();
        helper.addDataType("FIELD_1", new IdentityDataType());

        // Mark the field as indexed and reverse indexed.
        helper.addIndexedField("FIELD_1");
        helper.addReverseIndexedField("FIELD_1");

        // Init the event metadata and add the event.
        initEventMetadata();
        long eventDate = getMillis("20140402");
        eventMetadata.addEvent(helper, createMockEvent("xyzabc", eventDate, helper), fieldValues, loadDate);

        // Validate the resulting bulk entries.
        collectBulkEntries();
        assertTotalBulkEntries(9);
        assertContainsMetadataTableEntry("FIELD_1", "e", "xyzabc", eventDate, NULL_VALUE);
        assertContainsMetadataTableEntry("FIELD_1", "ri", "xyzabc" + DELIMITER + "20140402", eventDate, NULL_VALUE);
        assertContainsMetadataTableEntry("FIELD_1", "i", "xyzabc" + DELIMITER + "20140402", eventDate, NULL_VALUE);
        assertContainsMetadataTableEntry("FIELD_1", "f", "xyzabc" + DELIMITER + "20140402", eventDate, encodeCount(1L));
        assertContainsMetadataTableEntry("FIELD_1", "t", "xyzabc" + DELIMITER + "datawave.IdentityDataType", eventDate, NULL_VALUE);
        assertContainsMetadataTableEntry("LOAD_DATE", "e", "xyzabc", eventDate, NULL_VALUE);
        assertContainsMetadataTableEntry("LOAD_DATE", "f", "xyzabc" + DELIMITER + "20140402", eventDate, encodeCount(1L));
        assertContainsLoadDateTableEntry("FIELD_1", "FIELD_NAME" + DELIMITER + "shardIndex", "20140404" + DELIMITER + "xyzabc", encodeCount(1L));
        assertContainsLoadDateTableEntry("FIELD_1", "FIELD_NAME" + DELIMITER + "shardReverseIndex", "20140404" + DELIMITER + "xyzabc", encodeCount(1L));
    }

    /**
     * Test ingesting multiple events that have the same load date and event date.
     */
    @Test
    public void testMultipleEventsForSameLoadDateAndEventDate() {
        // Configure the field values.
        givenFieldValue("FIELD_1", "HEY HO HEY HO");
        long loadDate = getMillis("20140404");
        givenFieldValue("LOAD_DATE", String.valueOf(loadDate));

        // Configure the helper interface.
        IngestHelper helper = createIngestHelper();
        helper.addDataType("FIELD_1", new IdentityDataType());

        // Init the event metadata and add the event.
        initEventMetadata();
        long eventDate = getMillis("20140402");
        eventMetadata.addEvent(helper, createMockEvent("xyzabc", eventDate, helper), fieldValues, loadDate);
        eventMetadata.addEvent(helper, createMockEvent("xyzabc", eventDate, helper), fieldValues, loadDate);

        // Validate the resulting bulk entries.
        collectBulkEntries();
        assertTotalBulkEntries(4);
        assertContainsMetadataTableEntry("FIELD_1", "e", "xyzabc", eventDate, NULL_VALUE);
        assertContainsMetadataTableEntry("FIELD_1", "f", "xyzabc" + DELIMITER + "20140402", eventDate, encodeCount(2L));
        assertContainsMetadataTableEntry("LOAD_DATE", "e", "xyzabc", eventDate, NULL_VALUE);
        assertContainsMetadataTableEntry("LOAD_DATE", "f", "xyzabc" + DELIMITER + "20140402", eventDate, encodeCount(2L));
    }

    /**
     * Test ingesting multiple events that have the same load date, but different event dates.
     */
    @Test
    public void testMultipleEventsForSameLoadDateAndDifferentEventDate() {
        // Configure the field values.
        givenFieldValue("FIELD_1", "HEY HO HEY HO");
        long loadDate = getMillis("20140404");
        givenFieldValue("LOAD_DATE", String.valueOf(loadDate));

        // Configure the helper interface.
        IngestHelper helper = createIngestHelper();
        helper.addDataType("FIELD_1", new IdentityDataType());

        // Init the event metadata and add the event.
        initEventMetadata();
        long eventDate1 = getMillis("20140402");
        eventMetadata.addEvent(helper, createMockEvent("xyzabc", eventDate1, helper), fieldValues, loadDate);
        long eventDate2 = getMillis("20140319");
        eventMetadata.addEvent(helper, createMockEvent("xyzabc", eventDate2, helper), fieldValues, loadDate);

        // Validate the resulting bulk entries.
        collectBulkEntries();
        assertTotalBulkEntries(6);
        assertContainsMetadataTableEntry("FIELD_1", "e", "xyzabc", eventDate1, NULL_VALUE);
        assertContainsMetadataTableEntry("FIELD_1", "f", "xyzabc" + DELIMITER + "20140319", eventDate2, encodeCount(1L));
        assertContainsMetadataTableEntry("FIELD_1", "f", "xyzabc" + DELIMITER + "20140402", eventDate1, encodeCount(1L));
        assertContainsMetadataTableEntry("LOAD_DATE", "e", "xyzabc", eventDate1, NULL_VALUE);
        assertContainsMetadataTableEntry("LOAD_DATE", "f", "xyzabc" + DELIMITER + "20140319", eventDate2, encodeCount(1L));
        assertContainsMetadataTableEntry("LOAD_DATE", "f", "xyzabc" + DELIMITER + "20140402", eventDate1, encodeCount(1L));
    }

    /**
     * Test ingesting an event for a field that should be targeted for tokenization.
     */
    @Test
    public void testTermFrequencyForTokenization() {
        // Configure the field values.
        givenFieldValue("FIELD_1", "HEY HO HEY HO");
        long loadDate = getMillis("20140404");
        givenFieldValue("LOAD_DATE", String.valueOf(loadDate));

        // Configure the helper interface.
        ContentIngestHelper helper = createContentIngestHelper();
        helper.addDataType("FIELD_1", new IdentityDataType());

        // Mark the field as a content index field.
        helper.addContentIndexField("FIELD_1");

        // Init the event metadata and add the event.
        initEventMetadata();
        long eventDate = getMillis("20140402");
        eventMetadata.addEvent(helper, createMockEvent("xyzabc", eventDate, helper), fieldValues, loadDate);

        // Validate the resulting bulk entries.
        collectBulkEntries();
        assertTotalBulkEntries(8);
        assertContainsMetadataTableEntry("FIELD_1", "e", "xyzabc", eventDate, NULL_VALUE);
        assertContainsMetadataTableEntry("FIELD_1", "f", "xyzabc" + DELIMITER + "20140402", eventDate, encodeCount(1L));
        assertContainsMetadataTableEntry("FIELD_1_TOKEN", "t", "xyzabc" + DELIMITER + "datawave.IdentityDataType", eventDate, NULL_VALUE);
        assertContainsMetadataTableEntry("FIELD_1_TOKEN", "tf", "xyzabc", eventDate, NULL_VALUE);
        assertContainsMetadataTableEntry("FIELD_1_TOKEN", "i", "xyzabc" + DELIMITER + "20140402", eventDate, NULL_VALUE);
        assertContainsMetadataTableEntry("LOAD_DATE", "e", "xyzabc", eventDate, NULL_VALUE);
        assertContainsMetadataTableEntry("LOAD_DATE", "f", "xyzabc" + DELIMITER + "20140402", eventDate, encodeCount(1L));
        assertContainsLoadDateTableEntry("FIELD_1_TOKEN", "FIELD_NAME" + DELIMITER + "shardIndex", "20140404" + DELIMITER + "xyzabc", encodeCount(1L));
    }

    /**
     * Test ingesting an event for a field that should be targeted for term frequencies.
     */
    @Test
    public void testTermFrequencyForLists() {
        // Configure the field values.
        givenFieldValue("FIELD_1", "HEY HO HEY HO");
        long loadDate = getMillis("20140404");
        givenFieldValue("LOAD_DATE", String.valueOf(loadDate));

        // Configure the helper interface.
        ContentIngestHelper helper = createContentIngestHelper();
        helper.addDataType("FIELD_1", new IdentityDataType());

        // Mark the field as an index list field.
        helper.addIndexListField("FIELD_1");

        // Init the event metadata and add the event.
        initEventMetadata();
        long eventDate = getMillis("20140402");
        eventMetadata.addEvent(helper, createMockEvent("xyzabc", eventDate, helper), fieldValues, loadDate);

        // Validate the resulting bulk entries.
        collectBulkEntries();
        assertTotalBulkEntries(8);
        assertContainsMetadataTableEntry("FIELD_1", "e", "xyzabc", eventDate, NULL_VALUE);
        assertContainsMetadataTableEntry("FIELD_1", "f", "xyzabc" + DELIMITER + "20140402", eventDate, encodeCount(1L));
        assertContainsMetadataTableEntry("FIELD_1", "t", "xyzabc" + DELIMITER + "datawave.IdentityDataType", eventDate, NULL_VALUE);
        assertContainsMetadataTableEntry("FIELD_1", "tf", "xyzabc", eventDate, NULL_VALUE);
        assertContainsMetadataTableEntry("FIELD_1", "i", "xyzabc" + DELIMITER + "20140402", eventDate, NULL_VALUE);
        assertContainsMetadataTableEntry("LOAD_DATE", "e", "xyzabc", eventDate, NULL_VALUE);
        assertContainsMetadataTableEntry("LOAD_DATE", "f", "xyzabc" + DELIMITER + "20140402", eventDate, encodeCount(1L));
        assertContainsLoadDateTableEntry("FIELD_1", "FIELD_NAME" + DELIMITER + "shardIndex", "20140404" + DELIMITER + "xyzabc", encodeCount(1L));
    }

    /**
     * Test ingesting an event for a field that should be targeted for term frequencies.
     */
    @Test
    public void testMultipleFieldEvents() {
        // Configure the field values.
        givenFieldValue("FIELD_1", "HEY HO HEY HO");
        givenFieldValue("FIELD_2", "Follow the yellow brick road");
        givenFieldValue("FIELD_3", "542643");

        long loadDate = getMillis("20140404");
        givenFieldValue("LOAD_DATE", String.valueOf(loadDate));

        // Configure the helper interface.
        IngestHelper helper = createIngestHelper();
        helper.addDataType("FIELD_1", new IdentityDataType());
        helper.addDataType("FIELD_2", new LcNoDiacriticsType());
        helper.addDataType("FIELD_3", new NumberType());

        // Init the event metadata and add the event.
        initEventMetadata();
        long eventDate1 = getMillis("20140402");
        eventMetadata.addEvent(helper, createMockEvent("xyzabc", eventDate1, helper), fieldValues, loadDate);
        eventMetadata.addEvent(helper, createMockEvent("xyzabc", eventDate1, helper), fieldValues, loadDate);
        eventMetadata.addEvent(helper, createMockEvent("ababa", eventDate1, helper), fieldValues, loadDate);

        long eventDate2 = getMillis("20140403");
        eventMetadata.addEvent(helper, createMockEvent("ididi", eventDate2, helper), fieldValues, loadDate);

        // Validate the resulting bulk entries.
        collectBulkEntries();
        assertTotalBulkEntries(24);
        assertContainsMetadataTableEntry("FIELD_1", "e", "xyzabc", eventDate1, NULL_VALUE);
        assertContainsMetadataTableEntry("FIELD_1", "f", "xyzabc" + DELIMITER + "20140402", eventDate1, encodeCount(2L));
        assertContainsMetadataTableEntry("FIELD_1", "f", "ababa" + DELIMITER + "20140402", eventDate1, encodeCount(1L));
        assertContainsMetadataTableEntry("FIELD_1", "e", "ababa", eventDate1, NULL_VALUE);
        assertContainsMetadataTableEntry("FIELD_1", "f", "ididi" + DELIMITER + "20140403", eventDate2, encodeCount(1L));
        assertContainsMetadataTableEntry("FIELD_1", "e", "ididi", eventDate2, NULL_VALUE);
        assertContainsMetadataTableEntry("FIELD_2", "e", "xyzabc", eventDate1, NULL_VALUE);
        assertContainsMetadataTableEntry("FIELD_2", "f", "xyzabc" + DELIMITER + "20140402", eventDate1, encodeCount(2L));
        assertContainsMetadataTableEntry("FIELD_2", "f", "ababa" + DELIMITER + "20140402", eventDate1, encodeCount(1L));
        assertContainsMetadataTableEntry("FIELD_2", "e", "ababa", eventDate1, NULL_VALUE);
        assertContainsMetadataTableEntry("FIELD_2", "f", "ididi" + DELIMITER + "20140403", eventDate2, encodeCount(1L));
        assertContainsMetadataTableEntry("FIELD_2", "e", "ididi", eventDate2, NULL_VALUE);
        assertContainsMetadataTableEntry("FIELD_3", "e", "xyzabc", eventDate1, NULL_VALUE);
        assertContainsMetadataTableEntry("FIELD_3", "f", "xyzabc" + DELIMITER + "20140402", eventDate1, encodeCount(2L));
        assertContainsMetadataTableEntry("FIELD_3", "f", "ababa" + DELIMITER + "20140402", eventDate1, encodeCount(1L));
        assertContainsMetadataTableEntry("FIELD_3", "e", "ababa", eventDate1, NULL_VALUE);
        assertContainsMetadataTableEntry("FIELD_3", "f", "ididi" + DELIMITER + "20140403", eventDate2, encodeCount(1L));
        assertContainsMetadataTableEntry("FIELD_3", "e", "ididi", eventDate2, NULL_VALUE);
        assertContainsMetadataTableEntry("LOAD_DATE", "f", "xyzabc" + DELIMITER + "20140402", eventDate1, encodeCount(2L));
        assertContainsMetadataTableEntry("LOAD_DATE", "e", "xyzabc", eventDate1, NULL_VALUE);
        assertContainsMetadataTableEntry("LOAD_DATE", "f", "ababa" + DELIMITER + "20140402", eventDate1, encodeCount(1L));
        assertContainsMetadataTableEntry("LOAD_DATE", "e", "ababa", eventDate1, NULL_VALUE);
        assertContainsMetadataTableEntry("LOAD_DATE", "f", "ididi" + DELIMITER + "20140403", eventDate2, encodeCount(1L));
        assertContainsMetadataTableEntry("LOAD_DATE", "e", "ididi", eventDate2, NULL_VALUE);
    }

    // Return the given date as millis.
    private long getMillis(String date) {
        return LocalDate.parse(date, dateTimeFormatter).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
    }

    // Return a new mock event.
    private RawRecordContainer createMockEvent(String type, long date, IngestHelperInterface helper) {
        RawRecordContainer event = niceMock(RawRecordContainer.class);
        expect(event.getDataType()).andReturn(new Type(type, helper.getClass(), null, null, 4, null)).anyTimes();
        expect(event.getDate()).andReturn(date).anyTimes();
        replay(event);
        return event;
    }

    // Add the given field and value to the field values.
    private void givenFieldValue(String field, String value) {
        this.fieldValues.put(field, new NormalizedFieldAndValue(field, value.getBytes()));
    }

    /**
     * Initialize the EventMetadata instance.
     */
    private void initEventMetadata() {
        this.eventMetadata = new EventMetadata(SHARD_TABLE_NAME, METADATA_TABLE_NAME, LOAD_DATE_TABLE_NAME, SHARD_INDEX_TABLE_NAME,
                        SHARD_REVERSE_INDEX_TABLE_NAME, true);
    }

    // Return a new IngestHelper.
    private IngestHelper createIngestHelper() {
        return new IngestHelper(fieldValues);
    }

    // Return a new ContentIngestHelper.
    private ContentIngestHelper createContentIngestHelper() {
        return new ContentIngestHelper(fieldValues);
    }

    // Collect the bulk metadata entries from the event metadata instance.
    private void collectBulkEntries() {
        this.bulkMetadata = eventMetadata.getBulkMetadata();
    }

    // Assert the total number of bulk metadata entries.
    private void assertTotalBulkEntries(int total) {
        assertEquals(total, bulkMetadata.size());
    }

    // Assert that the collected bulk metadata entries contain a matching bulk ingest key and value.
    private void assertContainsMetadataTableEntry(String field, String columnFamily, String columnQualifier, long date, Value value) {
        assertTrue(bulkMetadata.containsEntry(createBulkIngestKey(METADATA_TABLE_NAME, field, columnFamily, columnQualifier, date), value));
    }

    // Assert that the collected bulk metadata entries contain a matching bulk ingest key and value, ignoring the timestamp.
    private void assertContainsLoadDateTableEntry(String field, String columnFamily, String columnQualifier, Value value) {
        Key partialKey = new Key(new Text(field), new Text(columnFamily), new Text(columnQualifier));
        for (Map.Entry<BulkIngestKey,Value> entry : bulkMetadata.entries()) {
            BulkIngestKey ingestKey = entry.getKey();
            if (ingestKey.getTableName().equals(LOAD_DATE_TABLE_NAME) && ingestKey.getKey().equals(partialKey, PartialKey.ROW_COLFAM_COLQUAL)
                            && entry.getValue().equals(value)) {
                return;
            }
        }
        Assert.fail("No match found");
    }

    // Return a new bulk ingest key with the given attributes.
    private BulkIngestKey createBulkIngestKey(Text tableName, String fieldName, String columnFamily, String columnQualifier, long timestamp) {
        Key key = new Key(new Text(fieldName), new Text(columnFamily), new Text(columnQualifier), timestamp);
        return new BulkIngestKey(tableName, key);
    }

    // Return the given count as an encoded value.
    private Value encodeCount(long count) {
        return new Value(SummingCombiner.VAR_LEN_ENCODER.encode(count));
    }

    /**
     * Ingest helper implementation for basic testing.
     */
    private static class IngestHelper extends TestBaseIngestHelper {

        private final ArrayListMultimap<String,datawave.data.type.Type<?>> dataTypes = ArrayListMultimap.create();

        public IngestHelper(Multimap<String,NormalizedContentInterface> fieldValues) {
            super(fieldValues);
        }

        @Override
        public List<datawave.data.type.Type<?>> getDataTypes(String fieldName) {
            return dataTypes.get(fieldName);
        }

        private void addDataType(String field, datawave.data.type.Type<?> dataType) {
            dataTypes.put(field, dataType);
        }
    }

    /**
     * Ingest helper implementation for testing term frequency row generation.
     */
    private static class ContentIngestHelper extends TestAbstractContentIngestHelper {

        private final ArrayListMultimap<String,datawave.data.type.Type<?>> dataTypes = ArrayListMultimap.create();
        private final Set<String> contentIndexFields = new HashSet<>();
        private final Set<String> indexListFields = new HashSet<>();

        public ContentIngestHelper(Multimap<String,NormalizedContentInterface> fieldValues) {
            super(fieldValues);
        }

        @Override
        public boolean isContentIndexField(String field) {
            return contentIndexFields.contains(field);
        }

        private void addContentIndexField(String field) {
            this.contentIndexFields.add(field);
        }

        @Override
        public boolean isIndexListField(String field) {
            return indexListFields.contains(field);
        }

        private void addIndexListField(String field) {
            this.indexListFields.add(field);
        }

        @Override
        public List<datawave.data.type.Type<?>> getDataTypes(String fieldName) {
            return dataTypes.get(fieldName);
        }

        private void addDataType(String field, datawave.data.type.Type<?> dataType) {
            dataTypes.put(field, dataType);
        }
    }
}
