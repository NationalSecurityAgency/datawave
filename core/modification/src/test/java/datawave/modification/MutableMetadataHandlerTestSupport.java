package datawave.modification;

import static datawave.modification.MutableMetadataHandler.FIELD_INDEX_PREFIX;
import static datawave.modification.MutableMetadataHandler.HISTORY_PREFIX;
import static datawave.modification.MutableMetadataHandler.NULL_BYTE;
import static datawave.modification.MutableMetadataHandler.NULL_VALUE;
import static org.apache.commons.lang3.StringUtils.reverse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import datawave.data.hash.HashUID;
import datawave.data.hash.UID;
import datawave.ingest.protobuf.Uid;
import datawave.marking.MarkingFunctions;
import datawave.query.QueryTestTableHelper;
import datawave.query.util.MetadataHelperFactory;
import datawave.query.util.WiseGuysIngest;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.ProxiedUserDetails;
import datawave.table.constants.MetadataColumnFamilyConstants;
import datawave.table.constants.TableName;
import datawave.webservice.modification.DefaultModificationRequest;
import datawave.webservice.modification.EventIdentifier;
import datawave.webservice.modification.ModificationRequestBase.MODE;

@SuppressWarnings({"SameParameterValue", "unused"})
class MutableMetadataHandlerTestSupport {
    static final Logger log = Logger.getLogger(MutableMetadataHandlerTestSupport.class);
    private static final BatchWriterConfig BATCH_WRITER_CONFIG = new BatchWriterConfig().setMaxMemory(1000L).setMaxLatency(1, TimeUnit.SECONDS)
                    .setMaxWriteThreads(1);

    static final String SHARD_ID = "20130101_0";
    static final String DATATYPE = "test";
    static final String USER = "testuser";
    static final long DAY_TIMESTAMP = 1356998400000L;
    static final long CAPONE_EVENT_TIMESTAMP = 1356998400020L;

    static final String METADATA_TABLE = QueryTestTableHelper.MODEL_TABLE_NAME;
    static final String AGE_FIELD = "AGE";
    static final String LOCATION_FIELD = "LOCATION";
    static final String NAME_FIELD = "NAME";
    static final String QUOTE_FIELD = "QUOTE";
    static final String QUOTE_TOKEN_FIELD = "QUOTE_TOKEN";
    static final String SENTENCE_FIELD = "SENTENCE";
    static final String UUID_FIELD = "UUID";

    static final Authorizations AUTHS = new Authorizations("ALL", "A", "B", "C");
    static final Set<Authorizations> AUTH_SET = Collections.singleton(AUTHS);

    static final String CAPONE_UID = WiseGuysIngest.caponeUID;
    static final String DAY_DATE_STRING = "20130101";
    static final String VIS_ALL = "ALL";
    static final String VIS_A_AND_B = "A&B";

    final AccumuloClient client;
    final MutableMetadataHandler handler;
    final ProxiedUserDetails userDetails = new SimpleUserDetails(USER);
    final Map<String,Set<String>> mutableFields = new HashMap<>();

    MutableMetadataHandlerTestSupport(String instanceName) throws Exception {
        System.setProperty(MetadataHelperFactory.ALL_AUTHS_PROPERTY, "ALL");

        QueryTestTableHelper tableHelper = new QueryTestTableHelper(instanceName, log);
        client = tableHelper.client;
        client.securityOperations().changeUserAuthorizations(client.whoami(), AUTHS);
        WiseGuysIngest.writeItAll(client, WiseGuysIngest.WhatKindaRange.DOCUMENT);

        handler = new MutableMetadataHandler();
        handler.setEventTableName(TableName.SHARD);
        handler.setIndexTableName(TableName.SHARD_INDEX);
        handler.setReverseIndexTableName(TableName.SHARD_RINDEX);
        handler.setMetadataTableName(METADATA_TABLE);
        handler.setMarkingFunctions(new MarkingFunctions.Default());
        handler.setMetadataHelperFactory(new MetadataHelperFactory());

        mutableFields.put(DATATYPE, new LinkedHashSet<>(List.of(UUID_FIELD, NAME_FIELD, AGE_FIELD, QUOTE_FIELD)));
    }

    DefaultModificationRequest newRequest(MODE mode, String fieldName, String fieldValue, String oldFieldValue) {
        return newRequest(mode, fieldName, fieldValue, oldFieldValue, "ALL");
    }

    DefaultModificationRequest newRequest(MODE mode, String fieldName, String fieldValue, String oldFieldValue, String columnVisibility) {
        DefaultModificationRequest request = new DefaultModificationRequest();
        request.setMode(mode);
        request.setEvents(Collections.singletonList(eventId(CAPONE_UID)));
        request.setFieldName(fieldName);
        request.setFieldValue(fieldValue);
        request.setColumnVisibility(columnVisibility);
        if (oldFieldValue != null) {
            request.setOldFieldValue(oldFieldValue);
            request.setOldColumnVisibility(columnVisibility);
        }
        return request;
    }

    DefaultModificationRequest newMarkingsInsertRequest(String fieldName, String fieldValue, String columnVisibility) {
        DefaultModificationRequest request = new DefaultModificationRequest();
        request.setMode(MODE.INSERT);
        request.setEvents(Collections.singletonList(eventId(CAPONE_UID)));
        request.setFieldName(fieldName);
        request.setFieldValue(fieldValue);
        Map<String,String> markings = new HashMap<>();
        markings.put("columnVisibility", columnVisibility);
        request.setFieldMarkings(markings);
        request.setColumnVisibility(null);
        return request;
    }

    static EventIdentifier eventId(String eventUid) {
        EventIdentifier event = new EventIdentifier();
        event.setShardId(SHARD_ID);
        event.setDatatype(DATATYPE);
        event.setEventUid(eventUid);
        return event;
    }

    void process(DefaultModificationRequest request) throws Exception {
        handler.process(client, request, mutableFields, AUTH_SET, userDetails);
    }

    void process(DefaultModificationRequest request, boolean purgeIndex, boolean insertHistory) throws Exception {
        handler.process(client, request, mutableFields, AUTH_SET, userDetails, purgeIndex, insertHistory);
    }

    void addMutableField(String fieldName) {
        mutableFields.computeIfAbsent(DATATYPE, key -> new LinkedHashSet<>()).add(fieldName);
    }

    void setContentFields(Set<String> fields) {
        handler.setContentFields(fields);
    }

    void setIndexOnlySuffixes(Set<String> suffixes) {
        handler.setIndexOnlySuffixes(suffixes);
    }

    void put(String tableName, String row, String cf, String cq, String visibility, long timestamp, Value value) throws Exception {
        Mutation mutation = new Mutation(row);
        mutation.put(new Text(cf), new Text(cq), new ColumnVisibility(visibility), timestamp, value);
        try (var writer = client.createBatchWriter(tableName, BATCH_WRITER_CONFIG)) {
            writer.addMutation(mutation);
        }
    }

    List<Entry<Key,Value>> scanRow(String tableName, String row) throws Exception {
        List<Entry<Key,Value>> entries = new ArrayList<>();
        try (var scanner = client.createScanner(tableName, AUTHS)) {
            scanner.setRange(new Range(row));
            for (Entry<Key,Value> entry : scanner) {
                if (entry.getKey().getRow().toString().equals(row)) {
                    entries.add(entry);
                }
            }
        }
        return entries;
    }

    /**
     * Asserts that an exact table entry exists and, when provided, that its visibility, timestamp, and value match.
     */
    void assertEntryPresent(String tableName, String row, String cf, String cq, String visibility, Long timestamp, Value value) throws Exception {
        Entry<Key,Value> entry = findEntry(tableName, row, cf, cq);
        assertNotNull(entry, "Missing expected entry in " + tableName + " for " + row + " " + cf + " " + cq);
        if (visibility != null) {
            assertEquals(new String(entry.getKey().getColumnVisibility().copyBytes(), StandardCharsets.UTF_8), visibility,
                            "Unexpected visibility for " + entry.getKey());
        }
        if (timestamp != null) {
            assertEquals(entry.getKey().getTimestamp(), (long) timestamp, "Unexpected timestamp for " + entry.getKey() + ": " + entry.getKey().getTimestamp());
        }
        if (value != null) {
            assertEquals(entry.getValue(), value, "Unexpected value for " + entry.getKey());
        }
    }

    /**
     * Asserts that no entry exists for the exact table, row, column family, and column qualifier.
     */
    void assertEntryAbsent(String tableName, String row, String cf, String cq) throws Exception {
        assertNull(findEntry(tableName, row, cf, cq), "Unexpected entry in " + tableName + " for " + row + " " + cf + " " + cq);
    }

    /**
     * Asserts that the primary event-table entry for the Capone event exists with the expected visibility.
     */
    void assertEventFieldPresent(String fieldName, String fieldValue, String visibility) throws Exception {
        assertEntryPresent(TableName.SHARD, SHARD_ID, eventFieldColumnFamily(), eventFieldQualifier(fieldName, fieldValue), visibility, CAPONE_EVENT_TIMESTAMP,
                        NULL_VALUE);
    }

    /**
     * Asserts that the primary event-table entry for the Capone event has been removed.
     */
    void assertEventFieldAbsent(String fieldName, String fieldValue) throws Exception {
        assertEntryAbsent(TableName.SHARD, SHARD_ID, eventFieldColumnFamily(), eventFieldQualifier(fieldName, fieldValue));
    }

    /**
     * Asserts that the shard-table field-index entry exists for the normalized value.
     */
    void assertFieldIndexPresent(String fieldName, String normalizedValue, String visibility) throws Exception {
        assertEntryPresent(TableName.SHARD, SHARD_ID, FIELD_INDEX_PREFIX + fieldName, fieldIndexQualifier(normalizedValue), visibility, CAPONE_EVENT_TIMESTAMP,
                        NULL_VALUE);
    }

    /**
     * Asserts that the shard-table field-index entry has been removed for the normalized value.
     */
    void assertFieldIndexAbsent(String fieldName, String normalizedValue) throws Exception {
        assertEntryAbsent(TableName.SHARD, SHARD_ID, FIELD_INDEX_PREFIX + fieldName, fieldIndexQualifier(normalizedValue));
    }

    /**
     * Asserts that the shard-table content row exists for the field and grouping index.
     */
    void assertContentRowPresent(String fieldName, String grouping, String visibility) throws Exception {
        assertEntryPresent(TableName.SHARD, SHARD_ID, "d", contentQualifier(fieldName, grouping), visibility, CAPONE_EVENT_TIMESTAMP, NULL_VALUE);
    }

    /**
     * Asserts that the shard-table content row has been removed for the field and grouping index.
     */
    void assertContentRowAbsent(String fieldName, String grouping) throws Exception {
        assertEntryAbsent(TableName.SHARD, SHARD_ID, "d", contentQualifier(fieldName, grouping));
    }

    /**
     * Asserts that the shard-table term-frequency row exists for the token and derived field name.
     */
    void assertTermFrequencyRowPresent(String fieldName, String token, String visibility) throws Exception {
        assertEntryPresent(TableName.SHARD, SHARD_ID, MetadataColumnFamilyConstants.COLF_TF.toString(), termFrequencyQualifier(token, fieldName), visibility,
                        CAPONE_EVENT_TIMESTAMP, NULL_VALUE);
    }

    /**
     * Asserts that the shard-table term-frequency row has been removed for the token and derived field name.
     */
    void assertTermFrequencyRowAbsent(String fieldName, String token) throws Exception {
        assertEntryAbsent(TableName.SHARD, SHARD_ID, MetadataColumnFamilyConstants.COLF_TF.toString(), termFrequencyQualifier(token, fieldName));
    }

    /**
     * Asserts that the shard-index table contains the expected UID-list mutation for the normalized value.
     */
    void assertShardIndexMutation(String fieldName, String normalizedValue, String visibility, int count) throws Exception {
        assertEntryPresent(TableName.SHARD_INDEX, normalizedValue, fieldName, globalIndexQualifier(), visibility, DAY_TIMESTAMP,
                        uidListValue(CAPONE_UID, count));
    }

    /**
     * Asserts that the reverse-index table contains the expected UID-list mutation for the normalized value.
     */
    void assertReverseIndexMutation(String fieldName, String normalizedValue, String visibility, int count) throws Exception {
        assertEntryPresent(TableName.SHARD_RINDEX, reverse(normalizedValue), fieldName, globalIndexQualifier(), visibility, DAY_TIMESTAMP,
                        uidListValue(CAPONE_UID, count));
    }

    Entry<Key,Value> findEntry(String tableName, String row, String cf, String cq) throws Exception {
        for (Entry<Key,Value> entry : scanRow(tableName, row)) {
            if (entry.getKey().getColumnFamily().toString().equals(cf) && entry.getKey().getColumnQualifier().toString().equals(cq)) {
                return entry;
            }
        }
        return null;
    }

    /**
     * Asserts that a history entry exists for the field and contains the expected fragment anywhere in its qualifier.
     */
    void assertHistoryEntryContains(String fieldName, String fragment) throws Exception {
        assertHistoryEntryContainsAtTimestamp(fieldName, fragment, null);
    }

    /**
     * Asserts that a history entry exists for the field, contains the expected qualifier fragment, and optionally matches the expected timestamp.
     */
    void assertHistoryEntryContainsAtTimestamp(String fieldName, String fragment, Long timestamp) throws Exception {
        String cf = eventFieldColumnFamily();
        for (Entry<Key,Value> entry : scanRow(TableName.SHARD, SHARD_ID)) {
            if (entry.getKey().getColumnFamily().toString().equals(cf)) {
                String cq = entry.getKey().getColumnQualifier().toString();
                if (cq.startsWith(HISTORY_PREFIX + fieldName + NULL_BYTE) && cq.contains(fragment)) {
                    if (timestamp != null) {
                        assertEquals(timestamp.longValue(), entry.getKey().getTimestamp(), "Unexpected history timestamp for " + cq);
                    }
                    return;
                }
            }
        }
        throw new AssertionError("No history entry for " + fieldName + " containing " + fragment);
    }

    /**
     * Asserts that the Capone event currently has the expected number of history entries for the field.
     */
    void assertHistoryEntryCount(String fieldName, int expectedCount) throws Exception {
        assertEquals(expectedCount, historyEntries(fieldName).size(), "Unexpected history entry count for " + fieldName);
    }

    List<Entry<Key,Value>> historyEntries(String fieldName) throws Exception {
        String cf = eventFieldColumnFamily();
        String prefix = HISTORY_PREFIX + fieldName + NULL_BYTE;
        List<Entry<Key,Value>> results = new ArrayList<>();
        for (Entry<Key,Value> entry : scanRow(TableName.SHARD, SHARD_ID)) {
            if (entry.getKey().getColumnFamily().toString().equals(cf) && entry.getKey().getColumnQualifier().toString().startsWith(prefix)) {
                results.add(entry);
            }
        }
        return results;
    }

    /**
     * Asserts that the latest metadata frequency row for the field stores the expected summed frequency value.
     */
    void assertLatestMetadataFrequencyValue(String fieldName, long count) throws Exception {
        Entry<Key,Value> entry = findEntry(METADATA_TABLE, fieldName, MetadataColumnFamilyConstants.COLF_F.toString(), metadataFrequencyQualifier());
        assertNotNull(entry, "Missing metadata frequency entry for " + fieldName);
        assertEquals(entry.getValue(), new Value(SummingCombiner.VAR_LEN_ENCODER.encode(count)), "Unexpected metadata frequency value for " + fieldName);
    }

    /**
     * Asserts that a history entry exists for the field with the expected insert operation payload.
     */
    void assertHistoryInsert(String fieldName, String value) throws Exception {
        assertHistoryEntryContains(fieldName, ":" + USER + ":" + value + ":insert");
    }

    /**
     * Asserts that a history entry exists for the field with the expected delete operation payload.
     */
    void assertHistoryDelete(String fieldName, String value) throws Exception {
        assertHistoryEntryContains(fieldName, ":" + USER + ":" + value + ":delete");
    }

    /**
     * Asserts that a history entry exists for the field with the expected update operation payload.
     */
    void assertHistoryUpdate(String fieldName, String value) throws Exception {
        assertHistoryEntryContains(fieldName, ":" + USER + ":" + value + ":update");
    }

    /**
     * Asserts that a history entry exists for the field with the expected operation payload at a specific timestamp.
     */
    void assertHistoryInsertAtTimestamp(String fieldName, String value, long timestamp) throws Exception {
        assertHistoryEntryContainsAtTimestamp(fieldName, ":" + USER + ":" + value + ":insert", timestamp);
    }

    /**
     * Asserts that a history entry exists for the field with the expected delete operation payload at a specific timestamp.
     */
    void assertHistoryDeleteAtTimestamp(String fieldName, String value, long timestamp) throws Exception {
        assertHistoryEntryContainsAtTimestamp(fieldName, ":" + USER + ":" + value + ":delete", timestamp);
    }

    /**
     * Asserts that a history entry exists for the field with the expected update operation payload at a specific timestamp.
     */
    void assertHistoryUpdateAtTimestamp(String fieldName, String value, long timestamp) throws Exception {
        assertHistoryEntryContainsAtTimestamp(fieldName, ":" + USER + ":" + value + ":update", timestamp);
    }

    /**
     * Asserts that metadata state has not changed by comparing the frequency value to a previously recorded value.
     */
    void assertMetadataUnchanged(String fieldName, Value beforeValue) throws Exception {
        Entry<Key,Value> entry = findEntry(METADATA_TABLE, fieldName, MetadataColumnFamilyConstants.COLF_F.toString(), metadataFrequencyQualifier());
        assertEquals(entry.getValue(), beforeValue, "Metadata changed unexpectedly for " + fieldName);
    }

    static String normalizeUuid(String value) {
        return value.toLowerCase();
    }

    static Value uidListValue(String eventUid, int count) {
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.setIGNORE(false);
        builder.setCOUNT(count);
        builder.addUID(eventUid);
        return new Value(builder.build().toByteArray());
    }

    /**
     * Retrieves a metadata frequency value snapshot for later comparison via {@link #assertMetadataUnchanged(String, Value)}.
     */
    Value captureMetadataFrequencyValue(String fieldName) throws Exception {
        Entry<Key,Value> entry = findEntry(METADATA_TABLE, fieldName, MetadataColumnFamilyConstants.COLF_F.toString(), metadataFrequencyQualifier());
        return entry != null ? entry.getValue() : null;
    }

    /**
     * Sets up a token field (e.g., QUOTE_TOKEN) with field-index, term-frequency, and global-index rows.
     */
    void setupTokenField(String tokenFieldName, String tokenValue) throws Exception {
        setupTokenField(tokenFieldName, tokenValue, tokenFieldName);
    }

    /**
     * Sets up a token field where the tf row may use a different field name than the fi/global-index rows, e.g. QUOTE_TOKEN.<hash>.
     */
    void setupTokenField(String tokenFieldName, String tokenValue, String termFrequencyFieldName) throws Exception {
        put(TableName.SHARD, SHARD_ID, FIELD_INDEX_PREFIX + tokenFieldName, fieldIndexQualifier(tokenValue), VIS_ALL, CAPONE_EVENT_TIMESTAMP, NULL_VALUE);
        put(TableName.SHARD, SHARD_ID, MetadataColumnFamilyConstants.COLF_TF.toString(), termFrequencyQualifier(tokenValue, termFrequencyFieldName), VIS_ALL,
                        CAPONE_EVENT_TIMESTAMP, NULL_VALUE);
        put(TableName.SHARD_INDEX, tokenValue, tokenFieldName, globalIndexQualifier(), VIS_ALL, DAY_TIMESTAMP, uidListValue(CAPONE_UID, 1));
    }

    static String contentContextField(String fieldName, String content) {
        return fieldName + "." + contentContextHash(content);
    }

    static String contentContextHash(String content) {
        UID uid = HashUID.builder().newId(content);
        String baseUid = uid.getBaseUid();
        return baseUid.substring(0, baseUid.indexOf('.'));
    }

    /**
     * Builds a shard event field column family qualifier: DATATYPE\0UID
     */
    static String eventFieldColumnFamily() {
        return DATATYPE + NULL_BYTE + CAPONE_UID;
    }

    /**
     * Builds a shard event field column qualifier: fieldName\0fieldValue
     */
    static String eventFieldQualifier(String fieldName, String fieldValue) {
        return fieldName + NULL_BYTE + fieldValue;
    }

    /**
     * Builds a shard field-index column qualifier: normalizedValue\0DATATYPE\0UID
     */
    static String fieldIndexQualifier(String normalizedValue) {
        return normalizedValue + NULL_BYTE + DATATYPE + NULL_BYTE + CAPONE_UID;
    }

    /**
     * Builds a shard term-frequency column qualifier: DATATYPE\0UID\0token\0fieldName
     */
    static String termFrequencyQualifier(String token, String fieldName) {
        return DATATYPE + NULL_BYTE + CAPONE_UID + NULL_BYTE + token + NULL_BYTE + fieldName;
    }

    /**
     * Builds a shard content ('d' column) column qualifier: DATATYPE\0UID\0fieldName\0grouping
     */
    static String contentQualifier(String fieldName, String grouping) {
        return DATATYPE + NULL_BYTE + CAPONE_UID + NULL_BYTE + fieldName + NULL_BYTE + grouping;
    }

    /**
     * Builds a metadata frequency column qualifier: DATATYPE\0DAY_DATE_STRING
     */
    static String metadataFrequencyQualifier() {
        return DATATYPE + NULL_BYTE + DAY_DATE_STRING;
    }

    /**
     * Builds a global/reverse index column qualifier: SHARD_ID\0DATATYPE
     */
    static String globalIndexQualifier() {
        return SHARD_ID + NULL_BYTE + DATATYPE;
    }

    /**
     * Asserts that a process() call with the given request throws an exception of the specified type. The exception is expected; if no exception is thrown, the
     * assertion fails.
     */
    <T extends Exception> void assertProcessThrows(Class<T> exceptionType, DefaultModificationRequest request) {
        try {
            process(request);
            throw new AssertionError("Expected " + exceptionType.getSimpleName() + " but no exception was thrown");
        } catch (Exception e) {
            if (!exceptionType.isInstance(e)) {
                throw new AssertionError("Expected " + exceptionType.getSimpleName() + " but got " + e.getClass().getSimpleName(), e);
            }
            // Expected - the right exception type was thrown
        }
    }

    /**
     * Asserts that a process() call with the given request and parameters throws an exception of the specified type. The exception is expected; if no exception
     * is thrown, the assertion fails.
     */
    <T extends Exception> void assertProcessThrows(Class<T> exceptionType, DefaultModificationRequest request, boolean purgeIndex, boolean insertHistory) {
        try {
            process(request, purgeIndex, insertHistory);
            throw new AssertionError("Expected " + exceptionType.getSimpleName() + " but no exception was thrown");
        } catch (Exception e) {
            if (!exceptionType.isInstance(e)) {
                throw new AssertionError("Expected " + exceptionType.getSimpleName() + " but got " + e.getClass().getSimpleName(), e);
            }
            // Expected - the right exception type was thrown
        }
    }

    static class SimpleUserDetails implements ProxiedUserDetails {
        private final String shortName;

        SimpleUserDetails(String shortName) {
            this.shortName = shortName;
        }

        @Override
        public Collection<? extends DatawaveUser> getProxiedUsers() {
            return Collections.emptyList();
        }

        @Override
        public String getName() {
            return shortName;
        }

        @Override
        public DatawaveUser getPrimaryUser() {
            return null;
        }

        @Override
        public Collection<? extends Collection<String>> getAuthorizations() {
            return Collections.emptyList();
        }

        @Override
        public String[] getDNs() {
            return new String[0];
        }

        @Override
        public String getShortName() {
            return shortName;
        }

        @Override
        public List<String> getProxyServers() {
            return Collections.emptyList();
        }

        @Override
        public <T extends ProxiedUserDetails> T newInstance(List<DatawaveUser> proxiedUsers) {
            throw new UnsupportedOperationException("Not needed for tests");
        }
    }
}
