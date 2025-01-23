package datawave.modification;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.iterators.FieldIndexDocumentFilter;
import datawave.data.ColumnFamilyConstants;
import datawave.data.type.Type;
import datawave.ingest.protobuf.Uid;
import datawave.ingest.protobuf.Uid.List.Builder;
import datawave.marking.MarkingFunctions;
import datawave.microservice.query.DefaultQueryParameters;
import datawave.microservice.query.QueryPersistence;
import datawave.modification.configuration.ModificationServiceConfiguration;
import datawave.modification.query.ModificationQueryService;
import datawave.query.data.parsers.DatawaveKey;
import datawave.query.data.parsers.DatawaveKey.KeyType;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MetadataHelperFactory;
import datawave.security.authorization.ProxiedUserDetails;
import datawave.security.util.ScannerHelper;
import datawave.util.TextUtil;
import datawave.util.time.DateHelper;
import datawave.webservice.modification.DefaultModificationRequest;
import datawave.webservice.modification.EventIdentifier;
import datawave.webservice.modification.ModificationOperation;
import datawave.webservice.modification.ModificationRequestBase;
import datawave.webservice.modification.ModificationRequestBase.MODE;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.EventQueryResponseBase;
import datawave.webservice.result.GenericResponse;

/**
 * Class that handles requests for modification requests (INSERT, UPDATE, DELETE) for metadata in the shard schema. <br>
 * <br>
 * INSERT Example using Column Visibility: <br>
 *
 * <pre>
 * {@code
 *
 * <DefaultModificationRequest>
 *   <Events>
 *     <Event>
 *       <shardId>20120731_9</shardId>
 *       <datatype>myDatatype</datatype>
 *       <eventUid>0000-0000-0000-0000</eventUid>
 *     </Event>
 *   </Events>
 *   <mode>INSERT</mode>
 *   <fieldName>TEST</fieldName>
 *   <fieldValue>ABC</fieldValue>
 *   <columnVisibility>PUBLIC</columnVisibility>
 * </DefaultModificationRequest>
 * }
 * </pre>
 *
 * <br>
 * DELETE Example when removing a single value using Column Visibility: <br>
 *
 * <pre>
 * {@code
 *
 * <DefaultModificationRequest>
 *   <Events>
 *     <Event>
 *       <shardId>20120731_9</shardId>
 *       <datatype>myDatatype</datatype>
 *       <eventUid>0000-0000-0000-0000</eventUid>
 *     </Event>
 *   </Events>
 *   <mode>DELETE</mode>
 *   <fieldName>TEST</fieldName>
 *   <fieldValue>ABC</fieldValue>
 *   <columnVisibility>PRIVATE</columnVisibility>
 * </DefaultModificationRequest>
 * }
 * </pre>
 *
 * <br>
 * DELETE Example when removing all entries for a field with a specific value (different column visibilities): <br>
 *
 * <pre>
 * {@code
 * <DefaultModificationRequest>
 *   <Events>
 *     <Event>
 *       <shardId>20120731_9</shardId>
 *       <datatype>myDatatype</datatype>
 *       <eventUid>0000-0000-0000-0000</eventUid>
 *     </Event>
 *   </Events>
 *   <mode>DELETE</mode>
 *   <fieldName>TEST</fieldName>
 *   <fieldValue>ABC</fieldValue>
 * </DefaultModificationRequest>
 * }
 * </pre>
 *
 * <br>
 * UPDATE Example when removing a single value and replacing it with a new one using ColumnVisibilities:<br>
 *
 * <pre>
 * {@code
 * <DefaultModificationRequest>
 *   <Events>
 *     <Event>
 *       <shardId>20120731_9</shardId>
 *       <datatype>myDatatype</datatype>
 *       <eventUid>0000-0000-0000-0000</eventUid>
 *     </Event>
 *   </Events>
 *   <mode>UPDATE</mode>
 *   <fieldName>TEST</fieldName>
 *   <fieldValue>XYZ</fieldValue>
 *   <columnVisibility>PRIVATE|PUBLIC</columnVisibility>
 *   <oldFieldValue>ABC</oldFieldValue>
 *   <oldColumnVisibility>PRIVATE</oldColumnVisibility>
 * </DefaultModificationRequest>
 * }
 * </pre>
 *
 * <br>
 * * UPDATE Example removing all entries for a field with a specific value (different column visibilities) and replacing it with a new one:<br>
 *
 * <pre>
 * {@code
 *
 * <DefaultModificationRequest>
 *   <Events>
 *     <Event>
 *       <shardId>20120731_9</shardId>
 *       <datatype>myDatatype</datatype>
 *       <eventUid>0000-0000-0000-0000</eventUid>
 *     </Event>
 *   </Events>
 *   <mode>UPDATE</mode>
 *   <fieldName>TEST</fieldName>
 *   <fieldValue>XYZ</fieldValue>
 *   <oldFieldValue>ABC</oldFieldValue>
 *   <columnVisibility>PRIVATE|PUBLIC</columnVisibility>
 * </DefaultModificationRequest>
 * }
 * </pre>
 *
 */
public class MutableMetadataHandler extends ModificationServiceConfiguration {

    private Logger log = LoggerFactory.getLogger(this.getClass());

    protected static final long MS_PER_DAY = TimeUnit.DAYS.toMillis(1);
    protected static final String DESCRIPTION = "Modification service that processes insert, update, and delete requests of event fields for event(s) identified by the shard id, datatype, and event uid.";
    public static final String FIELD_INDEX_PREFIX = "fi\0";
    protected static final String NULL_BYTE = "\0";
    protected static final String MAX_CHAR = new String(Character.toChars(Character.MAX_CODE_POINT));
    protected static final Value NULL_VALUE = new Value(new byte[0]);
    public static final String HISTORY_PREFIX = "HISTORY_";

    protected String eventTableName = null;
    protected String indexTableName = null;
    protected String reverseIndexTableName = null;
    protected String metadataTableName = null;
    protected MetadataHelperFactory metadataHelperFactory;
    protected MarkingFunctions markingFunctions = null;

    // a map of event fields to index only/derived fields to enable appropriate deleting of event fields and all derivatives
    protected Multimap<String,String> indexOnlyMap = null;

    // a set of token suffixes to include to enable appropriate deleting of event fields and all derivatives
    protected Set<String> indexOnlySuffixes = null;

    // a list of event fields that map to content
    protected Set<String> contentFields = null;

    public String getEventTableName() {
        return eventTableName;
    }

    public void setEventTableName(String eventTableName) {
        this.eventTableName = eventTableName;
    }

    public String getIndexTableName() {
        return indexTableName;
    }

    public void setIndexTableName(String indexTableName) {
        this.indexTableName = indexTableName;
    }

    public String getReverseIndexTableName() {
        return reverseIndexTableName;
    }

    public void setReverseIndexTableName(String reverseIndexTableName) {
        this.reverseIndexTableName = reverseIndexTableName;
    }

    public String getMetadataTableName() {
        return metadataTableName;
    }

    public void setMetadataTableName(String metadataTableName) {
        this.metadataTableName = metadataTableName;
    }

    public MarkingFunctions getMarkingFunctions() {
        return markingFunctions;
    }

    public void setMarkingFunctions(MarkingFunctions markingFunctions) {
        this.markingFunctions = markingFunctions;
    }

    public Multimap<String,String> getIndexOnlyMap() {
        return indexOnlyMap;
    }

    // this is set from a spring configuration where the value is comma delimited
    public void setIndexOnlyMap(Map<String,String> map) {
        this.indexOnlyMap = HashMultimap.create();
        for (Map.Entry<String,String> entry : map.entrySet()) {
            for (String value : StringUtils.split(entry.getValue(), ',')) {
                this.indexOnlyMap.put(entry.getKey(), value.trim());
            }
        }
    }

    public Set<String> getContentFields() {
        return contentFields;
    }

    public void setContentFields(Set<String> fields) {
        this.contentFields = fields;
    }

    public Set<String> getIndexOnlySuffixes() {
        return indexOnlySuffixes;
    }

    public void setIndexOnlySuffixes(Set<String> suffixes) {
        this.indexOnlySuffixes = suffixes;
    }

    public MetadataHelperFactory getMetadataHelperFactory() {
        return metadataHelperFactory;
    }

    public void setMetadataHelperFactory(MetadataHelperFactory metadataHelperFactory) {
        this.metadataHelperFactory = metadataHelperFactory;
    }

    @Override
    public String getDescription() {
        return DESCRIPTION;
    }

    @Override
    public Class<? extends ModificationRequestBase> getRequestClass() {
        return DefaultModificationRequest.class;
    }

    // Default the insert history option to true so that the call remains backwards compatible.
    @Override
    public void process(AccumuloClient client, ModificationRequestBase request, Map<String,Set<String>> mutableFieldList, Set<Authorizations> userAuths,
                    ProxiedUserDetails userDetails) throws Exception {
        this.process(client, request, mutableFieldList, userAuths, userDetails, false, true);
    }

    public void process(AccumuloClient client, ModificationRequestBase request, Map<String,Set<String>> mutableFieldList, Set<Authorizations> userAuths,
                    ProxiedUserDetails userDetails, boolean purgeIndex, boolean insertHistory) throws Exception {

        String user = userDetails.getShortName();

        DefaultModificationRequest mr = DefaultModificationRequest.class.cast(request);

        if (null == mr.getEvents() || mr.getEvents().isEmpty()) {
            throw new IllegalArgumentException("No events specified for modification");
        }

        String fieldName = mr.getFieldName();
        MetadataHelper helper = getMetadataHelper(client);
        MODE mode = mr.getMode();
        MultiTableBatchWriter writer = client
                        .createMultiTableBatchWriter(new BatchWriterConfig().setMaxLatency(1, TimeUnit.SECONDS).setMaxMemory(1048576L).setMaxWriteThreads(4));
        try {
            for (EventIdentifier e : mr.getEvents()) {
                String shardId = e.getShardId();
                String datatype = e.getDatatype();
                Set<String> datatypeFilter = Collections.singleton(datatype);
                String eventUid = e.getEventUid();

                String oldFieldValue = null;
                Map<String,String> oldFieldMarkings = null;
                String oldColumnVisibility = null;
                List<Pair<Key,Value>> currentEntryList = null;
                int valHistoryCount = 0;

                /*
                 * Makes all fields mutable for services requiring no history.
                 */
                if (insertHistory && !isFieldMutable(mutableFieldList, datatype, fieldName))
                    throw new IllegalArgumentException("Field " + fieldName + " is not mutable");

                boolean isIndexed = helper.isIndexed(fieldName, datatypeFilter);
                boolean isReverseIndexed = helper.isReverseIndexed(fieldName, datatypeFilter);
                boolean isIndexOnly = helper.getIndexOnlyFields(datatypeFilter).contains(fieldName);
                boolean isContent = (contentFields != null && contentFields.contains(fieldName));
                Set<Type<?>> dataTypes = helper.getDatatypesForField(fieldName, Collections.singleton(datatype));

                if ((isIndexed || isReverseIndexed || isIndexOnly) && (null == dataTypes || dataTypes.isEmpty()))
                    throw new IllegalStateException("Field " + fieldName + " is marked index only but has no dataTypes");

                long origTimestamp = getOriginalEventTimestamp(client, userAuths, shardId, datatype, eventUid);

                // Count the history entries if history is going to be inserted.
                if (insertHistory && (MODE.INSERT.equals(mode) || MODE.UPDATE.equals(mode))) {
                    List<Pair<Key,Value>> fieldHistoryList = getField(client, userAuths, shardId, datatype, eventUid, "HISTORY_" + fieldName, null,
                                    new HashMap<>(), null);

                    for (Pair<Key,Value> p : fieldHistoryList) {
                        if (p.getFirst().getColumnQualifier().find(mr.getFieldValue()) > -1) {
                            ++valHistoryCount;
                        }
                    }
                }

                if (MODE.UPDATE.equals(mode) || MODE.DELETE.equals(mode)) {
                    if (MODE.UPDATE.equals(mode)) {
                        oldFieldValue = mr.getOldFieldValue();
                        oldFieldMarkings = mr.getOldFieldMarkings();
                        oldColumnVisibility = mr.getOldColumnVisibility();
                        if (null == oldFieldValue)
                            throw new IllegalArgumentException("fieldValue parameter required for update");
                    } else {
                        oldFieldValue = mr.getFieldValue();
                        oldFieldMarkings = mr.getFieldMarkings();
                        oldColumnVisibility = mr.getColumnVisibility();
                        if (null == oldFieldValue)
                            throw new IllegalArgumentException("fieldValue parameter required for delete");
                    }
                    ColumnVisibility oldViz = null;
                    if (null != oldColumnVisibility) {
                        oldViz = new ColumnVisibility(oldColumnVisibility);
                    }

                    // find the current values
                    currentEntryList = getField(client, userAuths, shardId, datatype, eventUid, fieldName, oldFieldValue, oldFieldMarkings, oldViz);
                    if (oldFieldValue != null && currentEntryList.isEmpty()) {
                        throw new IllegalArgumentException("Modification request rejected. Current value of " + fieldName + " does not match submitted value.");
                    }
                } else {
                    if (null == mr.getFieldValue())
                        throw new IllegalArgumentException("fieldValue parameter required for insert");
                }

                if (MODE.INSERT.equals(mode)) {
                    String fieldValue = mr.getFieldValue();
                    Map<String,String> fieldMarkings = mr.getFieldMarkings();
                    String columnVisibility = mr.getColumnVisibility();
                    ColumnVisibility colviz = null;
                    if (null != columnVisibility) {
                        colviz = new ColumnVisibility(columnVisibility);
                    }
                    insert(writer, shardId, datatype, eventUid, fieldMarkings, colviz, fieldName, fieldValue, isIndexOnly, isIndexed, isReverseIndexed,
                                    dataTypes, user, MODE.INSERT, origTimestamp + valHistoryCount, insertHistory);
                } else if (MODE.DELETE.equals(mode)) {
                    delete(writer, client, userAuths, currentEntryList, isIndexOnly, isIndexed, isReverseIndexed, isContent, dataTypes, user, MODE.DELETE,
                                    origTimestamp + valHistoryCount, purgeIndex, insertHistory);
                } else {
                    delete(writer, client, userAuths, currentEntryList, isIndexOnly, isIndexed, isReverseIndexed, isContent, dataTypes, user, MODE.UPDATE,
                                    origTimestamp + valHistoryCount, purgeIndex, insertHistory);
                    String fieldValue = mr.getFieldValue();
                    Map<String,String> fieldMarkings = mr.getFieldMarkings();
                    String columnVisibility = mr.getColumnVisibility();
                    ColumnVisibility colviz = null;
                    if (null != columnVisibility) {
                        colviz = new ColumnVisibility(columnVisibility);
                    }
                    insert(writer, shardId, datatype, eventUid, fieldMarkings, colviz, fieldName, fieldValue, isIndexOnly, isIndexed, isReverseIndexed,
                                    dataTypes, user, MODE.UPDATE, origTimestamp + valHistoryCount, insertHistory);
                }
            }
        } finally {
            writer.close();
        }
    }

    /**
     * Insert new field value with provided timestamp
     *
     * @param writer
     *            the batch writer
     * @param shardId
     *            the shard id
     * @param datatype
     *            the datatype
     * @param eventUid
     *            the event uid
     * @param viz
     *            the column visibility
     * @param fieldName
     *            field name string
     * @param fieldValue
     *            field value string
     * @param timestamp
     *            timestamp
     * @param isIndexed
     *            boolean flag for isIndexed
     * @param isReverseIndexed
     *            boolean flag for isReverseIndexed
     * @param dataTypes
     *            set of datatypes
     * @param historicalValue
     *            historical value flag
     * @param insertHistory
     *            flag for insertHistory
     * @param user
     *            the user string
     * @param mode
     *            the mode
     * @param isIndexOnlyField
     *            isIndexOnlyField
     * @throws Exception
     *             if there are issues
     */
    protected void insert(MultiTableBatchWriter writer, String shardId, String datatype, String eventUid, ColumnVisibility viz, String fieldName,
                    String fieldValue, long timestamp, boolean isIndexOnlyField, boolean isIndexed, boolean isReverseIndexed, Set<Type<?>> dataTypes,
                    boolean historicalValue, boolean insertHistory, String user, MODE mode) throws Exception {

        // increment the term frequency
        Mutation m = new Mutation(fieldName);
        if (!isIndexOnlyField) {
            m.put(ColumnFamilyConstants.COLF_E, new Text(datatype), NULL_VALUE);
            m.put(ColumnFamilyConstants.COLF_F, new Text(datatype + NULL_BYTE + DateHelper.format(timestamp)),
                            new Value(SummingCombiner.VAR_LEN_ENCODER.encode(1L)));
        }

        // Insert the new field.
        Mutation e = new Mutation(shardId);
        if (!isIndexOnlyField) {
            e.put(new Text(datatype + NULL_BYTE + eventUid), new Text(fieldName + NULL_BYTE + fieldValue), viz, timestamp, NULL_VALUE);
        }

        if (isIndexed) {

            long tsToDay = (timestamp / MS_PER_DAY) * MS_PER_DAY;

            // Create a UID object for the Value
            Builder uidBuilder = Uid.List.newBuilder();
            uidBuilder.setIGNORE(false);
            uidBuilder.setCOUNT(1);
            uidBuilder.addUID(eventUid);
            Uid.List uidList = uidBuilder.build();
            Value val = new Value(uidList.toByteArray());

            for (Type<?> n : dataTypes) {
                String indexTerm = fieldValue;
                if (historicalValue) {
                    int lastColon = fieldValue.lastIndexOf(":");
                    // The next two lines build up to the beginning of the indexTerm by finding the first two colons
                    // We could use split if we could guarantee a colon never appears in the index term itself
                    int indexTermLeadingColon = fieldValue.indexOf(":", 0);
                    indexTermLeadingColon = fieldValue.indexOf(":", indexTermLeadingColon + 1);
                    indexTerm = fieldValue.substring(indexTermLeadingColon + 1, lastColon);
                }
                String indexedValue = n.normalize(indexTerm);

                // Insert the global index entry
                Mutation i = new Mutation(indexedValue);
                i.put(fieldName, shardId + NULL_BYTE + datatype, viz, tsToDay, val);
                writer.getBatchWriter(this.getIndexTableName()).addMutation(i);
                m.put(ColumnFamilyConstants.COLF_I, new Text(datatype + NULL_BYTE + n.getClass().getName()), NULL_VALUE);

                if (isReverseIndexed) {
                    String reverseIndexedValue = StringUtils.reverse(indexedValue);
                    // Insert the global reverse index entry
                    Mutation rm = new Mutation(reverseIndexedValue);
                    rm.put(fieldName, shardId + NULL_BYTE + datatype, viz, tsToDay, val);
                    writer.getBatchWriter(this.getReverseIndexTableName()).addMutation(rm);
                    m.put(ColumnFamilyConstants.COLF_RI, new Text(datatype + NULL_BYTE + n.getClass().getName()), NULL_VALUE);
                }
                // Insert the field index entry
                e.put(new Text(FIELD_INDEX_PREFIX + fieldName), new Text(indexedValue + NULL_BYTE + datatype + NULL_BYTE + eventUid), viz, timestamp,
                                NULL_VALUE);
            }
        }
        writer.getBatchWriter(this.getEventTableName()).addMutation(e);
        writer.getBatchWriter(this.getMetadataTableName()).addMutation(m);
        writer.flush();

        if (!isIndexOnlyField && insertHistory) {
            insertHistory(writer, shardId, datatype, eventUid, viz, fieldName, fieldValue, timestamp, isIndexOnlyField, isIndexed, isReverseIndexed, dataTypes,
                            user, mode);
        }
    }

    /**
     * Prepares the value to be inserted as history, then calls insert
     *
     * @param writer
     *            the batch writer
     * @param shardId
     *            the shard id
     * @param datatype
     *            the datatype
     * @param eventUid
     *            the event uid
     * @param viz
     *            the column visibility
     * @param fieldName
     *            field name string
     * @param fieldValue
     *            field value string
     * @param timestamp
     *            timestamp
     * @param isIndexed
     *            boolean flag for isIndexed
     * @param isReverseIndexed
     *            boolean flag for isReverseIndexed
     * @param dataTypes
     *            set of datatypes
     * @param user
     *            the user string
     * @param mode
     *            the mode
     * @param isIndexOnlyField
     *            isIndexOnlyField
     * @throws Exception
     *             if there are issues
     */
    protected void insertHistory(MultiTableBatchWriter writer, String shardId, String datatype, String eventUid, ColumnVisibility viz, String fieldName,
                    String fieldValue, long timestamp, boolean isIndexOnlyField, boolean isIndexed, boolean isReverseIndexed, Set<Type<?>> dataTypes,
                    String user, MODE mode) throws Exception {
        // Capture the fact of the insert in a history element
        // History element has the following structure
        // FIELD NAME: HISTORY_<ORIGINAL FIELD NAME>
        // for Deletes - FIELD VALUE: <timestamp of original field value> : <user that modified/deleted it> : < original field value> : <operation type>
        // for Inserts - FIELD VALUE: <timestamp of operation> : <user that modified it> : < original field value> : <operation type>
        // Timestamp of history element is now.
        String historyFieldName = HISTORY_PREFIX + fieldName;
        String historyFieldValue;
        if (mode.equals(MODE.INSERT)) {
            historyFieldValue = System.currentTimeMillis() + ":" + user + ":" + fieldValue;
            historyFieldValue += ":insert";
        } else if (mode.equals(MODE.DELETE)) {
            historyFieldValue = System.currentTimeMillis() + ":" + user + ":" + fieldValue;
            historyFieldValue += ":delete";
        } else { // update
            historyFieldValue = System.currentTimeMillis() + ":" + user + ":" + fieldValue;
            historyFieldValue += ":update";
        }

        insert(writer, shardId, datatype, eventUid, viz, historyFieldName, historyFieldValue, timestamp, isIndexOnlyField, isIndexed, isReverseIndexed,
                        dataTypes, true, false, user, mode);
    }

    /**
     * Insert new field value with original event timestamp
     *
     * @param writer
     *            the batch writer
     * @param shardId
     *            the shard id
     * @param datatype
     *            the datatype
     * @param eventUid
     *            the event uid
     * @param viz
     *            the column visibility
     * @param fieldName
     *            field name string
     * @param fieldValue
     *            field value string
     * @param isIndexOnlyField
     *            isIndexOnlyField
     * @param insertHistory
     *            insert history flag
     * @param markings
     *            markings
     * @param ts
     *            timestamp
     * @param isIndexed
     *            boolean flag for isIndexed
     * @param isReverseIndexed
     *            boolean flag for isReverseIndexed
     * @param dataTypes
     *            set of datatypes
     * @param user
     *            the user string
     * @param mode
     *            the mode
     * @throws Exception
     *             if there are issues
     */
    protected void insert(MultiTableBatchWriter writer, String shardId, String datatype, String eventUid, Map<String,String> markings, ColumnVisibility viz,
                    String fieldName, String fieldValue, boolean isIndexOnlyField, boolean isIndexed, boolean isReverseIndexed, Set<Type<?>> dataTypes,
                    String user, MODE mode, long ts, boolean insertHistory) throws Exception {

        if (null == viz) {
            if (null == markings || markings.isEmpty())
                throw new IllegalArgumentException("No security information specified. Security markings must be supplied");

            viz = markingFunctions.translateToColumnVisibility(markings);
        }

        insert(writer, shardId, datatype, eventUid, viz, fieldName, fieldValue, ts, isIndexOnlyField, isIndexed, isReverseIndexed, dataTypes, false,
                        insertHistory, user, mode);
    }

    /**
     * Delete the current K,V from the event, put in a history element
     *
     * @param writer
     *            the batch writer
     * @param currentEntryList
     *            the current entry list
     * @param isIndexed
     *            boolean flag for isIndexed
     * @param isReverseIndexed
     *            boolean flag for isReverseIndexed
     * @param isContentField
     *            boolean flag for isContentField
     * @param dataTypes
     *            set of datatypes
     * @param user
     *            the user string
     * @param mode
     *            the mode
     * @param isIndexOnlyField
     *            isIndexOnlyField
     * @param client
     *            a client
     * @param userAuths
     *            userAuths
     * @param ts
     *            the timestamp
     * @param purgeTokens
     *            If set true, then this will delete all tokens for a field as well.
     * @param insertHistory
     *            flag to insert history
     * @throws Exception
     *             if there are issues
     */
    protected void delete(MultiTableBatchWriter writer, AccumuloClient client, Set<Authorizations> userAuths, List<Pair<Key,Value>> currentEntryList,
                    boolean isIndexOnlyField, boolean isIndexed, boolean isReverseIndexed, boolean isContentField, Set<Type<?>> dataTypes, String user,
                    MODE mode, long ts, boolean purgeTokens, boolean insertHistory) throws Exception {

        for (Pair<Key,Value> currentEntry : currentEntryList) {

            ColumnVisibility viz = currentEntry.getFirst().getColumnVisibilityParsed();

            DatawaveKey key = new DatawaveKey(currentEntry.getFirst());

            String shardId = key.getRow().toString();

            long currentEntryTimestamp = currentEntry.getFirst().getTimestamp();

            if (key.getType().equals(KeyType.INDEX_EVENT)) {
                // Only the delete the fi key
                Mutation e = new Mutation(currentEntry.getFirst().getRow());
                e.putDelete(currentEntry.getFirst().getColumnFamily(), currentEntry.getFirst().getColumnQualifier(), viz, currentEntryTimestamp);
                writer.getBatchWriter(this.getEventTableName()).addMutation(e);
            } else if (key.getType().equals(KeyType.EVENT)) {
                Mutation m = new Mutation(key.getFieldName());

                // Decrement the frequency (metadata table)
                m.put(ColumnFamilyConstants.COLF_F, new Text(key.getDataType() + NULL_BYTE + DateHelper.format(currentEntryTimestamp)),
                                new Value(SummingCombiner.VAR_LEN_ENCODER.encode(-1L)));
                // Remove the event field.
                Mutation e = new Mutation(currentEntry.getFirst().getRow());
                if (!isIndexOnlyField) {
                    e.putDelete(currentEntry.getFirst().getColumnFamily(), currentEntry.getFirst().getColumnQualifier(), viz, currentEntryTimestamp);
                }

                // Remove the content column
                if (isContentField) {
                    ContentIterable dKeys = getContentKeys(client, this.getEventTableName(), userAuths, shardId, key.getDataType(), key.getUid());
                    try {
                        for (Key dKey : dKeys) {
                            e.putDelete(dKey.getColumnFamily(), dKey.getColumnQualifier(), dKey.getColumnVisibilityParsed(), dKey.getTimestamp());
                        }
                    } finally {
                        dKeys.close();
                    }
                }

                long tsToDay = (ts / MS_PER_DAY) * MS_PER_DAY;

                FieldIndexIterable fiKeys = getFieldIndexKeys(client, this.getEventTableName(), userAuths, shardId, key.getDataType(), key.getUid(),
                                key.getFieldName(), key.getFieldValue(), dataTypes, purgeTokens);
                try {
                    for (Key fiKey : fiKeys) {
                        // Remove the field index entry
                        e.putDelete(fiKey.getColumnFamily(), fiKey.getColumnQualifier(), fiKey.getColumnVisibilityParsed(), fiKey.getTimestamp());

                        DatawaveKey fiKeyParsed = new DatawaveKey(fiKey);

                        // Remove the term frequency entry
                        e.putDelete(ColumnFamilyConstants.COLF_TF.toString(), fiKeyParsed.getDataType() + NULL_BYTE + fiKeyParsed.getUid() + NULL_BYTE
                                        + fiKeyParsed.getFieldValue() + NULL_BYTE + fiKeyParsed.getFieldName(), fiKey.getColumnVisibilityParsed(),
                                        fiKey.getTimestamp());

                        // Create a UID object for the Value which will remove this UID
                        Builder uidBuilder = Uid.List.newBuilder();
                        uidBuilder.setIGNORE(false);
                        uidBuilder.setCOUNT(-1);
                        uidBuilder.addUID(fiKeyParsed.getUid());
                        Uid.List uidList = uidBuilder.build();
                        Value val = new Value(uidList.toByteArray());

                        // buffer the global indexes cq
                        String cq = shardId + NULL_BYTE + fiKeyParsed.getDataType();

                        // Remove the global index entry by adding the value
                        Mutation i = new Mutation(fiKeyParsed.getFieldValue());
                        i.put(fiKeyParsed.getFieldName(), cq, fiKey.getColumnVisibilityParsed(), tsToDay, val);
                        writer.getBatchWriter(this.getIndexTableName()).addMutation(i);

                        // Remove the reverse global index entry
                        if (isReverseIndexed) {
                            String reverseIndexedValue = StringUtils.reverse(fiKeyParsed.getFieldValue());
                            Mutation ri = new Mutation(reverseIndexedValue);
                            ri.put(fiKeyParsed.getFieldName(), cq, viz, tsToDay, val);
                            writer.getBatchWriter(this.getReverseIndexTableName()).addMutation(ri);
                        }
                    }
                } finally {
                    fiKeys.close();
                }

                if (e.size() > 0) {
                    writer.getBatchWriter(this.getEventTableName()).addMutation(e);
                }

                writer.getBatchWriter(this.getMetadataTableName()).addMutation(m);

                if (!isIndexOnlyField && insertHistory) {
                    insertHistory(writer, shardId, key.getDataType(), key.getUid(), viz, key.getFieldName(), key.getFieldValue(), ts, isIndexOnlyField,
                                    isIndexed, isReverseIndexed, dataTypes, user, mode);
                }
            }
        }
        writer.flush();
    }

    /**
     * Get the Key,Value pair for the field to be updated/deleted from the event table
     *
     * @param client
     *            the client
     * @param userAuths
     *            set of user auths
     * @param shardId
     *            the shard id
     * @param datatype
     *            the datatype
     * @param eventUid
     *            the event uid
     * @param fieldName
     *            field name string
     * @param oldFieldValue
     *            old field value
     * @param oldFieldMarkings
     *            the old field markings
     * @param oldColumnVisibility
     *            the old column visibility
     * @return the Key,Value pair for the field to be updated/deleted from the event table
     * @throws Exception
     *             if there are issues
     */
    protected List<Pair<Key,Value>> getField(AccumuloClient client, Set<Authorizations> userAuths, String shardId, String datatype, String eventUid,
                    String fieldName, String oldFieldValue, Map<String,String> oldFieldMarkings, ColumnVisibility oldColumnVisibility) throws Exception {

        Text family = new Text(datatype);
        TextUtil.textAppend(family, eventUid);

        Text qualifier = null;

        if (oldFieldValue != null) {
            qualifier = new Text(fieldName);
            TextUtil.textAppend(qualifier, oldFieldValue);
        }

        List<Pair<Key,Value>> results = new ArrayList<>();

        Scanner s = ScannerHelper.createScanner(client, this.getEventTableName(), userAuths);
        try {
            s.setRange(new Range(shardId));
            if (qualifier == null) {
                s.fetchColumnFamily(family);
            } else {
                s.fetchColumn(family, qualifier);
            }

            for (Entry<Key,Value> e : s) {
                ColumnVisibility thisViz = new ColumnVisibility(e.getKey().getColumnVisibility());

                if (!e.getKey().getColumnQualifier().toString().startsWith(fieldName)) {
                    continue;
                }

                if (null != oldColumnVisibility) {
                    // need to compare the flattened values for equivalence. It's possible for the visibility to be in a different order
                    String oldColViz = new String(oldColumnVisibility.flatten(), "UTF-8");
                    String thisVis = new String(thisViz.flatten(), "UTF-8");
                    if (!oldColViz.equals(thisVis)) {
                        log.trace("Skipping key that does not match with column visibility: {}", e.getKey());
                        continue;
                    }
                } else {
                    Map<String,String> markings = markingFunctions.translateFromColumnVisibilityForAuths(e.getKey().getColumnVisibilityParsed(), userAuths);
                    if (null != oldFieldMarkings && !oldFieldMarkings.equals(markings)) {
                        log.trace("Skipping key that does not match with markings: {}", e.getKey());
                        continue;
                    }
                }
                results.add(new Pair<>(e.getKey(), e.getValue()));
            }
        } finally {
            s.close();
        }
        return results;
    }

    /**
     * Pulls the entire event and returns the most common timestamp for the event. This *assumes* the most common timestamp is the original one. If this is not
     * the case, then it needs to change. Another option could be to return the earliest timestamp, which is also an assumption.
     *
     * @param client
     *            the client
     * @param userAuths
     *            set of user auths
     * @param shardId
     *            the shard id
     * @param datatype
     *            the datatype
     * @param eventUid
     *            the event uid
     * @return long - highestOccurrenceTimestamp - most common timestamp in the event
     * @throws Exception
     *             if there are issues
     */
    protected long getOriginalEventTimestamp(AccumuloClient client, Set<Authorizations> userAuths, String shardId, String datatype, String eventUid)
                    throws Exception {

        Text family = new Text(datatype);
        TextUtil.textAppend(family, eventUid);

        HashMap<Long,Integer> timestampCounts = new HashMap<>();

        long highestOccurrenceTimestamp = 0;
        int highestOccurrences = -1;

        // Pull the entire event
        Scanner s = ScannerHelper.createScanner(client, this.getEventTableName(), userAuths);
        try {
            s.setRange(new Range(shardId));
            s.fetchColumnFamily(family);

            // Populate map with how often each timestamp occurs
            for (Entry<Key,Value> e : s) {
                long ts = e.getKey().getTimestamp();
                if (!timestampCounts.containsKey(ts)) {
                    timestampCounts.put(ts, 1);
                } else {
                    timestampCounts.put(ts, timestampCounts.get(ts) + 1);
                }
            }

            // Determine the most common timestamp
            if (timestampCounts.isEmpty()) {
                // if no fields exist, then use the shard date at 00:00:00
                highestOccurrenceTimestamp = DateHelper.parse(shardId.substring(0, 8)).getTime();
            } else {
                for (Entry<Long,Integer> entry : timestampCounts.entrySet()) {
                    Long ts = entry.getKey();
                    int occurrences = entry.getValue();
                    if (occurrences > highestOccurrences) {
                        highestOccurrences = occurrences;
                        highestOccurrenceTimestamp = ts;
                    }
                }
            }
        } finally {
            s.close();
        }

        return highestOccurrenceTimestamp;
    }

    /**
     * Get an instance of a MetadataHelper object
     *
     * @param client
     *            the client
     * @return instance of a MetadataHelper object
     * @throws AccumuloException
     *             for issues with accumulo
     * @throws AccumuloSecurityException
     *             for accumulo authentication issues
     * @throws TableNotFoundException
     *             if the table is not found
     * @throws ExecutionException
     *             for execution exceptions
     */
    protected MetadataHelper getMetadataHelper(AccumuloClient client)
                    throws AccumuloException, AccumuloSecurityException, TableNotFoundException, ExecutionException {
        Authorizations auths = client.securityOperations().getUserAuthorizations(client.whoami());
        return metadataHelperFactory.createMetadataHelper(client, this.getMetadataTableName(), Collections.singleton(auths));
    }

    /**
     * Check to see if a field is mutable
     *
     * @param mutableFieldList
     *            a mutable field list map
     * @param datatype
     *            the datatype
     * @param fieldName
     *            field name string
     * @return if a field is mutable
     */
    protected boolean isFieldMutable(Map<String,Set<String>> mutableFieldList, String datatype, String fieldName) {
        if (null == mutableFieldList)
            return false;
        if (null == mutableFieldList.get(datatype))
            return false;
        return mutableFieldList.get(datatype).contains(fieldName);
    }

    /**
     * @return priority of the connection for this service
     */
    @Override
    public AccumuloConnectionFactory.Priority getPriority() {
        return AccumuloConnectionFactory.Priority.NORMAL;
    }

    /**
     * Finds the event by creating a query by UUID from the keys and values in the runtime parameters.
     *
     * @param uuid
     *            the uuid
     * @param uuidType
     *            type of uuid
     * @param operation
     *            operation
     * @param userAuths
     *            set of user auths
     * @return Event
     * @throws Exception
     *             if there are issues
     */
    protected EventBase<?,?> findMatchingEventUuid(String uuid, String uuidType, Set<Authorizations> userAuths, ModificationOperation operation,
                    ProxiedUserDetails userDetails) throws Exception {
        String field = operation.getFieldName();
        String columnVisibility = operation.getColumnVisibility();

        // query in format uuidType:uuid
        StringBuilder query = new StringBuilder();
        query.append(uuidType.toUpperCase()).append(":\"").append(uuid).append("\"");
        // make the query only return the field to be modified and the UUIDType (avoids NoResultsException on an insert where the field has no values)
        StringBuilder queryOptions = new StringBuilder();
        queryOptions.append("query.syntax:LUCENE-UUID;raw.data.only:true");
        if (field != null) {
            queryOptions.append(";return.fields:").append(field.toUpperCase()).append(",").append(uuidType.toUpperCase());
        }

        String logicName = "LuceneUUIDEventQuery";

        EventBase e = null;
        ModificationQueryService queryService = this.getQueryService(userDetails);
        String id = null;
        HashSet<String> auths = new HashSet<>();
        for (Authorizations a : userAuths)
            auths.addAll(Arrays.asList(a.toString().split(",")));

        Date expiration = new Date();
        expiration = new Date(expiration.getTime() + (1000 * 60 * 60 * 24));

        try {
            GenericResponse<String> createResponse = queryService.createQuery(logicName,
                            DefaultQueryParameters.paramsToMap(logicName, query.toString(), "Query to find matching records for metadata modification",
                                            columnVisibility, new Date(0), new Date(), StringUtils.join(auths, ','), expiration, 2, -1, null,
                                            QueryPersistence.TRANSIENT, null, queryOptions.toString(), false));

            id = createResponse.getResult();
            BaseQueryResponse response = queryService.next(id);
            if (response instanceof EventQueryResponseBase) {
                EventQueryResponseBase eResponse = (EventQueryResponseBase) response;
                if (eResponse.getEvents().size() > 1) {
                    throw new IllegalStateException("More than one event matched " + uuid + " (" + eResponse.getEvents().size() + " matched)");
                }
                if (eResponse.getEvents().isEmpty()) {
                    throw new IllegalStateException("No event matched " + uuid);
                }

                e = eResponse.getEvents().get(0);
            } else {
                log.error("Received a {} but expected a subclass of EventQueryResponseBase", response.getClass().getSimpleName());
            }
        } catch (Exception ex) {
            log.error("", ex);
        } finally {
            if (id != null) {
                queryService.close(id);
            }
        }

        return e;

    }

    /**
     * Find the field index keys associated with a specified field and a specified event. Note that this will return all keys for that field and configured
     * token fields if includeTokens is true.
     *
     * @param client
     *            the connector
     * @param shardTable
     *            shard table name
     * @param userAuths
     *            set of user auths
     * @param shardId
     *            the shard id
     * @param datatype
     *            the datatype
     * @param eventUid
     *            the event uid
     * @param fieldName
     *            the field name string
     * @param fieldValue
     *            field value string
     * @param dataTypes
     *            set of datatypes
     * @param includeTokens
     *            If true then this will return all associated token fields as well
     * @return An iterable of Keys
     * @throws Exception
     *             if there are issues
     */
    protected FieldIndexIterable getFieldIndexKeys(AccumuloClient client, String shardTable, Set<Authorizations> userAuths, String shardId, String datatype,
                    String eventUid, String fieldName, String fieldValue, Set<Type<?>> dataTypes, boolean includeTokens) throws Exception {

        List<Range> ranges = new ArrayList();
        fieldName = stripGrouping(fieldName);
        if (includeTokens) {
            // if we are including everything, then include the mapped tokenized fields as well.
            List<String> fields = new ArrayList();
            fields.add(fieldName);
            if (this.indexOnlyMap != null) {
                fields.addAll(this.indexOnlyMap.get(fieldName));
            }
            if (this.indexOnlySuffixes != null) {
                for (String suffix : indexOnlySuffixes) {
                    fields.add(fieldName + suffix);
                }
            }
            for (String field : fields) {
                Key startKey = new Key(shardId, "fi" + NULL_BYTE + field);
                Key endKey = startKey.followingKey(PartialKey.ROW_COLFAM);
                Range range = new Range(startKey, true, endKey, false);
                ranges.add(range);
            }
        } else {
            for (Type<?> n : dataTypes) {
                String indexedValue = n.normalize(fieldValue);
                Key startKey = new Key(shardId, "fi" + NULL_BYTE + fieldName, indexedValue + NULL_BYTE + datatype + NULL_BYTE);
                Key endKey = new Key(shardId, "fi" + NULL_BYTE + fieldName, indexedValue + NULL_BYTE + datatype + NULL_BYTE + MAX_CHAR);
                Range range = new Range(startKey, true, endKey, true);
                ranges.add(range);
            }
        }
        return new FieldIndexIterable(client, shardTable, eventUid, datatype, userAuths, ranges);
    }

    protected static class FieldIndexIterable implements Iterable<Key>, AutoCloseable {
        private BatchScanner scanner;

        public FieldIndexIterable(AccumuloClient client, String shardTable, String eventUid, String datatype, Set<Authorizations> userAuths, List<Range> ranges)
                        throws TableNotFoundException {
            if (!ranges.isEmpty()) {
                scanner = ScannerHelper.createBatchScanner(client, shardTable, userAuths, ranges.size());
                scanner.setRanges(ranges);
                Map<String,String> options = new HashMap();
                options.put(FieldIndexDocumentFilter.DATA_TYPE_OPT, datatype);
                options.put(FieldIndexDocumentFilter.EVENT_UID_OPT, eventUid);
                IteratorSetting settings = new IteratorSetting(100, FieldIndexDocumentFilter.class, options);
                scanner.addScanIterator(settings);
            }
        }

        @Override
        public Iterator<Key> iterator() {
            if (scanner != null) {
                return Iterators.transform(scanner.iterator(), new Function<Entry<Key,Value>,Key>() {
                    @Nullable
                    @Override
                    public Key apply(@Nullable Entry<Key,Value> keyValueEntry) {
                        return keyValueEntry.getKey();
                    }
                });
            } else {
                List<Key> list = Collections.emptyList();
                return list.iterator();
            }
        }

        @Override
        public void close() throws Exception {
            if (scanner != null) {
                scanner.close();
            }
        }
    }

    private String stripGrouping(String fieldName) {
        int index = fieldName.indexOf('.');
        if (index >= 0) {
            return fieldName.substring(0, index);
        } else {
            return fieldName;
        }
    }

    /**
     * Find the index only keys associated with a specified field and a specified event
     *
     * @param client
     *            the client
     * @param shardTable
     *            shard table name
     * @param userAuths
     *            set of user auths
     * @param shardId
     *            the shard id
     * @param datatype
     *            the datatype
     * @param eventUid
     *            the event uid
     * @return An iterable of Keys
     * @throws Exception
     *             if there are issues
     */
    protected ContentIterable getContentKeys(AccumuloClient client, String shardTable, Set<Authorizations> userAuths, String shardId, String datatype,
                    String eventUid) throws Exception {

        Key startKey = new Key(shardId, "d", datatype + NULL_BYTE + eventUid + NULL_BYTE);
        Key endKey = new Key(shardId, "d", datatype + NULL_BYTE + eventUid + NULL_BYTE + MAX_CHAR);
        Range range = new Range(startKey, true, endKey, false);
        return new ContentIterable(client, shardTable, eventUid, datatype, userAuths, range);
    }

    protected static class ContentIterable implements Iterable<Key>, AutoCloseable {
        private Scanner scanner;

        public ContentIterable(AccumuloClient client, String shardTable, String eventUid, String datatype, Set<Authorizations> userAuths, Range range)
                        throws TableNotFoundException {
            scanner = ScannerHelper.createScanner(client, shardTable, userAuths);
            scanner.setRange(range);
        }

        @Override
        public Iterator<Key> iterator() {
            return Iterators.transform(scanner.iterator(), new Function<Entry<Key,Value>,Key>() {
                @Nullable
                @Override
                public Key apply(@Nullable Entry<Key,Value> keyValueEntry) {
                    return keyValueEntry.getKey();
                }
            });
        }

        @Override
        public void close() throws Exception {
            scanner.close();
        }
    }

}
