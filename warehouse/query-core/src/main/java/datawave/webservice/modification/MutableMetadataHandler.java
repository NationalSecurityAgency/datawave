package datawave.webservice.modification;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import datawave.data.ColumnFamilyConstants;
import datawave.data.type.Type;
import datawave.ingest.protobuf.Uid;
import datawave.ingest.protobuf.Uid.List.Builder;
import datawave.marking.MarkingFunctions;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MetadataHelperFactory;
import datawave.security.util.ScannerHelper;
import datawave.util.TextUtil;
import datawave.util.time.DateHelper;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.modification.ModificationRequestBase.MODE;
import datawave.webservice.modification.configuration.ModificationServiceConfiguration;
import datawave.webservice.query.QueryParametersImpl;
import datawave.webservice.query.QueryPersistence;
import datawave.webservice.query.result.event.DefaultEvent;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.runner.QueryExecutorBean;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.DefaultEventQueryResponse;
import datawave.webservice.result.GenericResponse;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

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
    
    private Logger log = Logger.getLogger(this.getClass());
    
    protected static final long MS_PER_DAY = TimeUnit.DAYS.toMillis(1);
    protected static final String DESCRIPTION = "Modification service that processes insert, update, and delete requests of event fields for event(s) identified by the shard id, datatype, and event uid.";
    public static final String FIELD_INDEX_PREFIX = "fi\0";
    protected static final String NULL_BYTE = "\0";
    protected static final Value NULL_VALUE = new Value(new byte[0]);
    public static final String HISTORY_PREFIX = "HISTORY_";
    
    protected String eventTableName = null;
    protected String indexTableName = null;
    protected String reverseIndexTableName = null;
    protected String metadataTableName = null;
    protected MetadataHelperFactory metadataHelperFactory;
    protected MarkingFunctions markingFunctions = null;
    
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
    public void process(Connector con, ModificationRequestBase request, Map<String,Set<String>> mutableFieldList, Set<Authorizations> userAuths, String user)
                    throws Exception {
        this.process(con, request, mutableFieldList, userAuths, user, true);
    }
    
    public void process(Connector con, ModificationRequestBase request, Map<String,Set<String>> mutableFieldList, Set<Authorizations> userAuths, String user,
                    boolean insertHistory) throws Exception {
        
        DefaultModificationRequest mr = DefaultModificationRequest.class.cast(request);
        
        if (null == mr.getEvents() || mr.getEvents().size() == 0) {
            throw new IllegalArgumentException("No events specified for modification");
        }
        
        String fieldName = mr.getFieldName();
        MetadataHelper helper = getMetadataHelper(con);
        MODE mode = mr.getMode();
        MultiTableBatchWriter writer = con.createMultiTableBatchWriter(new BatchWriterConfig().setMaxLatency(1, TimeUnit.SECONDS).setMaxMemory(1048576L)
                        .setMaxWriteThreads(4));
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
                Set<Type<?>> dataTypes = helper.getDatatypesForField(fieldName, Collections.singleton(datatype));
                
                if ((isIndexed || isReverseIndexed || isIndexOnly) && (null == dataTypes || dataTypes.size() == 0))
                    throw new IllegalStateException("Field " + fieldName + " is marked index only but has no dataTypes");
                
                long origTimestamp = getOriginalEventTimestamp(con, userAuths, shardId, datatype, eventUid);
                
                // Count the history entries if history is going to be inserted.
                if (insertHistory && (MODE.INSERT.equals(mode) || MODE.UPDATE.equals(mode))) {
                    List<Pair<Key,Value>> fieldHistoryList = getField(con, userAuths, shardId, datatype, eventUid, "HISTORY_" + fieldName, null,
                                    new HashMap<String,String>(), null);
                    
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
                    currentEntryList = getField(con, userAuths, shardId, datatype, eventUid, fieldName, oldFieldValue, oldFieldMarkings, oldViz);
                    if (oldFieldValue != null && currentEntryList.size() == 0) {
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
                    delete(writer, currentEntryList, isIndexOnly, isIndexed, isReverseIndexed, dataTypes, user, MODE.DELETE, origTimestamp + valHistoryCount,
                                    insertHistory);
                } else {
                    delete(writer, currentEntryList, isIndexOnly, isIndexed, isReverseIndexed, dataTypes, user, MODE.UPDATE, origTimestamp + valHistoryCount,
                                    insertHistory);
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
            writer.flush();
            writer.close();
        }
    }
    
    /**
     * Insert new field value with provided timestamp
     * 
     * @param writer
     * @param shardId
     * @param datatype
     * @param eventUid
     * @param viz
     * @param fieldName
     * @param fieldValue
     * @param timestamp
     * @param isIndexed
     * @param isReverseIndexed
     * @param dataTypes
     * @param historicalValue
     * @param insertHistory
     * @param user
     * @param mode
     * @throws Exception
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
            InsertHistory(writer, shardId, datatype, eventUid, viz, fieldName, fieldValue, timestamp, isIndexOnlyField, isIndexed, isReverseIndexed, dataTypes,
                            user, mode);
        }
    }
    
    /**
     * Prepares the value to be inserted as history, then calls insert
     * 
     * @param writer
     * @param shardId
     * @param datatype
     * @param eventUid
     * @param viz
     * @param fieldName
     * @param fieldValue
     * @param timestamp
     * @param isIndexOnlyField
     * @param isIndexed
     * @param isReverseIndexed
     * @param dataTypes
     * @param user
     * @param mode
     * @throws Exception
     */
    protected void InsertHistory(MultiTableBatchWriter writer, String shardId, String datatype, String eventUid, ColumnVisibility viz, String fieldName,
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
     * @param shardId
     * @param datatype
     * @param eventUid
     * @param fieldName
     * @param fieldValue
     * @param isIndexed
     * @param isReverseIndexed
     * @param dataTypes
     * @param user
     * @param mode
     * @throws Exception
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
     * @param currentEntryList
     * @param isIndexed
     * @param isReverseIndexed
     * @param dataTypes
     * @param user
     * @param mode
     * @throws Exception
     */
    protected void delete(MultiTableBatchWriter writer, List<Pair<Key,Value>> currentEntryList, boolean isIndexOnlyField, boolean isIndexed,
                    boolean isReverseIndexed, Set<Type<?>> dataTypes, String user, MODE mode, long ts, boolean insertHistory) throws Exception {
        
        for (Pair<Key,Value> currentEntry : currentEntryList) {
            
            String shardId = currentEntry.getFirst().getRow().toString();
            
            ColumnVisibility viz = new ColumnVisibility(currentEntry.getFirst().getColumnVisibility());
            
            // Need to parse current entry differently if this is an FI key that we are deleting.
            boolean isFIKey = false;
            if (!currentEntry.getFirst().getColumnFamily().toString().startsWith(FIELD_INDEX_PREFIX)) {} else {
                isFIKey = true;
            }
            
            long currentEntryTimestamp = currentEntry.getFirst().getTimestamp();
            
            if (isFIKey) {
                // Only the delete the fi key
                Mutation e = new Mutation(currentEntry.getFirst().getRow());
                e.putDelete(currentEntry.getFirst().getColumnFamily(), currentEntry.getFirst().getColumnQualifier(), viz, currentEntryTimestamp);
                writer.getBatchWriter(this.getEventTableName()).addMutation(e);
            } else {
                
                int idx = currentEntry.getFirst().getColumnFamily().toString().indexOf(NULL_BYTE);
                String datatype = currentEntry.getFirst().getColumnFamily().toString().substring(0, idx);
                String eventUid = currentEntry.getFirst().getColumnFamily().toString().substring(idx + 1);
                
                idx = currentEntry.getFirst().getColumnQualifier().toString().indexOf(NULL_BYTE);
                String fieldName = currentEntry.getFirst().getColumnQualifier().toString().substring(0, idx);
                String fieldValue = currentEntry.getFirst().getColumnQualifier().toString().substring(idx + 1);
                
                Mutation m = new Mutation(fieldName);
                // Decrement the term frequency
                m.put(ColumnFamilyConstants.COLF_F, new Text(datatype + NULL_BYTE + DateHelper.format(currentEntryTimestamp)), new Value(
                                SummingCombiner.VAR_LEN_ENCODER.encode(-1L)));
                // Remove the field.
                Mutation e = new Mutation(currentEntry.getFirst().getRow());
                if (!isIndexOnlyField)
                    e.putDelete(currentEntry.getFirst().getColumnFamily(), currentEntry.getFirst().getColumnQualifier(), viz, currentEntryTimestamp);
                
                if (isIndexed) {
                    
                    long tsToDay = (ts / MS_PER_DAY) * MS_PER_DAY;
                    
                    // Create a UID object for the Value
                    Builder uidBuilder = Uid.List.newBuilder();
                    uidBuilder.setIGNORE(false);
                    uidBuilder.setCOUNT(-1);
                    uidBuilder.addUID(eventUid);
                    Uid.List uidList = uidBuilder.build();
                    Value val = new Value(uidList.toByteArray());
                    
                    for (Type<?> n : dataTypes) {
                        String indexedValue = n.normalize(fieldValue);
                        
                        // Remove the global index entry
                        Mutation i = new Mutation(indexedValue);
                        i.put(fieldName, shardId + NULL_BYTE + datatype, viz, tsToDay, val);
                        writer.getBatchWriter(this.getIndexTableName()).addMutation(i);
                        
                        if (isReverseIndexed) {
                            String reverseIndexedValue = StringUtils.reverse(indexedValue);
                            // Remove the global reverse index entry
                            Mutation ri = new Mutation(reverseIndexedValue);
                            ri.put(fieldName, shardId + NULL_BYTE + datatype, viz, tsToDay, val);
                            writer.getBatchWriter(this.getReverseIndexTableName()).addMutation(ri);
                        }
                        // Remove the field index entry
                        e.putDelete(new Text(FIELD_INDEX_PREFIX + fieldName), new Text(indexedValue + NULL_BYTE + datatype + NULL_BYTE + eventUid), viz,
                                        currentEntryTimestamp);
                    }
                }
                if (e.size() > 0)
                    writer.getBatchWriter(this.getEventTableName()).addMutation(e);
                writer.getBatchWriter(this.getMetadataTableName()).addMutation(m);
                
                if (!isIndexOnlyField && insertHistory) {
                    InsertHistory(writer, shardId, datatype, eventUid, viz, fieldName, fieldValue, ts, isIndexOnlyField, isIndexed, isReverseIndexed,
                                    dataTypes, user, mode);
                }
                
            }
        }
        writer.flush();
    }
    
    /**
     * Get the Key,Value pair for the field to be updated/deleted from the event table
     * 
     * @param con
     * @param userAuths
     * @param shardId
     * @param datatype
     * @param eventUid
     * @param fieldName
     * @param oldFieldValue
     * @param oldFieldMarkings
     * @param oldColumnVisibility
     * @return
     * @throws Exception
     */
    protected List<Pair<Key,Value>> getField(Connector con, Set<Authorizations> userAuths, String shardId, String datatype, String eventUid, String fieldName,
                    String oldFieldValue, Map<String,String> oldFieldMarkings, ColumnVisibility oldColumnVisibility) throws Exception {
        
        Text family = new Text(datatype);
        TextUtil.textAppend(family, eventUid);
        
        Text qualifier = null;
        
        if (oldFieldValue != null) {
            qualifier = new Text(fieldName);
            TextUtil.textAppend(qualifier, oldFieldValue);
        }
        
        List<Pair<Key,Value>> results = new ArrayList<>();
        
        Scanner s = ScannerHelper.createScanner(con, this.getEventTableName(), userAuths);
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
                    log.trace("Skipping key that does not match with column visibility: " + e.getKey().toString());
                    continue;
                }
            } else {
                Map<String,String> markings = markingFunctions.translateFromColumnVisibilityForAuths(e.getKey().getColumnVisibilityParsed(), userAuths);
                if (null != oldFieldMarkings && !oldFieldMarkings.equals(markings)) {
                    log.trace("Skipping key that does not match with markings: " + e.getKey().toString());
                    continue;
                }
            }
            results.add(new Pair<>(e.getKey(), e.getValue()));
        }
        return results;
    }
    
    /**
     * Pulls the entire event and returns the most common timestamp for the event. This *assumes* the most common timestamp is the original one. If this is not
     * the case, then it needs to change. Another option could be to return the earliest timestamp, which is also an assumption.
     * 
     * @param con
     * @param userAuths
     * @param shardId
     * @param datatype
     * @param eventUid
     * @return long - highestOccurrenceTimestamp - most common timestamp in the event
     * @throws Exception
     */
    protected long getOriginalEventTimestamp(Connector con, Set<Authorizations> userAuths, String shardId, String datatype, String eventUid) throws Exception {
        
        Text family = new Text(datatype);
        TextUtil.textAppend(family, eventUid);
        
        HashMap<Long,Integer> timestampCounts = new HashMap<>();
        
        // Pull the entire event
        Scanner s = ScannerHelper.createScanner(con, this.getEventTableName(), userAuths);
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
        
        long highestOccurrenceTimestamp = 0;
        int highestOccurrences = -1;
        // Determine the most common timestamp
        for (Entry<Long,Integer> entry : timestampCounts.entrySet()) {
            Long ts = entry.getKey();
            int occurrences = entry.getValue();
            if (occurrences > highestOccurrences) {
                highestOccurrences = occurrences;
                highestOccurrenceTimestamp = ts;
            }
        }
        
        return highestOccurrenceTimestamp;
    }
    
    /**
     * Get an instance of a MetadataHelper object
     * 
     * @param con
     * @return
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     * @throws ExecutionException
     * @throws TableNotFoundException
     */
    protected MetadataHelper getMetadataHelper(Connector con) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, ExecutionException {
        Authorizations auths = con.securityOperations().getUserAuthorizations(con.whoami());
        return metadataHelperFactory.createMetadataHelper().initialize(con, this.getMetadataTableName(), Collections.singleton(auths));
    }
    
    /**
     * Check to see if a field is mutable
     * 
     * @param mutableFieldList
     * @param datatype
     * @param fieldName
     * @return
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
     * @param uuidType
     * @param userAuths
     * @return Event
     * @throws Exception
     */
    protected EventBase<?,?> findMatchingEventUuid(String uuid, String uuidType, Set<Authorizations> userAuths, ModificationOperation operation)
                    throws Exception {
        
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
        
        DefaultEvent e = null;
        QueryExecutorBean queryService = this.getQueryService();
        
        String id = null;
        HashSet<String> auths = new HashSet<>();
        for (Authorizations a : userAuths)
            auths.addAll(Arrays.asList(a.toString().split(",")));
        
        Date expiration = new Date();
        expiration = new Date(expiration.getTime() + (1000 * 60 * 60 * 24));
        
        try {
            GenericResponse<String> createResponse = queryService.createQuery(logicName, QueryParametersImpl.paramsToMap(logicName, query.toString(),
                            "Query to find matching records for metadata modification", columnVisibility, new Date(0), new Date(),
                            StringUtils.join(auths, ','), expiration, 2, QueryPersistence.TRANSIENT, queryOptions.toString(), false));
            
            id = createResponse.getResult();
            BaseQueryResponse response = queryService.next(id);
            if (response instanceof DefaultEventQueryResponse) {
                DefaultEventQueryResponse eResponse = (DefaultEventQueryResponse) response;
                if (eResponse.getEvents().size() > 1) {
                    throw new IllegalStateException("More than one event matched " + uuid + " (" + eResponse.getEvents().size() + " matched)");
                }
                if (eResponse.getEvents().size() == 0) {
                    throw new IllegalStateException("No event matched " + uuid);
                }
                
                e = (DefaultEvent) eResponse.getEvents().get(0);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (id != null) {
                queryService.close(id);
            }
        }
        
        return e;
        
    }
    
}
