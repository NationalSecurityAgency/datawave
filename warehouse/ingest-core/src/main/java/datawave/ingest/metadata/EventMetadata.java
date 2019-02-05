package datawave.ingest.metadata;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.data.ColumnFamilyConstants;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.AbstractContentIngestHelper;
import datawave.ingest.data.config.ingest.CompositeIngestHelperInterface;
import datawave.ingest.data.config.ingest.IndexOnlyIngestHelperInterface;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.data.config.ingest.TermFrequencyIngestHelperInterface;
import datawave.ingest.mapreduce.handler.DataTypeHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.util.TextUtil;
import datawave.util.time.DateHelper;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Object that summarizes the events that are processed by the EventMapper. This object extracts metadata about the events (i.e. fields, indexed fields, field
 * frequency, etc.) and creates mutations for a accumulo table with the format:
 * <p>
 * <br>
 * <table border="1" summary="">
 * <tr>
 * <th>Schema Type</th>
 * <th>Use</th>
 * <th>Row</th>
 * <th>Column Family</th>
 * <th>Column Qualifier</th>
 * <th>Value</th>
 * </tr>
 * <tr>
 * <td>MetaData</td>
 * <td>Event Metadata</td>
 * <td>Field Name</td>
 * <td>'e'</td>
 * <td>DataType</td>
 * <td>NULL</td>
 * </tr>
 * <tr>
 * <td>MetaData</td>
 * <td>Index Metadata</td>
 * <td>Normalized Field Name</td>
 * <td>'i'</td>
 * <td>DataType\0NormalizerClass</td>
 * <td>NULL</td>
 * </tr>
 * <tr>
 * <td>MetaData</td>
 * <td>Reverse Index Metadata</td>
 * <td>Normalized Field Name</td>
 * <td>'ri'</td>
 * <td>DataType\0NormalizerClass</td>
 * <td>NULL</td>
 * </tr>
 * <tr>
 * <td>MetaData</td>
 * <td>Event Column Frequency</td>
 * <td>Field Name</td>
 * <td>'f'</td>
 * <td>DataType\0YYYMMDD</td>
 * <td>Count</td>
 * </tr>
 * <tr>
 * <td>Load Dates Table</td>
 * <td>Load Date Frequency (See LoadDateScanBuilder)</td>
 * <td>Field Name</td>
 * <td>Table Name</td>
 * <td>YYYYMMDD\0DataType</td>
 * <td>Count (See SummingCombiner.VARLEN)</td>
 * </tr>
 * </table>
 */
public class EventMetadata implements RawRecordMetadata {
    
    private MetadataWithMostRecentDate compositeFieldsInfo = new MetadataWithMostRecentDate(ColumnFamilyConstants.COLF_CI);
    private MetadataWithMostRecentDate compositeSeparators = new MetadataWithMostRecentDate(ColumnFamilyConstants.COLF_CISEP);
    private MetadataWithMostRecentDate dataTypeFieldsInfo = new MetadataWithMostRecentDate(ColumnFamilyConstants.COLF_T);
    private MetadataWithMostRecentDate normalizedFieldsInfo = new MetadataWithMostRecentDate(ColumnFamilyConstants.COLF_N);
    
    private static final Logger log = Logger.getLogger(EventMetadata.class);
    private final Text metadataTableName;
    private final Text loadDatesTableName;
    // stores field name, data type, and most recent event date
    private final MetadataWithMostRecentDate eventFieldsInfo = new MetadataWithMostRecentDate(ColumnFamilyConstants.COLF_E);
    private final MetadataWithMostRecentDate indexedFieldsInfo = new MetadataWithMostRecentDate(ColumnFamilyConstants.COLF_I);
    private final MetadataWithMostRecentDate reverseIndexedFieldsInfo = new MetadataWithMostRecentDate(ColumnFamilyConstants.COLF_RI);
    private final MetadataWithMostRecentDate termFrequencyFieldsInfo = new MetadataWithMostRecentDate(ColumnFamilyConstants.COLF_TF);
    // stores counts
    private final MetadataCounterGroup frequencyCounts = new MetadataCounterGroup(ColumnFamilyConstants.COLF_F); // by event date
    private final MetadataCounterGroup indexedFieldsLoadDateCounts;
    private final MetadataCounterGroup reverseIndexedFieldsLoadDateCounts;
    private boolean frequency = false;
    
    /**
     * @param shardTableName
     *            used as part of column family within LOAD_DATES_TABLE_NAME.
     * @param metadataTableName
     *            table where the metadata entries will be placed
     * @param shardIndexTableName
     *            used as part of column family within LOAD_DATES_TABLE_NAME
     * @param shardReverseIndexTableName
     *            used as part of column family within LOAD_DATES_TABLE_NAME
     * @param frequency
     *            whether to add to the metadata table's counts for field name by event date
     */
    public EventMetadata(@SuppressWarnings("UnusedParameters") Text shardTableName, Text metadataTableName, Text loadDatesTableName, Text shardIndexTableName,
                    Text shardReverseIndexTableName, boolean frequency) {
        this.metadataTableName = metadataTableName;
        this.loadDatesTableName = loadDatesTableName;
        this.frequency = frequency;
        
        this.indexedFieldsLoadDateCounts = new MetadataCounterGroup("FIELD_NAME", shardIndexTableName);
        this.reverseIndexedFieldsLoadDateCounts = new MetadataCounterGroup("FIELD_NAME", shardReverseIndexTableName);
    }
    
    @Override
    public void addEvent(IngestHelperInterface helper, RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields, long loadTimeInMillis) {
        addEvent(helper, event, fields, this.frequency, INCLUDE_LOAD_DATES, loadTimeInMillis);
    }
    
    @Override
    public void addEvent(IngestHelperInterface helper, RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields) {
        addEvent(helper, event, fields, this.frequency, INCLUDE_LOAD_DATES, System.currentTimeMillis());
    }
    
    @Override
    public void addEventWithoutLoadDates(IngestHelperInterface helper, RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields) {
        addEvent(helper, event, fields, this.frequency, EXCLUDE_LOAD_DATES, System.currentTimeMillis());
    }
    
    @Override
    public void addEvent(IngestHelperInterface helper, RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields, boolean frequency) {
        addEvent(helper, event, fields, frequency, INCLUDE_LOAD_DATES, System.currentTimeMillis());
    }
    
    protected void addEvent(IngestHelperInterface helper, RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields, boolean frequency,
                    boolean shouldIncludeLoadDates, long loadDateInMillis) {
        /***
         * TODO: move this into the ingest helpers and data handlers
         */
        long countDelta = getCountDelta(helper);
        
        String loadDateStr = shouldIncludeLoadDates ? DateHelper.format(loadDateInMillis) : null;
        
        for (String fieldName : fields.keySet()) {
            long fieldDelta = countDelta * fields.get(fieldName).size();
            addEventField(helper, event, fieldName, fieldDelta, frequency);
            
            // if the field is indexed, reverse-indexed, or normalized, we need to write a dataType entry
            // using either the assigned dataType or the default dataType
            boolean shouldWriteDataType = false;
            
            if (helper.isIndexedField(fieldName)) {
                shouldWriteDataType = true;
                updateForIndexedField(helper, event, fields, countDelta, loadDateStr, NO_TOKEN_DESIGNATOR, fieldName);
            }
            
            if (helper.isReverseIndexedField(fieldName)) {
                shouldWriteDataType = true;
                updateForReverseIndexedField(helper, event, fields, countDelta, loadDateStr, NO_TOKEN_DESIGNATOR, fieldName);
            }
            
            if (helper.isNormalizedField(fieldName)) {
                shouldWriteDataType = true;
                log.debug(fieldName + " is normalized");
                updateMetadata(this.normalizedFieldsInfo, helper, event, fields, fieldName);
            }
            
            if (helper.isDataTypeField(fieldName) || shouldWriteDataType) {
                log.debug(fieldName + " has a data type");
                // write a dataType entry
                // using either the assigned dataType or the default dataType
                update(helper.getDataTypes(fieldName), event, fields.get(fieldName), "", 0, null, this.dataTypeFieldsInfo, null);
            } else {
                log.debug(fieldName + " apparently has no data type");
            }
            
            if (helper.isCompositeField(fieldName)) {
                Collection<String> componentFields = helper.getCompositeFieldDefinitions().get(fieldName);
                this.compositeFieldsInfo.createOrUpdate(fieldName, event.getDataType().outputName(), String.join(",", componentFields), event.getDate());
                this.compositeSeparators.createOrUpdate(fieldName, event.getDataType().outputName(), helper.getCompositeFieldSeparators().get(fieldName),
                                event.getDate());
            }
        }
        
        addTokenizedContent(helper, event, fields, countDelta, loadDateStr);
    }
    
    protected long getCountDelta(IngestHelperInterface helper) {
        return (helper.getDeleteMode()) ? -1L : 1L;
    }
    
    protected void addEventField(IngestHelperInterface helper, RawRecordContainer event, String fieldName, long countDelta, boolean frequency) {
        // if only indexing this field, then do not add to event and frequency maps
        if (helper.isIndexOnlyField(fieldName)) {
            log.debug(fieldName + " is indexonly, not adding to event");
            return;
        }
        
        if (helper.isCompositeField(fieldName) && !helper.isOverloadedCompositeField(fieldName)) {
            log.debug(fieldName + " is a composite, not adding to event");
            return;
        }
        
        log.debug("createOrUpdate for " + fieldName);
        eventFieldsInfo.createOrUpdate(fieldName, event.getDataType().outputName(), MetadataWithMostRecentDate.IGNORED_NORMALIZER_CLASS, event.getDate());
        
        if (frequency) {
            addToFrequencyCounts(event, fieldName, countDelta);
        }
    }
    
    protected void addToFrequencyCounts(RawRecordContainer event, String fieldName, long countDelta) {
        String date = DateHelper.format(event.getDate());
        frequencyCounts.addToCount(countDelta, event.getDataType().outputName(), fieldName, date);
    }
    
    protected void updateForIndexedField(@SuppressWarnings("UnusedParameters") IngestHelperInterface helper, RawRecordContainer event,
                    Multimap<String,NormalizedContentInterface> fields, long countDelta, String loadDate, String tokenDesignator, String fieldName) {
        update(event, fields.get(fieldName), tokenDesignator, countDelta, loadDate, indexedFieldsInfo, indexedFieldsLoadDateCounts);
    }
    
    private void updateForCompositeField(@SuppressWarnings("UnusedParameters") IngestHelperInterface helper, RawRecordContainer event,
                    Multimap<String,NormalizedContentInterface> fields, long countDelta, String loadDate, String tokenDesignator, String fieldName) {
        update(event, fields.get(fieldName), tokenDesignator, countDelta, loadDate, indexedFieldsInfo, indexedFieldsLoadDateCounts);
    }
    
    protected void updateForReverseIndexedField(@SuppressWarnings("UnusedParameters") IngestHelperInterface helper, RawRecordContainer event,
                    Multimap<String,NormalizedContentInterface> fields, long countDelta, String loadDate, String tokenDesignator, String fieldName) {
        update(event, fields.get(fieldName), tokenDesignator, countDelta, loadDate, reverseIndexedFieldsInfo, reverseIndexedFieldsLoadDateCounts);
    }
    
    protected void update(List<datawave.data.type.Type<?>> types, RawRecordContainer event, Collection<NormalizedContentInterface> norms,
                    String tokenDesignator, long countDelta, String loadDate, MetadataWithMostRecentDate mostRecentDatesByIdentifier,
                    MetadataCounterGroup counts) {
        Type dataType = event.getDataType();
        long eventDate = event.getDate();
        for (NormalizedContentInterface norm : norms) {
            for (datawave.data.type.Type<?> type : types) {
                mostRecentDatesByIdentifier.createOrUpdate(norm.getIndexedFieldName() + tokenDesignator, dataType.outputName(), type.getClass().getName(),
                                eventDate);
                if (null != loadDate) {
                    updateLoadDateCounters(counts, event, norm.getIndexedFieldName() + tokenDesignator, countDelta, loadDate);
                }
            }
        }
    }
    
    protected void update(String[] names, RawRecordContainer event, Collection<NormalizedContentInterface> norms,
                    @SuppressWarnings("UnusedParameters") String tokenDesignator, long countDelta, String loadDate,
                    MetadataWithMostRecentDate mostRecentDatesByIdentifier, MetadataCounterGroup counts) {
        Type dataType = event.getDataType();
        long eventDate = event.getDate();
        for (NormalizedContentInterface norm : norms) {
            int counter = 0;
            for (String name : names) {
                // mostRecentDatesByIdentifier.createOrUpdate(norm.getIndexedFieldName() + tokenDesignator, dataType.outputName(), name, eventDate);
                mostRecentDatesByIdentifier.createOrUpdate(name, dataType.outputName(), norm.getIndexedFieldName() + "," + counter++, eventDate);
                if (null != loadDate) {
                    // updateLoadDateCounters(counts, event, norm.getIndexedFieldName() + tokenDesignator, countDelta, loadDate);
                    updateLoadDateCounters(counts, event, name, countDelta, loadDate);
                }
            }
        }
    }
    
    protected void update(RawRecordContainer event, Collection<NormalizedContentInterface> norms, String tokenDesignator, long countDelta, String loadDate,
                    MetadataWithMostRecentDate mostRecentDatesByIdentifier, MetadataCounterGroup counts) {
        Type dataType = event.getDataType();
        long eventDate = event.getDate();
        for (NormalizedContentInterface norm : norms) {
            mostRecentDatesByIdentifier.createOrUpdate(norm.getIndexedFieldName() + tokenDesignator, dataType.outputName(), null, eventDate);
            if (null != loadDate) {
                updateLoadDateCounters(counts, event, norm.getIndexedFieldName() + tokenDesignator, countDelta, loadDate);
            }
        }
    }
    
    protected void updateMetadata(MetadataWithMostRecentDate metadataInfo, @SuppressWarnings("UnusedParameters") IngestHelperInterface helper,
                    RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields, String fieldName) {
        // will ignore the update load counters because it's null
        update(event, fields.get(fieldName), "", 0, null, metadataInfo, null);
    }
    
    protected void addTokenizedContent(IngestHelperInterface helper, RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields,
                    long countDelta, String loadDate) {
        if (helper instanceof TermFrequencyIngestHelperInterface) {
            TermFrequencyIngestHelperInterface h = (TermFrequencyIngestHelperInterface) helper;
            for (String field : fields.keySet()) {
                if (h.isTermFrequencyField(field)) {
                    termFrequencyFieldsInfo.createOrUpdate(field, event.getDataType().outputName(), MetadataWithMostRecentDate.IGNORED_NORMALIZER_CLASS,
                                    event.getDate());
                }
            }
        }
        
        // The ContentIndexingColumnBasedHandler uses helpers with this interface
        if (helper instanceof AbstractContentIngestHelper) {
            String tokenDesignator = ((AbstractContentIngestHelper) helper).getTokenFieldNameDesignator();
            AbstractContentIngestHelper h = (AbstractContentIngestHelper) helper;
            for (String field : fields.keySet()) {
                if (h.isContentIndexField(field)) {
                    updateForIndexedField(helper, event, fields, countDelta, loadDate, tokenDesignator, field);
                    termFrequencyFieldsInfo.createOrUpdate(field, event.getDataType().outputName(), MetadataWithMostRecentDate.IGNORED_NORMALIZER_CLASS,
                                    event.getDate());
                }
                
                if (h.isReverseContentIndexField(field)) {
                    updateForReverseIndexedField(helper, event, fields, countDelta, loadDate, tokenDesignator, field);
                }
            }
        }
        
        if (helper instanceof IndexOnlyIngestHelperInterface) {
            Set<String> indexOnlyFields = ((IndexOnlyIngestHelperInterface) helper).getIndexOnlyFields();
            for (String fieldName : indexOnlyFields) {
                if (fields.containsKey(fieldName)) {
                    updateForIndexedField(helper, event, fields, countDelta, loadDate, "", fieldName);
                }
            }
        }
        
        if (helper instanceof CompositeIngestHelperInterface) {
            Set<String> compositeFields = ((CompositeIngestHelperInterface) helper).getCompositeFields();
            for (String fieldName : compositeFields) {
                if (fields.containsKey(fieldName)) {
                    updateForCompositeField(helper, event, fields, countDelta, loadDate, "", fieldName);
                }
            }
        }
    }
    
    protected void updateLoadDateCounters(MetadataCounterGroup countGroup, RawRecordContainer event, String fieldName, long countDelta, String loadDate) {
        if (null != loadDate) {
            countGroup.addToCount(countDelta, event.getDataType().outputName(), fieldName, loadDate);
        }
    }
    
    @Override
    public Multimap<BulkIngestKey,Value> getBulkMetadata() {
        Multimap<BulkIngestKey,Value> bulkData = HashMultimap.create();
        
        addIndexedFieldToMetadata(bulkData, eventFieldsInfo);
        addIndexedFieldToMetadata(bulkData, termFrequencyFieldsInfo);
        
        addIndexedFieldToMetadata(bulkData, indexedFieldsInfo);
        addIndexedFieldToMetadata(bulkData, reverseIndexedFieldsInfo);
        
        addFrequenciesToMetadata(bulkData);
        
        addIndexedFieldToMetadata(bulkData, dataTypeFieldsInfo);
        addIndexedFieldToMetadata(bulkData, normalizedFieldsInfo);
        
        addIndexedFieldToMetadata(bulkData, this.compositeFieldsInfo);
        addIndexedFieldToMetadata(bulkData, this.compositeSeparators);
        
        addToLoadDates(bulkData, this.indexedFieldsLoadDateCounts);
        addToLoadDates(bulkData, this.reverseIndexedFieldsLoadDateCounts);
        
        return bulkData;
    }
    
    protected void addToLoadDates(Multimap<BulkIngestKey,Value> results, MetadataCounterGroup countsGroup) {
        if (loadDatesTableName != null) {
            for (MetadataCounterGroup.CountAndKeyComponents entry : countsGroup.getEntries()) {
                Long count = entry.getCount();
                Key k = new Key(new Text(entry.getRowId()), countsGroup.getColumnFamily(), new Text(entry.getDate() + DELIMITER + entry.getDataType()));
                
                addToResults(results, count, k, loadDatesTableName);
            }
        }
    }
    
    protected void addFrequenciesToMetadata(Multimap<BulkIngestKey,Value> results) {
        if (frequency) {
            for (MetadataCounterGroup.CountAndKeyComponents entry : frequencyCounts.getEntries()) {
                Long count = entry.getCount();
                Key k = new Key(new Text(entry.getRowId()), frequencyCounts.getColumnFamily(), new Text(entry.getDataType() + DELIMITER + entry.getDate()),
                                DateHelper.parse(entry.getDate()).getTime());
                addToResults(results, count, k, this.metadataTableName);
            }
        }
    }
    
    protected void addToResults(Multimap<BulkIngestKey,Value> results, Long value, Key key, Text tableName) {
        BulkIngestKey bk = new BulkIngestKey(tableName, key);
        results.put(bk, new Value(SummingCombiner.VAR_LEN_ENCODER.encode(value)));
    }
    
    protected void addIndexedFieldToMetadata(Multimap<BulkIngestKey,Value> results, MetadataWithMostRecentDate mostRecentDates) {
        for (MetadataWithMostRecentDate.MostRecentEventDateAndKeyComponents entry : mostRecentDates.entries()) {
            long mostRecentDate = entry.getMostRecentDate();
            Text fieldName = new Text(entry.getFieldName());
            Text colq = new Text(entry.getDataType());
            if (null != entry.getNormalizerClassName()) {
                TextUtil.textAppend(colq, entry.getNormalizerClassName());
            }
            Key k = new Key(fieldName, mostRecentDates.getColumnFamily(), colq, mostRecentDate);
            BulkIngestKey bk = new BulkIngestKey(metadataTableName, k);
            results.put(bk, DataTypeHandler.NULL_VALUE);
        }
    }
    
    @Override
    public void clear() {
        this.eventFieldsInfo.clear();
        this.termFrequencyFieldsInfo.clear();
        this.frequencyCounts.clear();
        
        this.indexedFieldsInfo.clear();
        this.indexedFieldsLoadDateCounts.clear();
        
        this.reverseIndexedFieldsInfo.clear();
        this.reverseIndexedFieldsLoadDateCounts.clear();
    }
}
