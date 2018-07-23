package datawave.ingest.mapreduce.handler.shard;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import com.google.common.base.Stopwatch;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.hash.BloomFilter;

import datawave.ingest.config.IngestConfiguration;
import datawave.ingest.config.IngestConfigurationFactory;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.config.MaskedFieldHelper;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.mapreduce.MemberShipTest;
import datawave.ingest.mapreduce.handler.DataTypeHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.statsd.StatsDEnabledDataTypeHandler;
import datawave.ingest.metadata.RawRecordMetadata;
import datawave.ingest.protobuf.Uid;
import datawave.ingest.protobuf.Uid.List.Builder;
import datawave.ingest.table.config.LoadDateTableConfigHelper;
import datawave.ingest.util.BloomFilterUtil;
import datawave.ingest.util.BloomFilterWrapper;
import datawave.ingest.util.DiskSpaceStarvationStrategy;
import datawave.marking.MarkingFunctions;
import datawave.util.TextUtil;
import datawave.webservice.common.logging.ThreadConfigurableLogger;

/**
 * <p>
 * When the processBulk method is called on this DataTypeHandler it creates Key/Values for the shard, shardIndex, and ShardReverseIndex tables formats. The
 * names of these tables need to be specified in the configuration and are checked upon the call to setup().
 * 
 * <p>
 * This class creates the following Mutations or Key/Values: <br>
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
 * <td>Shard</td>
 * <td>Event Data</td>
 * <td>ShardId</td>
 * <td>DataType\0UID</td>
 * <td>Normalized Field Name\0Field Value</td>
 * <td>NULL</td>
 * </tr>
 * <tr>
 * <td>Shard</td>
 * <td>Field Index</td>
 * <td>ShardId</td>
 * <td>'fi'\0Normalized Field Name</td>
 * <td>Normalized Field Value\0DataType\0UID</td>
 * <td>NULL</td>
 * </tr>
 * <tr>
 * <td>Shard Index</td>
 * <td>Global Index</td>
 * <td>Normalized Field Value</td>
 * <td>Normalized Field Name</td>
 * <td>ShardId\0DataType</td>
 * <td>Uid.List</td>
 * </tr>
 * <tr>
 * <td>Shard Reverse Index</td>
 * <td>Global Reverse Index</td>
 * <td>Reversed Normalized Field Value</td>
 * <td>Normalized Field Name</td>
 * <td>ShardId\0DataType</td>
 * <td>Uid.List</td>
 * </tr>
 * </table>
 * 
 * <p>
 * The table with the name specified by {@link #SHARD_TNAME} will be the shard table. The shard table is partitioned into {@link #NUM_SHARDS} slices of a day.
 * The hash function is {@code (Integer.MAX_VALUE & Event.getUid().toString().hashCode()) % numShards}. The ShardId looks like YYYYMMDD_N, where N is the result
 * of the hash function. This approach ends up creating a tablet in Accumulo that contains one row. That row will contain all of the events that for that day
 * and hash value. To find an event that happened on a particular day, all of the tablets for that day have to be queried.
 * 
 * <p>
 * The tables with the name specified by {@link #SHARD_GIDX_TNAME} and {@link #SHARD_GRIDX_TNAME} will be the global index and global reverse index. The column
 * qualifier in these indexes contain the shardId and the datatype. The global indexes can be used to identify which tablets contain data for the indexed term.
 * Furthermore, the datatype can also be used to query the tablet more accurately. The value portion of the indexed term will contain a Uid.List Protocol Buffer
 * object. It is intended that this object will contain UIDs for events that contain the indexed term when the cardinality is low. The absence of UIDs in the
 * Uid.List object indicates that there are more than {@link datawave.ingest.table.aggregator.GlobalIndexUidAggregator#MAX} Events of that datatype that contain
 * the indexed term in the shard. This is an optimization that will allow low cardinality terms to be found more quickly.
 * 
 * @param <KEYIN>
 */
public abstract class ShardedDataTypeHandler<KEYIN> extends StatsDEnabledDataTypeHandler<KEYIN> implements DataTypeHandler<KEYIN> {
    
    private static final Logger log = ThreadConfigurableLogger.getLogger(ShardedDataTypeHandler.class);
    
    public static final String NUM_SHARDS = ShardIdFactory.NUM_SHARDS;
    public static final String SHARD_TNAME = "shard.table.name";
    public static final String SHARD_ININDEX_BLOOM = "shard.table.index.bloom.enable";
    public static final String SHARD_ININDEX_BLOOM_DISK_THRESHOLD = "shard.table.index.bloom.min.disk.percent"; // % of remaining disk space before n-grams will
                                                                                                                // cease to be added to the bloom filter
    public static final String SHARD_ININDEX_BLOOM_DISK_THRESHOLD_PATH = "shard.table.index.bloom.min.disk.path"; // Path used to measure the remaining disk
                                                                                                                  // space
    public static final String SHARD_ININDEX_BLOOM_MEMORY_THRESHOLD = "shard.table.index.bloom.min.memory.percent"; // % of remaining memory before n-grams will
                                                                                                                    // cease to be added to the bloom filter
    public static final String SHARD_ININDEX_BLOOM_TIMEOUT_THRESHOLD = "shard.table.index.bloom.min.timeout.percent"; // % of remaining time (with respect to
                                                                                                                      // mapred.task.timeout) before n-grams
                                                                                                                      // will stop being added to a bloom filter
    public static final String SHARD_ININDEX_BLOOM_OPTIMUM_MAX_FILTER_SIZE = "shard.table.index.bloom.optimum.max.filter.size"; // Bytes
    public static final String SHARD_STATS_TNAME = "shard.stats.table.name";
    public static final String SHARD_GIDX_TNAME = "shard.global.index.table.name";
    public static final String SHARD_GRIDX_TNAME = "shard.global.rindex.table.name";
    public static final String SHARD_LPRIORITY = "shard.table.loader.priority";
    public static final String SHARD_GIDX_LPRIORITY = "shard.global.index.table.loader.priority";
    public static final String SHARD_GRIDX_LPRIORITY = "shard.global.rindex.table.loader.priority";
    
    public static final String IS_REINDEX_ENABLED = "ingest.reindex.enabled";
    public static final String FIELDS_TO_REINDEX = "ingest.reindex.fields";
    
    /**
     * name of ACCUMULO table to store DATAWAVE metadata
     */
    public static final String METADATA_TABLE_NAME = "metadata.table.name";
    public static final String METADATA_TABLE_LOADER_PRIORITY = "metadata.table.loader.priority";
    
    /**
     * term dictionary optimization table
     */
    
    public static final String SHARD_DINDX_LPRIORITY = "shard.dicitonary.index.table.loader.priority";
    public static final String SHARD_DICTIONARY_CACHE_ENTRIES = "shard.dictionary.cache.entries";
    public static final String SHARD_DINDX_NAME = "shard.dictionary.index.table.name";
    public static final Text SHARD_DINDX_FLABEL = new Text("for");
    public static final Text SHARD_DINDX_RLABEL = new Text("rev");
    public static final String SHARD_DINDX_FLABEL_LOCALITY_NAME = "forward";
    public static final String SHARD_DINDX_RLABEL_LOCALITY_NAME = "reverse";
    public static final int SHARD_DINDEX_CACHE_DEFAULT_SIZE = 1024;
    
    /**
     * Enable/Disable term frequency calcuation for fields in the metadata
     */
    public static final String METADATA_TERM_FREQUENCY = "metadata.term.frequency.enabled";
    
    /**
     * Suppress event key generation making this into a psuedo re-indexing job No type prefix here as it is meant to be job level not datatype level.
     */
    public static final String SUPPRESS_EVENT_KEYS = "shard.suppress.event.key";
    
    // Config option name for all tables that are "sharded"
    public static final String SHARDED_TNAMES = "sharded.table.names";
    
    private static final long MS_PER_DAY = TimeUnit.DAYS.toMillis(1);
    
    private float bloomFilteringDiskThreshold;
    private String bloomFilteringDiskThresholdPath;
    private float bloomFilteringMemoryThreshold;
    private int bloomFilteringOptimumMaxFilterSize;
    private float bloomFilteringTimeoutThreshold;
    private Text shardTableName = null;
    private Text shardIndexTableName = null;
    private Text indexStatsTableName = null;
    private Text shardReverseIndexTableName = null;
    private Text metadataTableName = null;
    private Text loadDatesTableName = null;
    private Text shardDictionaryName = null;
    private RawRecordMetadata metadata = null;
    private ShardIdFactory shardIdFactory = null;
    private LoadingCache<String,String> dCache = null;
    protected MarkingFunctions markingFunctions;
    protected IngestConfiguration ingestConfig = IngestConfigurationFactory.getIngestConfiguration();
    private boolean suppressEventKeys = false;
    
    /**
     * Determines whether or not we produce cardinality estimates for data
     */
    protected boolean produceStats = false;
    /**
     * Determines whether or not the bloom filter is enabled.
     */
    private boolean bloomFiltersEnabled = false;
    
    boolean isReindexEnabled;
    private Collection<String> requestedFieldsForReindex;
    
    @Override
    public void setup(TaskAttemptContext context) {
        markingFunctions = MarkingFunctions.Factory.createMarkingFunctions();
        
        Configuration conf = context.getConfiguration();
        shardIdFactory = new ShardIdFactory(conf);
        
        String tableName = conf.get(SHARD_TNAME, null);
        if (null == tableName)
            log.error(SHARD_TNAME + " not specified, no events will be created, and the global index will be useless");
        else
            setShardTableName(new Text(tableName));
        
        tableName = conf.get(SHARD_STATS_TNAME, null);
        if (null == tableName)
            log.warn(SHARD_STATS_TNAME + " not specified, no global index mutations will be created.");
        else {
            setIndexStatsTableName(new Text(tableName));
            setProduceStats(true);
        }
        
        tableName = conf.get(SHARD_GIDX_TNAME, null);
        if (null == tableName)
            log.warn(SHARD_GIDX_TNAME + " not specified, no global index mutations will be created.");
        else
            setShardIndexTableName(new Text(tableName));
        
        tableName = conf.get(SHARD_GRIDX_TNAME, null);
        if (null == tableName)
            log.warn(SHARD_GRIDX_TNAME + " not specified, no global reverse index mutations will be created.");
        else
            setShardReverseIndexTableName(new Text(tableName));
        
        tableName = conf.get(METADATA_TABLE_NAME, null);
        if (null == tableName)
            log.warn(METADATA_TABLE_NAME + " not specified, no metadata will be created, I hope nothing requires normalizers.");
        else
            setMetadataTableName(new Text(tableName));
        
        tableName = (LoadDateTableConfigHelper.isLoadDatesEnabled(conf) ? LoadDateTableConfigHelper.getLoadDatesTableName(conf) : null);
        if (null == tableName)
            log.warn(LoadDateTableConfigHelper.LOAD_DATES_TABLE_NAME_PROP + " not specified, no load dates will be created");
        else
            setLoadDatesTableName(new Text(tableName));
        
        if (getMetadataTableName() != null) {
            setMetadata(ingestConfig.createMetadata(getShardTableName(), getMetadataTableName(), getLoadDatesTableName(), getShardIndexTableName(),
                            getShardReverseIndexTableName(), conf.getBoolean(METADATA_TERM_FREQUENCY, true)));
        }
        
        tableName = conf.get(SHARD_DINDX_NAME, null);
        if (null == tableName) {
            log.warn(SHARD_DINDX_NAME + " not specified, no term dictionary will be created.");
        } else {
            setShardDictionaryIndexTableName(new Text(tableName));
            this.setupDictionaryCache(conf.getInt(SHARD_DICTIONARY_CACHE_ENTRIES, SHARD_DINDEX_CACHE_DEFAULT_SIZE));
            
        }
        
        setupToReindexIfEnabled(conf);
        
        // enabled by default
        this.bloomFiltersEnabled = conf.getBoolean(SHARD_ININDEX_BLOOM, false);
        if (this.bloomFiltersEnabled) {
            this.bloomFilteringDiskThreshold = conf.getFloat(SHARD_ININDEX_BLOOM_DISK_THRESHOLD, 0.0f);
            this.bloomFilteringDiskThresholdPath = conf.get(SHARD_ININDEX_BLOOM_DISK_THRESHOLD_PATH,
                            DiskSpaceStarvationStrategy.DEFAULT_PATH_FOR_DISK_SPACE_VALIDATION);
            this.bloomFilteringMemoryThreshold = conf.getFloat(SHARD_ININDEX_BLOOM_MEMORY_THRESHOLD, 0.0f);
            this.bloomFilteringTimeoutThreshold = conf.getFloat(SHARD_ININDEX_BLOOM_TIMEOUT_THRESHOLD, 0.0f);
            this.bloomFilteringOptimumMaxFilterSize = conf.getInt(SHARD_ININDEX_BLOOM_OPTIMUM_MAX_FILTER_SIZE, -1);
        }
        
        // Event key suppression
        this.suppressEventKeys = conf.getBoolean(SUPPRESS_EVENT_KEYS, false);
    }
    
    private void setupToReindexIfEnabled(Configuration conf) {
        this.isReindexEnabled = conf.getBoolean(IS_REINDEX_ENABLED, false);
        log.info("isReindexEnabled: " + this.isReindexEnabled);
        if (this.isReindexEnabled) {
            String commaSeparatedFieldNames = conf.get(FIELDS_TO_REINDEX);
            if (log.isDebugEnabled()) {
                log.debug("configured reindex fields: " + commaSeparatedFieldNames);
            }
            if (null != commaSeparatedFieldNames) {
                this.requestedFieldsForReindex = Arrays.asList(commaSeparatedFieldNames.split(","));
            }
            if (null == this.requestedFieldsForReindex || this.requestedFieldsForReindex.isEmpty()) {
                throw new RuntimeException("Missing or empty " + FIELDS_TO_REINDEX + " from configuration: " + conf.toString());
            }
            if (log.isDebugEnabled()) {
                log.debug("list of fields to reindex: " + requestedFieldsForReindex.toString());
            }
        }
    }
    
    @Override
    public String[] getTableNames(Configuration conf) {
        List<String> tableNames = new ArrayList<>(4);
        String tableName = conf.get(SHARD_TNAME, null);
        if (null != tableName)
            tableNames.add(tableName);
        
        tableName = conf.get(SHARD_GIDX_TNAME, null);
        if (null != tableName)
            tableNames.add(tableName);
        
        tableName = conf.get(SHARD_GRIDX_TNAME, null);
        if (null != tableName)
            tableNames.add(tableName);
        
        tableName = conf.get(METADATA_TABLE_NAME, null);
        if (null != tableName)
            tableNames.add(tableName);
        
        tableName = conf.get(SHARD_DINDX_NAME, null);
        if (null != tableName)
            tableNames.add(tableName);
        
        if (LoadDateTableConfigHelper.isLoadDatesEnabled(conf)) {
            tableNames.add(LoadDateTableConfigHelper.getLoadDatesTableName(conf));
        }
        
        return tableNames.toArray(new String[tableNames.size()]);
    }
    
    @Override
    public int[] getTableLoaderPriorities(Configuration conf) {
        int[] priorities = new int[6];
        int index = 0;
        String tableName = conf.get(SHARD_TNAME, null);
        if (null != tableName)
            priorities[index++] = conf.getInt(SHARD_LPRIORITY, 20);
        
        tableName = conf.get(SHARD_GIDX_TNAME, null);
        if (null != tableName)
            priorities[index++] = conf.getInt(SHARD_GIDX_LPRIORITY, 30);
        
        tableName = conf.get(SHARD_GRIDX_TNAME, null);
        if (null != tableName)
            priorities[index++] = conf.getInt(SHARD_GRIDX_LPRIORITY, 40);
        
        tableName = conf.get(METADATA_TABLE_NAME, null);
        if (null != tableName)
            priorities[index++] = conf.getInt(METADATA_TABLE_LOADER_PRIORITY, 40);
        
        tableName = conf.get(SHARD_DINDX_NAME, null);
        if (null != tableName)
            priorities[index++] = conf.getInt(SHARD_DINDX_LPRIORITY, 40);
        
        if (LoadDateTableConfigHelper.isLoadDatesEnabled(conf)) {
            priorities[index++] = LoadDateTableConfigHelper.getLoadDatesTableLoaderPriority(conf);
        }
        
        if (index != priorities.length) {
            return Arrays.copyOf(priorities, index);
        } else {
            return priorities;
        }
    }
    
    /**
     * Calculates the shard id of the event
     * 
     * @param event
     * @return Shard id
     */
    public byte[] getShardId(RawRecordContainer event) {
        return shardIdFactory.getShardIdBytes(event);
    }
    
    /**
     * Creates entries for the shard, shardIndex, and shardReverseIndex tables. This method calls the getFieldValues() method to retrieve the field names and
     * values for the shard table, the getGlobalIndexTerms() method to retrieve the field names and values for the shardIndex table, and the
     * getGlobalReverseIndexTerms() method to retrieve the field names and values for the shardReverseIndex table. The benefit to this approach is that
     * subclasses may only have to parse the event object once to calculate all of this information. This method returns null if the Event objects fatalError()
     * method returns true, Else it will return a Multimap of BulkIngestKey to Value pairs
     */
    @Override
    public Multimap<BulkIngestKey,Value> processBulk(KEYIN key, RawRecordContainer event, Multimap<String,NormalizedContentInterface> eventFields,
                    StatusReporter reporter) {
        if (event.fatalError()) {
            return null;
        } else {
            if (isReindexEnabled) {
                Multimap<String,NormalizedContentInterface> filteredEventFields = filterByRequestedFields(eventFields);
                if (filteredEventFields.isEmpty()) {
                    return HashMultimap.create(); // nothing to do (none of the reindex fields were found)
                }
                eventFields = filteredEventFields;
            }
            
            Multimap<String,NormalizedContentInterface> fields = getShardNamesAndValues(event, eventFields, (null != getShardIndexTableName()),
                            (null != getShardReverseIndexTableName()), reporter);
            
            return createColumns(event, fields, reporter);
        }
    }
    
    /**
     * @param event
     * @param fields
     * @param reporter
     */
    protected Multimap<BulkIngestKey,Value> createColumns(RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields, StatusReporter reporter) {
        IngestHelperInterface helper = this.getHelper(event.getDataType());
        
        Multimap<BulkIngestKey,Value> values = HashMultimap.create();
        
        byte[] maskedVisibility = computeMaskedVisibility(event);
        MaskedFieldHelper maskedFieldHelper = createMaskedFieldHelper(helper, event);
        
        byte[] shardId = shardIdFactory.getShardIdBytes(event);
        
        if (null != fields && fields.size() != 0 && null != shardTableName) {
            // Shard Event Table Structure
            // Row: shard id
            // Colf: DataType : UID
            // Colq: FieldName : FieldValue
            // Value: NULL
            Text colf = new Text(event.getDataType().outputName());
            TextUtil.textAppend(colf, event.getId().toString(), helper.getReplaceMalformedUTF8());
            
            Value indexedValue = createUidArray(event.getId().toString(), helper.getDeleteMode());
            
            if (!getSuppressEventKeys()) {
                for (Entry<String,NormalizedContentInterface> e : fields.entries()) {
                    NormalizedContentInterface value = e.getValue();
                    byte[] visibility = getVisibility(event, value);
                    
                    values.putAll(createShardEventColumn(event, colf, value, visibility, maskedVisibility, maskedFieldHelper, shardId));
                    
                }
            }
            
            for (Entry<String,NormalizedContentInterface> e : getGlobalIndexTerms().entries()) {
                NormalizedContentInterface value = e.getValue();
                byte[] visibility = getVisibility(event, value);
                if (log.isTraceEnabled()) {
                    log.trace("Is " + e.getKey() + " indexed? " + hasIndexTerm(e.getKey()) + " " + helper.isIndexedField(e.getKey()));
                }
                
                values.putAll(createForwardIndices(helper, event, fields, value, visibility, maskedVisibility, maskedFieldHelper, shardId, indexedValue,
                                reporter));
                
                if (getProduceStats())
                    values.putAll(createStats(helper, event, fields, value, visibility, maskedVisibility, maskedFieldHelper, shardId, indexedValue, reporter));
                
                if (getShardDictionaryIndexTableName() != null) {
                    if (dCache.getIfPresent(value.getIndexedFieldName() + value.getIndexedFieldValue() + visibility + maskedVisibility) == null) {
                        createDictionaryColumn(event, values, value.getIndexedFieldName(), value.getIndexedFieldValue(), visibility, maskedVisibility,
                                        maskedFieldHelper, this.SHARD_DINDX_FLABEL, this.getShardDictionaryIndexTableName());
                        createDictionaryColumn(event, values, value.getIndexedFieldName(), StringUtils.reverse(value.getIndexedFieldValue()), visibility,
                                        maskedVisibility, maskedFieldHelper, this.SHARD_DINDX_RLABEL, this.getShardDictionaryIndexTableName());
                    }
                    dCache.put(value.getIndexedFieldName() + value.getIndexedFieldValue() + visibility + maskedVisibility, e.getValue().getIndexedFieldValue());
                }
                
            }
            
            for (Entry<String,NormalizedContentInterface> e : getGlobalReverseIndexTerms().entries()) {
                NormalizedContentInterface value = e.getValue();
                byte[] visibility = getVisibility(event, value);
                values.putAll(createReverseIndices(helper, event, fields, value, visibility, maskedVisibility, maskedFieldHelper, shardId, indexedValue,
                                reporter));
                
            }
            
        }
        
        return values;
    }
    
    protected MaskedFieldHelper createMaskedFieldHelper(IngestHelperInterface helper, RawRecordContainer event) {
        return null;
    }
    
    protected byte[] computeMaskedVisibility(RawRecordContainer event) {
        return null;
    }
    
    protected Multimap<BulkIngestKey,Value> createStats(IngestHelperInterface helper, RawRecordContainer event,
                    Multimap<String,NormalizedContentInterface> fields, NormalizedContentInterface value, byte[] visibility, byte[] maskedVisibility,
                    MaskedFieldHelper maskedFieldHelper, byte[] shardId, Value indexValue, StatusReporter reporter) {
        Multimap<BulkIngestKey,Value> values = HashMultimap.create();
        
        // produce cardinality of terms
        values.putAll(createTermIndexColumn(event, value.getIndexedFieldName(), value.getIndexedFieldValue(), visibility, maskedVisibility, maskedFieldHelper,
                        shardId, this.getIndexStatsTableName(), indexValue));
        
        String reverse = new StringBuilder(value.getIndexedFieldValue()).reverse().toString();
        
        values.putAll(createTermIndexColumn(event, value.getIndexedFieldName(), reverse, visibility, maskedVisibility, maskedFieldHelper, shardId,
                        this.getIndexStatsTableName(), indexValue));
        
        return values;
    }
    
    /**
     * @param helper
     * @param event
     * @param fields
     * @param value
     * @param visibility
     * @param maskedVisibility
     * @param maskedFieldHelper
     * @param shardId
     * @param indexValue
     * @param reporter
     */
    protected Multimap<BulkIngestKey,Value> createForwardIndices(IngestHelperInterface helper, RawRecordContainer event,
                    Multimap<String,NormalizedContentInterface> fields, NormalizedContentInterface value, byte[] visibility, byte[] maskedVisibility,
                    MaskedFieldHelper maskedFieldHelper, byte[] shardId, Value indexValue, StatusReporter reporter) {
        
        Multimap<BulkIngestKey,Value> values = HashMultimap.create();
        
        String fieldName = value.getIndexedFieldName();
        String fieldValue = value.getIndexedFieldValue();
        // produce field index.
        values.putAll(createShardFieldIndexColumn(event, fieldName, fieldValue, visibility, maskedVisibility, maskedFieldHelper, shardId,
                        createBloomFilter(event, fields, reporter)));
        
        // produce index column
        values.putAll(createTermIndexColumn(event, fieldName, fieldValue, visibility, maskedVisibility, maskedFieldHelper, shardId,
                        this.getShardIndexTableName(), indexValue));
        
        return values;
    }
    
    protected Multimap<BulkIngestKey,Value> createReverseIndices(IngestHelperInterface helper, RawRecordContainer event,
                    Multimap<String,NormalizedContentInterface> fields, NormalizedContentInterface value, byte[] visibility, byte[] maskedVisibility,
                    MaskedFieldHelper maskedFieldHelper, byte[] shardId, Value indexValue, StatusReporter reporter) {
        
        Multimap<BulkIngestKey,Value> values = HashMultimap.create();
        
        String fieldName = value.getIndexedFieldName();
        String fieldValue = value.getIndexedFieldValue();
        // produce index column
        values.putAll(createTermIndexColumn(event, fieldName, fieldValue, visibility, maskedVisibility, maskedFieldHelper, shardId,
                        this.getShardReverseIndexTableName(), indexValue));
        
        return values;
    }
    
    private Multimap<String,NormalizedContentInterface> filterByRequestedFields(Multimap<String,NormalizedContentInterface> eventFields) {
        Multimap<String,NormalizedContentInterface> filteredMap = HashMultimap.create();
        for (String requestedField : this.requestedFieldsForReindex) {
            // the keys correspond to getIndexedFieldName
            filteredMap.putAll(requestedField, eventFields.get(requestedField));
        }
        return filteredMap;
    }
    
    /**
     * Creates a bloom filter based on a multi-map of normalized fields. The returned wrapper contains not only an instance of the finalized {@link BloomFilter}
     * class, but also counts of applied field values and n-grams to assist with statistical reporting.
     * 
     * @param fields
     *            a multi-map of normalized fields
     * @return a wrapped bloom filter created from a multi-map of normalized fields
     */
    protected BloomFilterWrapper createBloomFilter(final Multimap<String,NormalizedContentInterface> fields) {
        final BloomFilterUtil factory = BloomFilterUtil.newInstance();
        return factory.newMultimapBasedFilter(fields);
    }
    
    /**
     * @param event
     * @param fields
     * @param reporter
     */
    protected Value createBloomFilter(RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields, StatusReporter reporter) {
        Value filterValue = DataTypeHandler.NULL_VALUE;
        if (this.bloomFiltersEnabled) {
            
            try {
                // Create and start the stopwatch
                final Stopwatch stopWatch = new Stopwatch();
                stopWatch.start();
                
                // Create the bloom filter, which may involve NGram expansion
                final BloomFilterWrapper result = this.createBloomFilter(fields);
                final BloomFilter<String> bloomFilter = result.getFilter();
                filterValue = MemberShipTest.toValue(bloomFilter);
                
                // Stop the stopwatch
                stopWatch.stop();
                
                if (null != reporter) {
                    final Counter filterCounter = reporter.getCounter(MemberShipTest.class.getSimpleName(), "BloomFilterCreated");
                    if (null != filterCounter) {
                        filterCounter.increment(1);
                    }
                    
                    final Counter sizeCounter = reporter.getCounter(MemberShipTest.class.getSimpleName(), "BloomFilterSize");
                    if (null != sizeCounter) {
                        sizeCounter.increment(filterValue.getSize());
                    }
                    
                    final Counter fieldsCounter = reporter.getCounter(MemberShipTest.class.getSimpleName(), "BloomFilterAppliedFields");
                    if (null != fieldsCounter) {
                        fieldsCounter.increment(result.getFieldValuesAppliedToFilter());
                    }
                    
                    final Counter ngramsCounter = reporter.getCounter(MemberShipTest.class.getSimpleName(), "BloomFilterAppliedNGrams");
                    if (null != ngramsCounter) {
                        ngramsCounter.increment(result.getNGramsAppliedToFilter());
                    }
                    
                    final Counter prunedCounter = reporter.getCounter(MemberShipTest.class.getSimpleName(), "BloomFilterPrunedNGrams");
                    if (null != prunedCounter) {
                        prunedCounter.increment(result.getNGramsPrunedFromFilter());
                    }
                    
                    final Counter creationTime = reporter.getCounter(MemberShipTest.class.getSimpleName(), "Creation Time-(ms)");
                    if (null != creationTime) {
                        creationTime.increment(stopWatch.elapsed(TimeUnit.MILLISECONDS));
                    }
                }
            } catch (Exception e) {
                if (null != reporter) {
                    final Counter errorCounter = reporter.getCounter(MemberShipTest.class.getSimpleName(), "BloomFilterError");
                    if (null != errorCounter) {
                        errorCounter.increment(filterValue.getSize());
                    }
                }
            }
        }
        
        return filterValue;
        
    }
    
    /**
     * Creates a global index BulkIngestKey and Value and does apply masking logic
     * 
     * @param event
     * @param column
     * @param fieldValue
     * @param visibility
     * @param maskedVisibility
     * @param maskedFieldHelper
     * @param shardId
     * @param tableName
     * @param indexValue
     */
    protected Multimap<BulkIngestKey,Value> createTermIndexColumn(RawRecordContainer event, String column, String fieldValue, byte[] visibility,
                    byte[] maskedVisibility, MaskedFieldHelper maskedFieldHelper, byte[] shardId, Text tableName, Value indexValue) {
        // Shard Global Index Table Structure
        // Row: Field Value
        // Colf: Field Name
        // Colq: Shard Id : DataType
        // Value: UID
        
        Multimap<BulkIngestKey,Value> values = ArrayListMultimap.create();
        
        if (log.isTraceEnabled()) {
            log.trace("Create index column " + tableName);
        }
        if (null == tableName) {
            return values;
        }
        
        // hold on to the helper
        IngestHelperInterface helper = this.getHelper(event.getDataType());
        boolean deleteMode = helper.getDeleteMode();
        
        if (null != maskedFieldHelper && maskedFieldHelper.contains(column)) {
            // These Keys are for the index, so if they are masked, we really want to use the normalized masked values
            final String normalizedMaskedValue = helper.getNormalizedMaskedValue(column);
            
            Text colf = new Text(column);
            Text colq = new Text(shardId);
            TextUtil.textAppend(colq, event.getDataType().outputName(), helper.getReplaceMalformedUTF8());
            
            Value val = indexValue;
            
            // Dont create index entries for empty values
            if (!StringUtils.isEmpty(normalizedMaskedValue)) {
                // Create a key for the masked field value with the masked visibility
                Key k = this.createIndexKey(normalizedMaskedValue.getBytes(), colf, colq, maskedVisibility, event.getDate(), false);
                
                BulkIngestKey bkey = new BulkIngestKey(tableName, k);
                values.put(bkey, val);
            }
            
            if (!StringUtils.isEmpty(fieldValue)) {
                // Now create a key for the unmasked value with the original visibility
                Key k = this.createIndexKey(fieldValue.getBytes(), colf, colq, visibility, event.getDate(), deleteMode);
                BulkIngestKey bkey = new BulkIngestKey(tableName, k);
                values.put(bkey, val);
            }
        } else if (!StringUtils.isEmpty(fieldValue)) {
            // This field is not masked. Add a key with the original field value and masked visibility
            Text colf = new Text(column);
            Text colq = new Text(shardId);
            TextUtil.textAppend(colq, event.getDataType().outputName(), helper.getReplaceMalformedUTF8());
            
            Value val = indexValue;
            
            /**
             * For values that are not being masked, we use the "unmaskedValue" and the masked visibility e.g. release the value as it was in the event at the
             * lower visibility
             */
            byte[] refVisibility = visibility;
            
            if (null != maskedFieldHelper) {
                refVisibility = maskedVisibility;
            }
            
            Key k = this.createIndexKey(fieldValue.getBytes(), colf, colq, refVisibility, event.getDate(), deleteMode);
            BulkIngestKey bkey = new BulkIngestKey(tableName, k);
            values.put(bkey, val);
            
        }
        
        return values;
    }
    
    /**
     * A helper routine to determine the visibility for a field.
     * 
     * @param event
     * @param value
     * @return the visibility
     */
    protected byte[] getVisibility(RawRecordContainer event, NormalizedContentInterface value) {
        ColumnVisibility visibility = event.getVisibility();
        if (value.getMarkings() != null && !value.getMarkings().isEmpty()) {
            try {
                visibility = markingFunctions.translateToColumnVisibility(value.getMarkings());
            } catch (MarkingFunctions.Exception e) {
                throw new RuntimeException("Cannot convert record-level markings into a column visibility", e);
            }
        }
        return flatten(visibility);
    }
    
    /**
     * Create a flattened visibility, using the cache if possible
     * 
     * @param vis
     * @return the flattened visibility
     */
    protected byte[] flatten(ColumnVisibility vis) {
        return markingFunctions == null ? vis.flatten() : markingFunctions.flatten(vis);
    }
    
    /**
     * Create Key from input parameters
     * 
     * @param row
     * @param colf
     * @param colq
     * @param vis
     * @param ts
     * @param delete
     * @return Accumulo Key object
     */
    protected Key createKey(byte[] row, Text colf, Text colq, byte[] vis, long ts, boolean delete) {
        Key k = new Key(row, 0, row.length, colf.getBytes(), 0, colf.getLength(), colq.getBytes(), 0, colq.getLength(), vis, 0, vis.length, ts);
        k.setDeleted(delete);
        return k;
    }
    
    /**
     * Create Key from input parameters
     * 
     * For global index keys, the granularity of the timestamp is to the millisecond, where the semantics of the index record is to the day. This makes
     * MapReduce unable to reduce all index keys together unless they occurred at the same millisecond. If we truncate the timestamp to the day, we should
     * reduce the number of keys output from a job.
     * 
     * @param row
     * @param colf
     * @param colq
     * @param vis
     * @param ts
     * @param delete
     * @return Accumulo Key object
     */
    protected Key createIndexKey(byte[] row, Text colf, Text colq, byte[] vis, long ts, boolean delete) {
        // Truncate the timestamp to the day
        long tsToDay = (ts / MS_PER_DAY) * MS_PER_DAY;
        
        Key k = new Key(row, 0, row.length, colf.getBytes(), 0, colf.getLength(), colq.getBytes(), 0, colq.getLength(), vis, 0, vis.length, tsToDay);
        k.setDeleted(delete);
        return k;
    }
    
    /**
     * Creates a shard column key and does *NOT* apply masking logic
     * 
     * @param event
     * @param colf
     * @param nFV
     * @param visibility
     * @param shardId
     */
    protected Multimap<BulkIngestKey,Value> createShardEventColumn(RawRecordContainer event, Text colf, NormalizedContentInterface nFV, byte[] visibility,
                    byte[] shardId) {
        return createShardEventColumn(event, colf, nFV, visibility, null, null, shardId);
    }
    
    /**
     * Creates a shard column key and does apply masking logic
     * 
     * @param event
     * @param colf
     * @param nFV
     * @param visibility
     * @param maskedVisibility
     * @param maskedFieldHelper
     * @param shardId
     */
    protected Multimap<BulkIngestKey,Value> createShardEventColumn(RawRecordContainer event, Text colf, NormalizedContentInterface nFV, byte[] visibility,
                    byte[] maskedVisibility, MaskedFieldHelper maskedFieldHelper, byte[] shardId) {
        
        Multimap<BulkIngestKey,Value> values = ArrayListMultimap.create();
        // hold on to the helper
        IngestHelperInterface helper = this.getHelper(event.getDataType());
        boolean replaceMalformedUTF8 = helper.getReplaceMalformedUTF8();
        boolean deleteMode = helper.getDeleteMode();
        
        String fieldName = nFV.getEventFieldName();
        String fieldValue = nFV.getEventFieldValue();
        String indexedFieldName = nFV.getIndexedFieldName();
        
        if (helper.isIndexOnlyField(indexedFieldName) || null == fieldValue) {
            return values;
        }
        
        // don't put composite fields into the event table, unless it is an overloaded composite field
        if (helper.isCompositeField(indexedFieldName) && !helper.isOverloadedCompositeField(indexedFieldName)) {
            return values;
        }
        
        // Create unmasked colq
        Text unmaskedColq = new Text(fieldName);
        if (!StringUtils.isEmpty(fieldValue)) {
            TextUtil.textAppend(unmaskedColq, fieldValue, replaceMalformedUTF8);
        }
        
        // If this field needs to be masked, then create two keys
        if (null != maskedFieldHelper && maskedFieldHelper.contains(indexedFieldName)) {
            final String maskedFieldValue = maskedFieldHelper.get(indexedFieldName);
            
            // Generate a key for the original, unmasked field field value
            if (!StringUtils.isEmpty(fieldValue)) {
                // One key with the original value and original visibility
                Key cbKey = createKey(shardId, colf, unmaskedColq, visibility, event.getDate(), deleteMode);
                BulkIngestKey bKey = new BulkIngestKey(this.getShardTableName(), cbKey);
                
                values.put(bKey, NULL_VALUE);
            }
            
            // Now generate a key for the masked field value
            createMaskedShardEventColumn(event, colf, maskedVisibility, shardId, values, replaceMalformedUTF8, deleteMode, fieldName, maskedFieldValue);
            
        } else if (!StringUtils.isEmpty(fieldValue)) {
            
            /**
             * For values that are not being masked, we use the "unmaskedValue" and the masked visibility e.g. release the value as it was in the event at the
             * lower visibility
             */
            byte[] refVisibility = visibility;
            
            if (null != maskedFieldHelper) {
                refVisibility = maskedVisibility;
            }
            
            // Else create one key for the field with the original value and the masked visiblity
            Key cbKey = createKey(shardId, colf, unmaskedColq, refVisibility, event.getDate(), deleteMode);
            BulkIngestKey bKey = new BulkIngestKey(this.getShardTableName(), cbKey);
            if (log.isTraceEnabled())
                log.trace("Creating bulk ingest Key " + bKey);
            values.put(bKey, NULL_VALUE);
        }
        
        return values;
        
    }
    
    protected void createMaskedShardEventColumn(RawRecordContainer event, Text colf, byte[] maskedVisibility, byte[] shardId,
                    Multimap<BulkIngestKey,Value> values, boolean replaceMalformedUTF8, boolean deleteMode, String fieldName, String maskedFieldValue) {
        if (!StringUtils.isEmpty(maskedFieldValue)) {
            // Create masked colq
            Text maskedColq = new Text(fieldName);
            TextUtil.textAppend(maskedColq, maskedFieldValue, replaceMalformedUTF8);
            
            // Another key with masked value and masked visibility
            Key cbKey = createKey(shardId, colf, maskedColq, maskedVisibility, event.getDate(), deleteMode);
            BulkIngestKey bKey = new BulkIngestKey(this.getShardTableName(), cbKey);
            values.put(bKey, NULL_VALUE);
        }
    }
    
    public void createShardFieldIndexColumn(RawRecordContainer event, Multimap<BulkIngestKey,Value> values, String fieldName, String fieldValue,
                    byte[] visibility, byte[] shardId, String uid, long eventTimestamp, Value value) {
        values.putAll(createShardFieldIndexColumn(event, fieldName, fieldValue, visibility, visibility, null, shardId, value));
        
    }
    
    /**
     * Creates a shard field index column Key and applies masking logic
     * 
     * @param event
     * @param fieldName
     * @param fieldValue
     * @param visibility
     * @param maskedVisibility
     * @param maskedFieldHelper
     * @param shardId
     * @param value
     */
    protected Multimap<BulkIngestKey,Value> createShardFieldIndexColumn(RawRecordContainer event, String fieldName, String fieldValue, byte[] visibility,
                    byte[] maskedVisibility, MaskedFieldHelper maskedFieldHelper, byte[] shardId, Value value) {
        if (log.isTraceEnabled())
            log.trace("Field value is " + fieldValue);
        
        // hold on to the helper
        IngestHelperInterface helper = this.getHelper(event.getDataType());
        boolean replaceMalformedUTF8 = helper.getReplaceMalformedUTF8();
        boolean deleteMode = helper.getDeleteMode();
        
        Multimap<BulkIngestKey,Value> values = HashMultimap.create();
        
        Text colf = new Text("fi");
        TextUtil.textAppend(colf, fieldName, replaceMalformedUTF8);
        Text unmaskedColq = new Text(fieldValue);
        TextUtil.textAppend(unmaskedColq, event.getDataType().outputName(), replaceMalformedUTF8);
        TextUtil.textAppend(unmaskedColq, event.getId().toString(), replaceMalformedUTF8);
        
        if (value == null) {
            value = NULL_VALUE;
        }
        
        if (null != maskedFieldHelper && maskedFieldHelper.contains(fieldName)) {
            if (!StringUtils.isEmpty(fieldValue)) {
                // Put unmasked colq with original visibility
                Key k = createKey(shardId, colf, unmaskedColq, visibility, event.getDate(), deleteMode);
                BulkIngestKey bKey = new BulkIngestKey(this.getShardTableName(), k);
                values.put(bKey, value);
            }
            
            // We need to use the normalized masked values
            final String normalizedMaskedValue = helper.getNormalizedMaskedValue(fieldName);
            if (!StringUtils.isEmpty(normalizedMaskedValue)) {
                Text maskedColq = new Text(normalizedMaskedValue);
                TextUtil.textAppend(maskedColq, event.getDataType().outputName(), replaceMalformedUTF8);
                TextUtil.textAppend(maskedColq, event.getId().toString(), replaceMalformedUTF8);
                
                // Put masked colq with masked visibility
                Key k = createKey(shardId, colf, maskedColq, maskedVisibility, event.getDate(), deleteMode);
                BulkIngestKey bKey = new BulkIngestKey(this.getShardTableName(), k);
                values.put(bKey, value);
            }
        } else if (!StringUtils.isEmpty(fieldValue)) {
            /**
             * For values that are not being masked, we use the "unmaskedValue" and the masked visibility e.g. release the value as it was in the event at the
             * lower visibility
             */
            byte[] refVisibility = visibility;
            
            if (null != maskedFieldHelper) {
                refVisibility = maskedVisibility;
            }
            
            Key k = createKey(shardId, colf, unmaskedColq, refVisibility, event.getDate(), deleteMode);
            BulkIngestKey bKey = new BulkIngestKey(this.getShardTableName(), k);
            values.put(bKey, value);
        }
        
        return values;
    }
    
    /**
     * Creates a shard field index column Key and applies masking logic
     * 
     * @param event
     * @param values
     * @param fieldName
     * @param fieldValue
     * @param maskedVisibility
     * @param maskedFieldHelper
     * @param shardId
     */
    protected void createShardFieldIndexColumn(RawRecordContainer event, Multimap<BulkIngestKey,Value> values, String fieldName, String fieldValue,
                    byte[] visibility, byte[] maskedVisibility, MaskedFieldHelper maskedFieldHelper, byte[] shardId, Value value) {
        // hold on to the helper
        IngestHelperInterface helper = this.getHelper(event.getDataType());
        boolean replaceMalformedUTF8 = helper.getReplaceMalformedUTF8();
        boolean deleteMode = helper.getDeleteMode();
        
        Text colf = new Text("fi");
        TextUtil.textAppend(colf, fieldName, replaceMalformedUTF8);
        Text unmaskedColq = new Text(fieldValue);
        TextUtil.textAppend(unmaskedColq, event.getDataType().outputName(), replaceMalformedUTF8);
        TextUtil.textAppend(unmaskedColq, event.getId().toString(), replaceMalformedUTF8);
        
        if (value == null) {
            value = NULL_VALUE;
        }
        
        if (null != maskedFieldHelper && maskedFieldHelper.contains(fieldName)) {
            if (!StringUtils.isEmpty(fieldValue)) {
                // Put unmasked colq with original visibility
                Key k = createKey(shardId, colf, unmaskedColq, visibility, event.getDate(), deleteMode);
                BulkIngestKey bKey = new BulkIngestKey(this.getShardTableName(), k);
                values.put(bKey, value);
            }
            
            // We need to use the normalized masked values
            final String normalizedMaskedValue = helper.getNormalizedMaskedValue(fieldName);
            if (!StringUtils.isEmpty(normalizedMaskedValue)) {
                Text maskedColq = new Text(normalizedMaskedValue);
                TextUtil.textAppend(maskedColq, event.getDataType().outputName(), replaceMalformedUTF8);
                TextUtil.textAppend(maskedColq, event.getId().toString(), replaceMalformedUTF8);
                
                // Put masked colq with masked visibility
                Key k = createKey(shardId, colf, maskedColq, maskedVisibility, event.getDate(), deleteMode);
                BulkIngestKey bKey = new BulkIngestKey(this.getShardTableName(), k);
                values.put(bKey, value);
            }
        } else if (!StringUtils.isEmpty(fieldValue)) {
            /**
             * For values that are not being masked, we use the "unmaskedValue" and the masked visibility e.g. release the value as it was in the event at the
             * lower visibility
             */
            byte[] refVisibility = visibility;
            
            if (null != maskedFieldHelper) {
                refVisibility = maskedVisibility;
            }
            
            Key k = createKey(shardId, colf, unmaskedColq, refVisibility, event.getDate(), deleteMode);
            
            BulkIngestKey bKey = new BulkIngestKey(this.getShardTableName(), k);
            values.put(bKey, value);
        }
    }
    
    /**
     * 
     */
    private Value createUidArray(String uid, boolean isDeleted) {
        
        // Create a UID object for the Value
        Builder uidBuilder = Uid.List.newBuilder();
        uidBuilder.setIGNORE(false);
        
        if (isDeleted)
            uidBuilder.setCOUNT(-1);
        else
            uidBuilder.setCOUNT(1);
        
        uidBuilder.addUID(uid);
        
        Uid.List uidList = uidBuilder.build();
        return new Value(uidList.toByteArray());
    }
    
    /**
     * Creates a global index BulkIngestKey and Value and does apply masking logic
     * 
     * @param event
     * @param values
     * @param fieldName
     * @param fieldValue
     * @param maskedVisibility
     * @param maskedFieldHelper
     * @param shardId
     * @param tableName
     */
    protected void createIndexColumn(RawRecordContainer event, Multimap<BulkIngestKey,Value> values, String fieldName, String fieldValue, byte[] visibility,
                    byte[] maskedVisibility, MaskedFieldHelper maskedFieldHelper, byte[] shardId, Text tableName) {
        // Shard Global Index Table Structure
        // Row: Field Value
        // Colf: Field Name
        // Colq: Shard Id : DataType
        // Value: UID
        
        // hold on to the helper
        IngestHelperInterface helper = this.getHelper(event.getDataType());
        boolean deleteMode = helper.getDeleteMode();
        
        if (null != maskedFieldHelper && maskedFieldHelper.contains(fieldName)) {
            // These Keys are for the index, so if they are masked, we really want to use the normalized masked values
            final String normalizedMaskedValue = helper.getNormalizedMaskedValue(fieldName);
            
            Text colf = new Text(fieldName);
            Text colq = new Text(shardId);
            TextUtil.textAppend(colq, event.getDataType().outputName(), helper.getReplaceMalformedUTF8());
            
            // Create a UID object for the Value
            Builder uidBuilder = Uid.List.newBuilder();
            uidBuilder.setIGNORE(false);
            if (!deleteMode) {
                uidBuilder.setCOUNT(1);
                uidBuilder.addUID(event.getId().toString());
            } else {
                uidBuilder.setCOUNT(-1);
                uidBuilder.addUID(event.getId().toString());
            }
            Uid.List uidList = uidBuilder.build();
            
            Value val = new Value(uidList.toByteArray());
            
            // Dont create index entries for empty values
            if (!StringUtils.isEmpty(normalizedMaskedValue)) {
                // Create a key for the masked field value with the masked visibility
                Key k = this.createIndexKey(normalizedMaskedValue.getBytes(), colf, colq, maskedVisibility, event.getDate(), false);
                
                BulkIngestKey bkey = new BulkIngestKey(tableName, k);
                values.put(bkey, val);
            }
            
            if (!StringUtils.isEmpty(fieldValue)) {
                // Now create a key for the unmasked value with the original visibility
                Key k = this.createIndexKey(fieldValue.getBytes(), colf, colq, visibility, event.getDate(), deleteMode);
                BulkIngestKey bkey = new BulkIngestKey(tableName, k);
                values.put(bkey, val);
            }
        } else if (!StringUtils.isEmpty(fieldValue)) {
            // This field is not masked. Add a key with the original field value and masked visibility
            Text colf = new Text(fieldName);
            Text colq = new Text(shardId);
            TextUtil.textAppend(colq, event.getDataType().outputName(), helper.getReplaceMalformedUTF8());
            
            // Create a UID object for the Value
            Builder uidBuilder = Uid.List.newBuilder();
            uidBuilder.setIGNORE(false);
            if (!deleteMode) {
                uidBuilder.setCOUNT(1);
                uidBuilder.addUID(event.getId().toString());
            } else {
                uidBuilder.setCOUNT(-1);
                uidBuilder.addUID(event.getId().toString());
            }
            Uid.List uidList = uidBuilder.build();
            Value val = new Value(uidList.toByteArray());
            
            /**
             * For values that are not being masked, we use the "unmaskedValue" and the masked visibility e.g. release the value as it was in the event at the
             * lower visibility
             */
            byte[] refVisibility = visibility;
            
            if (null != maskedFieldHelper) {
                refVisibility = maskedVisibility;
            }
            
            Key k = this.createIndexKey(fieldValue.getBytes(), colf, colq, refVisibility, event.getDate(), deleteMode);
            BulkIngestKey bkey = new BulkIngestKey(tableName, k);
            values.put(bkey, val);
            
        }
    }
    
    /**
     * Creates a global index BulkIngestKey and Value and does *NOT* apply masking logic
     * 
     * @param event
     * @param values
     * @param fieldName
     * @param fieldValue
     * @param visibility
     * @param directionColFam
     * @param tableName
     */
    protected void createDictionaryColumn(RawRecordContainer event, Multimap<BulkIngestKey,Value> values, String fieldName, String fieldValue,
                    byte[] visibility, Text directionColFam, Text tableName) {
        if (StringUtils.isEmpty(fieldValue)) {
            return;
        }
        
        // hold on to the helper
        IngestHelperInterface helper = this.getHelper(event.getDataType());
        // Shard Global Index Table Structure
        // Row: Field Value
        // Colf: Field Name
        // Colq: Shard Id : DataType
        // Value: UID
        Text colf = new Text(directionColFam);
        Text colq = new Text(fieldName);
        
        Key k = this.createIndexKey(fieldValue.getBytes(), colf, colq, visibility, event.getDate(), false);
        Value val = new Value("".getBytes());
        
        BulkIngestKey bKey = new BulkIngestKey(tableName, k);
        values.put(bKey, val);
        
    }
    
    /**
     * Creates a dictionary index BulkIngestKey and Value and does apply masking logic
     * 
     * @param event
     * @param values
     * @param fieldName
     * @param fieldValue
     * @param maskedVisibility
     * @param maskedFieldHelper
     * @param directionColFam
     * @param tableName
     */
    protected void createDictionaryColumn(RawRecordContainer event, Multimap<BulkIngestKey,Value> values, String fieldName, String fieldValue,
                    byte[] visibility, byte[] maskedVisibility, MaskedFieldHelper maskedFieldHelper, Text directionColFam, Text tableName) {
        // Shard Global Index Table Structure
        // Row: Field Value
        // Colf: Field Name
        // Colq: Shard Id : DataType
        // Value: UID
        
        // hold on to the helper
        IngestHelperInterface helper = this.getHelper(event.getDataType());
        boolean deleteMode = helper.getDeleteMode();
        
        if (null != maskedFieldHelper && maskedFieldHelper.contains(fieldName)) {
            // These Keys are for the index, so if they are masked, we really want to use the normalized masked values
            final String normalizedMaskedValue = helper.getNormalizedMaskedValue(fieldName);
            
            Text colf = new Text(directionColFam);
            Text colq = new Text(fieldName);
            
            Value val = new Value("".getBytes());
            
            // Dont create index entries for empty values
            if (!StringUtils.isEmpty(normalizedMaskedValue)) {
                // Create a key for the masked field value with the masked visibility
                Key k = this.createIndexKey(normalizedMaskedValue.getBytes(), colf, colq, maskedVisibility, event.getDate(), false);
                
                BulkIngestKey bkey = new BulkIngestKey(tableName, k);
                values.put(bkey, val);
            }
            
            if (!StringUtils.isEmpty(fieldValue)) {
                // Now create a key for the unmasked value with the original visibility
                Key k = this.createIndexKey(fieldValue.getBytes(), colf, colq, visibility, event.getDate(), deleteMode);
                BulkIngestKey bkey = new BulkIngestKey(tableName, k);
                values.put(bkey, val);
            }
        } else if (!StringUtils.isEmpty(fieldValue)) {
            // This field is not masked. Add a key with the original field value and masked visibility
            Text colf = new Text(directionColFam);
            Text colq = new Text(fieldName);
            // TextUtil.textAppend(colq, event.getDataType().outputName(), helper.getReplaceMalformedUTF8());
            
            Value val = new Value("".getBytes());
            
            /**
             * For values that are not being masked, we use the "unmaskedValue" and the masked visibility e.g. release the value as it was in the event at the
             * lower visibility
             */
            byte[] refVisibility = visibility;
            
            if (null != maskedFieldHelper) {
                refVisibility = maskedVisibility;
            }
            
            Key k = this.createIndexKey(fieldValue.getBytes(), colf, colq, refVisibility, event.getDate(), deleteMode);
            BulkIngestKey bkey = new BulkIngestKey(tableName, k);
            values.put(bkey, val);
            
        }
    }
    
    public Text getShardDictionaryIndexTableName() {
        return shardDictionaryName;
    }
    
    public void setShardDictionaryIndexTableName(Text shardDXName) {
        this.shardDictionaryName = shardDXName;
    }
    
    public Text getShardTableName() {
        return shardTableName;
    }
    
    public Text getShardIndexTableName() {
        return shardIndexTableName;
    }
    
    public Text getShardReverseIndexTableName() {
        return shardReverseIndexTableName;
    }
    
    public Text getMetadataTableName() {
        return metadataTableName;
    }
    
    public Text getLoadDatesTableName() {
        return loadDatesTableName;
    }
    
    public void setShardTableName(Text shardTableName) {
        this.shardTableName = shardTableName;
    }
    
    public void setShardReverseIndexTableName(Text shardReverseIndexTableName) {
        this.shardReverseIndexTableName = shardReverseIndexTableName;
    }
    
    public void setupDictionaryCache(int size) {
        dCache = CacheBuilder.newBuilder().maximumSize(size).build(new CacheLoader<String,String>() {
            @Override
            public String load(String key) {
                return key;
            }
        });
    }
    
    public void setIndexStatsTableName(Text indexStatsTableName) {
        this.indexStatsTableName = indexStatsTableName;
    }
    
    public Text getIndexStatsTableName() {
        return indexStatsTableName;
    }
    
    public void setShardIndexTableName(Text shardIndexTableName) {
        this.shardIndexTableName = shardIndexTableName;
    }
    
    public void setProduceStats(boolean produceStats) {
        this.produceStats = produceStats;
    }
    
    public boolean getProduceStats() {
        return produceStats;
    }
    
    public void setMetadataTableName(Text metadataTableName) {
        this.metadataTableName = metadataTableName;
    }
    
    public void setLoadDatesTableName(Text loadDatesTableName) {
        this.loadDatesTableName = loadDatesTableName;
    }
    
    @Override
    public RawRecordMetadata getMetadata() {
        return metadata;
    }
    
    public void setMetadata(RawRecordMetadata metadata) {
        this.metadata = metadata;
    }
    
    /**
     * This method is called by the process method for each Event. This method will receive the map of field names and values for the Event and will return a
     * map of field names and NormalizedFieldAndValue to put into the resulting mutations. Normalization, if required, should be done at this point. This method
     * should also populate the maps that are returned from the getGlobalIndexTerms and getGlobalReverseIndexTerms methods.
     * 
     * @param event
     *            current Event object
     * @param eventFields
     *            map of field names to field values that have been parsed from the event
     * @param createGlobalIndexTerms
     *            flag indicating that global index terms should be created
     * @param createGlobalReverseIndexTerms
     *            flag indicating that global reverse index terms should be created
     * @return map of indexed (normalized) field names (key) to non-normalized field values (value) or null
     */
    protected abstract Multimap<String,NormalizedContentInterface> getShardNamesAndValues(RawRecordContainer event,
                    Multimap<String,NormalizedContentInterface> eventFields, boolean createGlobalIndexTerms, boolean createGlobalReverseIndexTerms,
                    StatusReporter reporter);
    
    /**
     * Returns the minimum amount of available disk space, expressed as a percentage, allowed for creating a complete bloom filter
     * 
     * @return the minimum amount of available disk space allowed for creating a complete bloom filter
     */
    public float getBloomFilteringDiskThreshold() {
        return this.bloomFilteringDiskThreshold;
    }
    
    /**
     * Returns the path checked for available disk space when creating a complete bloom filter
     * 
     * @return the path checked for available disk space when creating a complete bloom filter
     */
    public String getBloomFilteringDiskThresholdPath() {
        return this.bloomFilteringDiskThresholdPath;
    }
    
    /**
     * Returns a value indicating whether or not bloom filters are enabled, which is determined during setup.
     * 
     * @return true if bloom filters are enabled
     */
    public boolean getBloomFiltersEnabled() {
        return this.bloomFiltersEnabled;
    }
    
    /**
     * Returns the minimum amount of available memory, expressed as a percentage, allowed for creating a complete bloom filter
     * 
     * @return the minimum amount of available memory allowed for creating a complete bloom filter
     */
    public float getBloomFilteringMemoryThreshold() {
        return this.bloomFilteringMemoryThreshold;
    }
    
    /**
     * Returns the largest ideal size of bloom filters, in bytes, created for normalized content during ingest
     * 
     * @return the largest ideal size of bloom filters, in bytes, created for normalized content during ingest
     */
    public int getBloomFilteringOptimumMaxFilterSize() {
        return this.bloomFilteringOptimumMaxFilterSize;
    }
    
    /**
     * Returns the minimum amount of remaining task time, expressed as a percentage, allowed for creating a complete bloom filter
     * 
     * @return the minimum amount of remaining task time, expressed as a percentage, allowed for creating a complete bloom filter
     */
    public float getBloomFilteringTimeoutThreshold() {
        return this.bloomFilteringTimeoutThreshold;
    }
    
    /**
     * @return map of field names (key) to normalized field values (value) or null
     */
    protected abstract Multimap<String,NormalizedContentInterface> getGlobalIndexTerms();
    
    /**
     * @return map of field names (key) to normalized reversed field values (value) or null
     */
    protected abstract Multimap<String,NormalizedContentInterface> getGlobalReverseIndexTerms();
    
    protected abstract boolean hasIndexTerm(String fieldName);
    
    protected abstract boolean hasReverseIndexTerm(String fieldName);
    
    public boolean getSuppressEventKeys() {
        return suppressEventKeys;
    }
    
    /**
     * helper object
     * 
     * @return helper object used in the subclass
     */
    @Override
    public abstract IngestHelperInterface getHelper(Type type);
    
    @Override
    public void close(TaskAttemptContext context) {}
    
}
