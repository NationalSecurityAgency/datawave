package datawave.ingest.mapreduce.handler.shard;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.core.common.logging.ThreadConfigurableLogger;
import datawave.ingest.config.IngestConfiguration;
import datawave.ingest.config.IngestConfigurationFactory;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.config.MaskedFieldHelper;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.mapreduce.handler.DataTypeHandler;
import datawave.ingest.mapreduce.handler.dateindex.DateIndexUtil;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.statsd.StatsDEnabledDataTypeHandler;
import datawave.ingest.metadata.RawRecordMetadata;
import datawave.ingest.protobuf.Uid;
import datawave.ingest.table.config.LoadDateTableConfigHelper;
import datawave.marking.MarkingFunctions;
import datawave.marking.Markings;
import datawave.query.model.Direction;
import datawave.table.constants.TableName;

/**
 * <p>
 * When the processBulk method is called on this DataTypeHandler it creates Key/Values for the shard, shardIndex, and ShardReverseIndex tables formats. The
 * names of these tables need to be specified in the configuration and are checked upon the call to setup().
 *
 * <p>
 * This class creates the following Mutations or Key/Values: <br>
 * <br>
 * <table border="1">
 * <caption>SgardedDataType</caption>
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
 *            the data type of the data type handler
 */
public abstract class ShardedDataTypeHandler<KEYIN> extends StatsDEnabledDataTypeHandler<KEYIN> implements DataTypeHandler<KEYIN> {

    private static final Logger log = ThreadConfigurableLogger.getLogger(ShardedDataTypeHandler.class);

    public static final String NUM_SHARDS = ShardIdFactory.NUM_SHARDS;
    public static final String SHARD_TNAME = "shard.table.name";
    public static final String SHARD_STATS_TNAME = "shard.stats.table.name";
    public static final String SHARD_GIDX_TNAME = "shard.global.index.table.name";
    public static final String SHARD_BITSET_INDEX_TABLE_NAME = "shard.bitset.index.table.name";
    public static final String SHARD_DAY_INDEX_TABLE_NAME = "shard.global.day.index.table.name";
    public static final String SHARD_YEAR_INDEX_TABLE_NAME = "shard.global.year.index.table.name";
    public static final String SHARD_GRIDX_TNAME = "shard.global.rindex.table.name";
    public static final String SHARD_LPRIORITY = "shard.table.loader.priority";
    public static final String SHARD_GIDX_LPRIORITY = "shard.global.index.table.loader.priority";
    public static final String SHARD_GRIDX_LPRIORITY = "shard.global.rindex.table.loader.priority";
    public static final String SHARD_DAY_INDEX_LPRIORITY = "shard.global.shard.day.index.table.loader.priority";
    public static final String SHARD_YEAR_INDEX_LPRIORITY = "shard.global.shard.year.index.table.loader.priority";

    public static final String IS_REINDEX_ENABLED = "ingest.reindex.enabled";
    public static final String FIELDS_TO_REINDEX = "ingest.reindex.fields";

    public static final String SHARD_INDEX_ENABLED = "shard.index.enabled";
    public static final String BITSET_INDEX_ENABLED = "bitset.index.enabled";
    public static final String DAY_INDEX_ENABLED = "day.index.enabled";
    public static final String YEAR_INDEX_ENABLED = "year.index.enabled";

    /**
     * name of ACCUMULO table to store DATAWAVE metadata
     */
    public static final String METADATA_TABLE_NAME = "metadata.table.name";
    public static final String METADATA_TABLE_LOADER_PRIORITY = "metadata.table.loader.priority";

    /**
     * term dictionary optimization table
     */

    public static final String SHARD_DINDX_LPRIORITY = "shard.dictionary.index.table.loader.priority";
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
     * Enable/disable creating uids for the global index
     */
    public static final String SHARD_INDEX_CREATE_UIDS = "shard.index.create.uids";

    /**
     * Suppress event key generation making this into a psuedo re-indexing job No type prefix here as it is meant to be job level not datatype level.
     */
    public static final String SUPPRESS_EVENT_KEYS = "shard.suppress.event.key";

    // Config option name for all tables that are "sharded"
    public static final String SHARDED_TNAMES = "sharded.table.names";

    /**
     * The {@code 'fi'} column family prefix used for field index entries in the shard table (see {@link #createShardFieldIndexColumn}).
     */
    protected static final byte[] FI_COLF_PREFIX = {'f', 'i'};

    private Text shardTableName = null;
    private Text shardIndexTableName = null;
    private Text shardBitsetIndexTableName = null;
    private Text shardDayIndexTableName = null;
    private Text shardYearIndexTableName = null;
    private Text indexStatsTableName = null;
    private Text shardReverseIndexTableName = null;
    private Text metadataTableName = null;
    private Text loadDatesTableName = null;
    private Text shardDictionaryName = null;
    private RawRecordMetadata metadata = null;
    private ShardIdFactory shardIdFactory = null;
    private LoadingCache<String,String> dCache = null;
    protected MarkingFunctions<?> markingFunctions;
    protected IngestConfiguration ingestConfig = IngestConfigurationFactory.getIngestConfiguration();

    // default setting is a standard index with uid and event keys
    private boolean shardIndexEnabled = true;
    private boolean shardIndexCreateUids = true;
    private boolean suppressEventKeys = false;
    // alternate global index implementations
    private boolean bitsetIndexEnabled = false;
    private boolean dayIndexEnabled = false;
    private boolean yearIndexEnabled = false;

    /**
     * Determines whether or not we produce cardinality estimates for data
     */
    protected boolean produceStats = false;

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

        shardIndexEnabled = conf.getBoolean(SHARD_INDEX_ENABLED, true);
        if (shardIndexEnabled) {
            tableName = conf.get(SHARD_GIDX_TNAME, null);
            if (null == tableName) {
                log.warn(SHARD_GIDX_TNAME + " not specified, no global index mutations will be created.");
            } else {
                setShardIndexTableName(new Text(tableName));
            }
        }

        bitsetIndexEnabled = conf.getBoolean(BITSET_INDEX_ENABLED, false);
        if (bitsetIndexEnabled) {
            tableName = conf.get(SHARD_BITSET_INDEX_TABLE_NAME, null);
            if (tableName == null) {
                log.warn(SHARD_BITSET_INDEX_TABLE_NAME + " not specified, setting to default value");
                setShardBitsetIndexTableName(new Text(TableName.TRUNCATED_SHARD_INDEX));
            } else {
                setShardBitsetIndexTableName(new Text(tableName));
            }
        }

        dayIndexEnabled = conf.getBoolean(DAY_INDEX_ENABLED, false);
        if (dayIndexEnabled) {
            tableName = conf.get(SHARD_DAY_INDEX_TABLE_NAME, null);
            if (tableName == null) {
                log.warn(SHARD_DAY_INDEX_TABLE_NAME + " not specified, setting to default value");
                setShardDayIndexTableName(new Text(TableName.SHARD_DAY_INDEX));
            } else {
                setShardDayIndexTableName(new Text(tableName));
            }
        }

        yearIndexEnabled = conf.getBoolean(YEAR_INDEX_ENABLED, false);
        if (yearIndexEnabled) {
            tableName = conf.get(SHARD_YEAR_INDEX_TABLE_NAME, null);
            if (tableName == null) {
                log.warn(SHARD_YEAR_INDEX_TABLE_NAME + " not specified, setting to default value");
                setShardDayIndexTableName(new Text(TableName.SHARD_YEAR_INDEX));
            } else {
                setShardDayIndexTableName(new Text(tableName));
            }
        }

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

        // Event key suppression
        this.suppressEventKeys = conf.getBoolean(SUPPRESS_EVENT_KEYS, false);

        // option to create uids for the global index
        this.shardIndexCreateUids = conf.getBoolean(SHARD_INDEX_CREATE_UIDS, true);
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
                throw new RuntimeException("Missing or empty " + FIELDS_TO_REINDEX + " from configuration: " + conf);
            }
            if (log.isDebugEnabled()) {
                log.debug("list of fields to reindex: " + requestedFieldsForReindex);
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

        tableName = conf.get(SHARD_BITSET_INDEX_TABLE_NAME, null);
        if (null != tableName) {
            tableNames.add(tableName);
        }

        tableName = conf.get(SHARD_DAY_INDEX_TABLE_NAME, null);
        if (null != tableName) {
            tableNames.add(tableName);
        }

        tableName = conf.get(SHARD_YEAR_INDEX_TABLE_NAME, null);
        if (null != tableName) {
            tableNames.add(tableName);
        }

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

        tableName = conf.get(SHARD_DAY_INDEX_TABLE_NAME, null);
        if (null != tableName) {
            priorities[index++] = conf.getInt(SHARD_DAY_INDEX_LPRIORITY, 30);
        }

        tableName = conf.get(SHARD_DAY_INDEX_TABLE_NAME, null);
        if (null != tableName) {
            priorities[index++] = conf.getInt(SHARD_YEAR_INDEX_LPRIORITY, 30);
        }

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
     *            the event
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
            // create an event that returns its timestamp date
            IngestHelperInterface helper = getHelper(event.getDataType());

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
     *            the event container
     * @param fields
     *            the event fields
     * @param reporter
     *            the status reporter
     * @return the column mappings
     */
    protected Multimap<BulkIngestKey,Value> createColumns(RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields,
                    StatusReporter reporter) {
        IngestHelperInterface helper = this.getHelper(event.getDataType());

        Multimap<BulkIngestKey,Value> values = HashMultimap.create();

        byte[] maskedVisibility = computeMaskedVisibility(event);
        MaskedFieldHelper maskedFieldHelper = createMaskedFieldHelper(helper, event);

        byte[] shardId = shardIdFactory.getShardIdBytes(event);

        if (null != fields && !fields.isEmpty() && null != shardTableName) {
            // Shard Event Table Structure
            // Row: shard id
            // Colf: DataType : UID
            // Colq: FieldName : FieldValue
            // Value: NULL
            byte[] colf = ShardUtil.joinWithNulls(ShardUtil.utf8(event.getDataType().outputName()), ShardUtil.utf8(event.getId().toString()));

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
                    final String cacheKey = value.getIndexedFieldName() + value.getIndexedFieldValue() + Arrays.toString(visibility)
                                    + Arrays.toString(maskedVisibility);
                    if (dCache.getIfPresent(cacheKey) == null) {
                        createDictionaryColumn(event, values, value.getIndexedFieldName(), value.getIndexedFieldValue(), visibility, maskedVisibility,
                                        maskedFieldHelper, this.SHARD_DINDX_FLABEL, this.getShardDictionaryIndexTableName());
                        createDictionaryColumn(event, values, value.getIndexedFieldName(), StringUtils.reverse(value.getIndexedFieldValue()), visibility,
                                        maskedVisibility, maskedFieldHelper, this.SHARD_DINDX_RLABEL, this.getShardDictionaryIndexTableName());
                    }
                    dCache.put(cacheKey, e.getValue().getIndexedFieldValue());
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
                        shardId, this.getIndexStatsTableName(), indexValue, Direction.FORWARD));

        String reverse = new StringBuilder(value.getIndexedFieldValue()).reverse().toString();

        values.putAll(createTermIndexColumn(event, value.getIndexedFieldName(), reverse, visibility, maskedVisibility, maskedFieldHelper, shardId,
                        this.getIndexStatsTableName(), indexValue, Direction.REVERSE));

        return values;
    }

    /**
     * @param helper
     *            the ingest helper
     * @param event
     *            the event container
     * @param fields
     *            the event fields
     * @param value
     *            the entry value
     * @param visibility
     *            the visibility
     * @param maskedVisibility
     *            the masked visibility
     * @param maskedFieldHelper
     *            the masked field helper
     * @param shardId
     *            the shard id
     * @param indexValue
     *            the index value
     * @param reporter
     *            the status reporter
     * @return the forward indices
     */
    protected Multimap<BulkIngestKey,Value> createForwardIndices(IngestHelperInterface helper, RawRecordContainer event,
                    Multimap<String,NormalizedContentInterface> fields, NormalizedContentInterface value, byte[] visibility, byte[] maskedVisibility,
                    MaskedFieldHelper maskedFieldHelper, byte[] shardId, Value indexValue, StatusReporter reporter) {

        Multimap<BulkIngestKey,Value> values = HashMultimap.create();

        String fieldName = value.getIndexedFieldName();
        String fieldValue = value.getIndexedFieldValue();
        // produce field index.
        values.putAll(createShardFieldIndexColumn(event, fieldName, fieldValue, visibility, maskedVisibility, maskedFieldHelper, shardId, NULL_VALUE));

        // produce index column
        values.putAll(createTermIndexColumn(event, fieldName, fieldValue, visibility, maskedVisibility, maskedFieldHelper, shardId,
                        this.getShardIndexTableName(), indexValue, Direction.FORWARD));

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
                        this.getShardReverseIndexTableName(), indexValue, Direction.REVERSE));

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
     * Creates a global index BulkIngestKey and Value and does apply masking logic
     *
     * @param event
     *            the event
     * @param column
     *            the column
     * @param fieldValue
     *            the field value
     * @param visibility
     *            the event visibility
     * @param maskedVisibility
     *            the masked visibility
     * @param maskedFieldHelper
     *            the masked field helper
     * @param shardId
     *            the shard id
     * @param tableName
     *            the table name
     * @param indexValue
     *            the index value
     * @param direction
     *            the direction
     * @return the term index
     */
    protected Multimap<BulkIngestKey,Value> createTermIndexColumn(RawRecordContainer event, String column, String fieldValue, byte[] visibility,
                    byte[] maskedVisibility, MaskedFieldHelper maskedFieldHelper, byte[] shardId, Text tableName, Value indexValue, Direction direction) {
        // Shard Global Index Table Structure
        // Row: Field Value
        // Colf: Field Name
        // Colq: Shard Id : DataType
        // Value: UID

        Multimap<BulkIngestKey,Value> values = ArrayListMultimap.create();

        if (log.isTraceEnabled()) {
            log.trace("Create index column " + tableName);
        }

        if (!shardIndexEnabled && !bitsetIndexEnabled && !dayIndexEnabled && !yearIndexEnabled) {
            log.warn("no index table enabled");
        }

        if (shardIndexEnabled) {
            if (tableName == null) {
                if (log.isTraceEnabled()) {
                    log.trace("Index table name is null, not creating index keys");
                }
            } else {
                writeStandardIndexKey(values, event, column, fieldValue, visibility, maskedVisibility, maskedFieldHelper, shardId, direction, tableName,
                                indexValue);
            }
        }

        if (bitsetIndexEnabled) {
            writeBitsetIndexKey(values, event, column, fieldValue, visibility, maskedVisibility, maskedFieldHelper, shardId, direction);
        }

        if (dayIndexEnabled) {
            writeShardDayIndexKey(values, event, column, fieldValue, visibility, maskedVisibility, maskedFieldHelper, shardId, direction);
        }

        if (yearIndexEnabled) {
            writeShardYearIndexKeyBitSet(values, event, column, fieldValue, visibility, maskedVisibility, maskedFieldHelper, shardId, direction);
        }

        return values;
    }

    public void writeStandardIndexKey(Multimap<BulkIngestKey,Value> values, RawRecordContainer event, String column, String fieldValue, byte[] visibility,
                    byte[] maskedVisibility, MaskedFieldHelper maskedFieldHelper, byte[] shardId, Direction direction, Text tableName, Value indexValue) {
        // hold on to the helper
        IngestHelperInterface helper = this.getHelper(event.getDataType());
        boolean deleteMode = helper.getDeleteMode();

        if (null != maskedFieldHelper && maskedFieldHelper.contains(column)) {
            // These Keys are for the index, so if they are masked, we really want to use the normalized masked values
            // It was observed that the normalized mask values aren't coming back reversed, so account for that before creating the row.
            String normalizedMaskedValue = helper.getNormalizedMaskedValue(column);

            byte[] colq = ShardUtil.joinWithNulls(shardId, ShardUtil.utf8(event.getDataType().outputName()));

            // if this method was called with the intention to create reverse index keys, ensure the masked values are reversed.
            if (!StringUtils.isEmpty(normalizedMaskedValue)) {
                if (direction == Direction.REVERSE) {
                    normalizedMaskedValue = new StringBuilder(normalizedMaskedValue).reverse().toString();
                    if (log.isTraceEnabled()) {
                        log.trace("normalizedMaskedValue is reversed to: " + normalizedMaskedValue);
                    }
                }
                // Create a key for the masked field value with the masked visibility.
                Key k = ShardUtil.createIndexKey(normalizedMaskedValue, column, colq, maskedVisibility, event.getTimestamp(), false);
                BulkIngestKey bkey = new BulkIngestKey(tableName, k);
                values.put(bkey, indexValue);
            }
            if (!StringUtils.isEmpty(fieldValue)) {
                // Now create a key for the unmasked value with the original visibility
                Key k = ShardUtil.createIndexKey(fieldValue, column, colq, visibility, event.getTimestamp(), deleteMode);
                BulkIngestKey bkey = new BulkIngestKey(tableName, k);
                values.put(bkey, indexValue);
            }
        } else if (!StringUtils.isEmpty(fieldValue)) {
            // This field is not masked. Add a key with the original field value and masked visibility
            byte[] colq = ShardUtil.joinWithNulls(shardId, ShardUtil.utf8(event.getDataType().outputName()));

            /*
             * For values that are not being masked, we use the "unmaskedValue" and the masked visibility e.g. release the value as it was in the event at the
             * lower visibility
             */
            byte[] refVisibility = visibility;

            if (null != maskedFieldHelper) {
                refVisibility = maskedVisibility;
            }

            Key k = ShardUtil.createIndexKey(fieldValue, column, colq, refVisibility, event.getTimestamp(), deleteMode);
            BulkIngestKey bkey = new BulkIngestKey(tableName, k);
            values.put(bkey, indexValue);
        }
    }

    /**
     * Write index key for the {@link TableName#TRUNCATED_SHARD_INDEX}
     *
     * <pre>
     * value FIELD:datatype0x00yyyyMMdd (bitset denoting shard offsets)
     * </pre>
     *
     * @param values
     *            the multimap of bulk ingest keys to values
     * @param event
     *            the event
     * @param field
     *            the field
     * @param value
     *            the value
     * @param visibility
     *            the visibility bytes
     * @param maskedVisibility
     *            the masked visibility bytes
     * @param maskedFieldHelper
     *            the {@link MaskedFieldHelper}
     * @param shardId
     *            the shard id bytes
     * @param direction
     *            the direction
     */
    public void writeBitsetIndexKey(Multimap<BulkIngestKey,Value> values, RawRecordContainer event, String field, String value, byte[] visibility,
                    byte[] maskedVisibility, MaskedFieldHelper maskedFieldHelper, byte[] shardId, Direction direction) {
        if (shardId != null && value != null && field != null && visibility != null) {
            String fullShard = new String(shardId, StandardCharsets.UTF_8);
            byte[] cf = ShardUtil.utf8(field);
            byte[] cq = ShardUtil.shardPrefixedBytes(shardId, 8, ShardUtil.utf8(event.getDataType().outputName()));
            long ts = DateIndexUtil.getIndexTimestamp(event.getTimestamp());

            Key key = ShardUtil.buildKey(ShardUtil.utf8(value), cf, cf.length, cq, cq.length, visibility, ts, false);
            BulkIngestKey bulkIngestKey = new BulkIngestKey(getShardBitsetIndexTableName(), key);
            Value bitSetValue = getValueForBitsetIndex(fullShard);
            values.put(bulkIngestKey, bitSetValue);

            if (maskedFieldHelper != null && maskedFieldHelper.contains(field)) {
                // write both the masked value and masked visibility
                // and the original value at the masked visibility
                String maskedValue = maskedFieldHelper.get(field);
                Key maskedKey = ShardUtil.buildKey(ShardUtil.utf8(maskedValue), cf, cf.length, cq, cq.length, maskedVisibility, ts, false);
                BulkIngestKey maskedBulkIngestKey = new BulkIngestKey(getShardBitsetIndexTableName(), maskedKey);
                values.put(maskedBulkIngestKey, bitSetValue);
            }
        }
    }

    /**
     * Write index key for the {@link TableName#SHARD_DAY_INDEX}
     *
     * <pre>
     * yyyyMMdd-null-value FIELD:datatype bitset
     * </pre>
     *
     * @param values
     *            the multimap of bulk ingest keys to values
     * @param event
     *            the event
     * @param field
     *            the field
     * @param value
     *            the value
     * @param visibility
     *            the visibility bytes
     * @param maskedVisibility
     *            the masked visibility bytes
     * @param maskedFieldHelper
     *            the {@link MaskedFieldHelper}
     * @param shardId
     *            the shard id bytes
     * @param direction
     *            the direction
     */
    public void writeShardDayIndexKey(Multimap<BulkIngestKey,Value> values, RawRecordContainer event, String field, String value, byte[] visibility,
                    byte[] maskedVisibility, MaskedFieldHelper maskedFieldHelper, byte[] shardId, Direction direction) {
        if (shardId != null && value != null && field != null && visibility != null) {
            String fullShard = new String(shardId, StandardCharsets.UTF_8);
            byte[] cf = ShardUtil.utf8(field);
            byte[] cq = ShardUtil.utf8(event.getDataType().outputName());
            byte[] row = ShardUtil.shardPrefixedBytes(shardId, 8, ShardUtil.utf8(value));
            long ts = DateIndexUtil.getIndexTimestamp(event.getTimestamp());

            Key key = ShardUtil.buildKey(row, cf, cf.length, cq, cq.length, visibility, ts, false);
            BulkIngestKey bulkIngestKey = new BulkIngestKey(getShardDayIndexTableName(), key);
            Value bitSetValue = DateIndexUtil.getValueForDayIndex(fullShard);
            values.put(bulkIngestKey, bitSetValue);

            if (maskedFieldHelper != null && maskedFieldHelper.contains(field)) {
                // write both the masked value and masked visibility
                // and the original value at the masked visibility
                String maskedValue = maskedFieldHelper.get(field);
                byte[] maskedRow = ShardUtil.shardPrefixedBytes(shardId, 8, ShardUtil.utf8(maskedValue));
                Key maskedKey = ShardUtil.buildKey(maskedRow, cf, cf.length, cq, cq.length, maskedVisibility, ts, false);
                BulkIngestKey maskedBulkIngestKey = new BulkIngestKey(getShardDayIndexTableName(), maskedKey);
                values.put(maskedBulkIngestKey, bitSetValue);
            }
        }
    }

    /**
     * Constructs a {@link Value} that contains a {@link java.util.BitSet}'s backing byte array. The bitset index is set to the shard offset.
     * <p>
     * This is the same offset computation as the day index ({@link DateIndexUtil#getValueForDayIndex(String)}); delegate to it rather than duplicating the
     * parsing logic.
     *
     * @param shard
     *            the full shard
     * @return a Value containing a BitSet
     */
    public Value getValueForBitsetIndex(String shard) {
        return DateIndexUtil.getValueForDayIndex(shard);
    }

    /**
     * Write index key for the {@link TableName#SHARD_YEAR_INDEX}
     *
     * <pre>
     * yyyy-null-value FIELD:datatype bitset
     * </pre>
     *
     * @param values
     *            the multimap of bulk ingest keys to values
     * @param event
     *            the event
     * @param field
     *            the field
     * @param value
     *            the value
     * @param visibility
     *            the visibility bytes
     * @param maskedVisibility
     *            the masked visibility bytes
     * @param maskedFieldHelper
     *            the {@link MaskedFieldHelper}
     * @param shardId
     *            the shard id bytes
     * @param direction
     *            the direction
     */
    public void writeShardYearIndexKeyBitSet(Multimap<BulkIngestKey,Value> values, RawRecordContainer event, String field, String value, byte[] visibility,
                    byte[] maskedVisibility, MaskedFieldHelper maskedFieldHelper, byte[] shardId, Direction direction) {

        if (shardId != null && value != null && field != null && visibility != null) {
            String fullShard = new String(shardId, StandardCharsets.UTF_8);
            byte[] cf = ShardUtil.utf8(field);
            byte[] cq = ShardUtil.utf8(event.getDataType().outputName());
            // you are insane if the data is from the year 999 or 10,000
            byte[] row = ShardUtil.shardPrefixedBytes(shardId, 4, ShardUtil.utf8(value));
            long ts = DateIndexUtil.getIndexTimestamp(event.getTimestamp());

            Key key = ShardUtil.buildKey(row, cf, cf.length, cq, cq.length, visibility, ts, false);
            BulkIngestKey bulkIngestKey = new BulkIngestKey(getShardYearIndexTableName(), key);
            Value bitsetValue = DateIndexUtil.getValueForYearIndex(fullShard);
            values.put(bulkIngestKey, bitsetValue);

            if (maskedFieldHelper != null && maskedFieldHelper.contains(field)) {
                // write both the masked value and masked visibility
                // and the original value at the masked visibility
                String maskedValue = maskedFieldHelper.get(field);
                byte[] maskedRow = ShardUtil.shardPrefixedBytes(shardId, 4, ShardUtil.utf8(maskedValue));
                Key maskedKey = ShardUtil.buildKey(maskedRow, cf, cf.length, cq, cq.length, maskedVisibility, ts, false);
                BulkIngestKey maskedBulkIngestKey = new BulkIngestKey(getShardYearIndexTableName(), maskedKey);
                values.put(maskedBulkIngestKey, bitsetValue);
            }
        }
    }

    /**
     * A helper routine to determine the visibility for a field.
     *
     * @param event
     *            the event
     * @param value
     *            the entry value
     * @return the visibility
     */
    public byte[] getVisibility(RawRecordContainer event, NormalizedContentInterface value) {
        ColumnVisibility visibility = event.getVisibility();
        Markings<?> markings = value.getMarkings();
        if (markings != null && !markings.isEmpty()) {
            visibility = markings.toColumnVisibility();
        }
        return flatten(visibility);
    }

    /**
     * Normalize a ColumnVisibility expression via AccessExpression and return the expression bytes.
     *
     * @param vis
     *            the visibility
     * @return the normalized visibility bytes
     */
    protected byte[] flatten(ColumnVisibility vis) {
        return markingFunctions == null ? vis.flatten() : markingFunctions.flatten(vis);
    }

    /**
     * Creates a shard column key and does apply masking logic
     *
     * @param event
     *            the event container
     * @param colf
     *            the column family
     * @param nFV
     *            the normalized pair of the field and value
     * @param visibility
     *            the event visibility
     * @param maskedVisibility
     *            the masked visibility
     * @param maskedFieldHelper
     *            the masked field helper
     * @param shardId
     *            the shard id
     * @return the shard event column
     */
    protected Multimap<BulkIngestKey,Value> createShardEventColumn(RawRecordContainer event, byte[] colf, NormalizedContentInterface nFV, byte[] visibility,
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

        // Create unmasked colq. This is only needed (and only computed) when there is a field value to key on, both to
        // avoid wasted work and so that an unwritten key can never fail the record.
        byte[] unmaskedColq = StringUtils.isEmpty(fieldValue) ? null : ShardUtil.joinWithNulls(ShardUtil.utf8(fieldName), ShardUtil.utf8(fieldValue));

        // If this field needs to be masked, then create two keys
        if (null != maskedFieldHelper && maskedFieldHelper.contains(indexedFieldName)) {
            final String maskedFieldValue = maskedFieldHelper.get(indexedFieldName);

            // Generate a key for the original, unmasked field field value
            if (!StringUtils.isEmpty(fieldValue)) {
                // One key with the original value and original visibility
                Key cbKey = ShardUtil.createKey(shardId, colf, unmaskedColq, visibility, event.getTimestamp(), deleteMode);
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
            Key cbKey = ShardUtil.createKey(shardId, colf, unmaskedColq, refVisibility, event.getTimestamp(), deleteMode);
            BulkIngestKey bKey = new BulkIngestKey(this.getShardTableName(), cbKey);
            if (log.isTraceEnabled())
                log.trace("Creating bulk ingest Key " + bKey);
            values.put(bKey, NULL_VALUE);
        }

        return values;

    }

    // Note: replaceMalformedUTF8 is retained for signature compatibility with overriding subclasses but is no longer
    // consulted. ShardUtil.utf8 always replaces malformed input rather than throwing, so key construction can never
    // fail a record.
    protected void createMaskedShardEventColumn(RawRecordContainer event, byte[] colf, byte[] maskedVisibility, byte[] shardId,
                    Multimap<BulkIngestKey,Value> values, boolean replaceMalformedUTF8, boolean deleteMode, String fieldName, String maskedFieldValue) {
        if (!StringUtils.isEmpty(maskedFieldValue)) {
            // Create masked colq
            byte[] maskedColq = ShardUtil.joinWithNulls(ShardUtil.utf8(fieldName), ShardUtil.utf8(maskedFieldValue));

            // Another key with masked value and masked visibility
            Key cbKey = ShardUtil.createKey(shardId, colf, maskedColq, maskedVisibility, event.getTimestamp(), deleteMode);
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
     *            the event
     * @param fieldName
     *            the field name
     * @param fieldValue
     *            the field value
     * @param visibility
     *            the visibility
     * @param maskedVisibility
     *            the masked visibility
     * @param maskedFieldHelper
     *            the masked field helper
     * @param shardId
     *            the shard id
     * @param value
     *            the value
     * @return the shard field index column
     */
    protected Multimap<BulkIngestKey,Value> createShardFieldIndexColumn(RawRecordContainer event, String fieldName, String fieldValue, byte[] visibility,
                    byte[] maskedVisibility, MaskedFieldHelper maskedFieldHelper, byte[] shardId, Value value) {
        if (log.isTraceEnabled())
            log.trace("Field value is " + fieldValue);

        // hold on to the helper
        IngestHelperInterface helper = this.getHelper(event.getDataType());
        boolean deleteMode = helper.getDeleteMode();

        Multimap<BulkIngestKey,Value> values = HashMultimap.create();

        if (!StringUtils.isEmpty(fieldValue)) {
            byte[] colf = ShardUtil.joinWithNulls(FI_COLF_PREFIX, ShardUtil.utf8(fieldName));
            byte[] idSuffix = ShardUtil.joinWithNulls(ShardUtil.utf8(event.getDataType().outputName()), ShardUtil.utf8(event.getId().toString()));
            byte[] unmaskedColq = ShardUtil.joinWithNulls(ShardUtil.utf8(fieldValue), idSuffix);

            if (value == null) {
                value = NULL_VALUE;
            }

            if (null != maskedFieldHelper && maskedFieldHelper.contains(fieldName)) {
                // Put unmasked colq with original visibility
                Key k = ShardUtil.createKey(shardId, colf, unmaskedColq, visibility, event.getTimestamp(), deleteMode);
                BulkIngestKey bKey = new BulkIngestKey(this.getShardTableName(), k);
                values.put(bKey, value);

                // We need to use the normalized masked values
                final String normalizedMaskedValue = helper.getNormalizedMaskedValue(fieldName);
                if (!StringUtils.isEmpty(normalizedMaskedValue)) {
                    byte[] maskedColq = ShardUtil.joinWithNulls(ShardUtil.utf8(normalizedMaskedValue), idSuffix);

                    // Put masked colq with masked visibility
                    Key key = ShardUtil.createKey(shardId, colf, maskedColq, maskedVisibility, event.getTimestamp(), deleteMode);
                    BulkIngestKey bulkIngestKey = new BulkIngestKey(this.getShardTableName(), key);
                    values.put(bulkIngestKey, value);
                }
            } else if (!StringUtils.isEmpty(fieldValue)) {
                /**
                 * For values that are not being masked, we use the "unmaskedValue" and the masked visibility e.g. release the value as it was in the event at
                 * the lower visibility
                 */
                byte[] refVisibility = visibility;

                if (null != maskedFieldHelper) {
                    refVisibility = maskedVisibility;
                }

                Key k = ShardUtil.createKey(shardId, colf, unmaskedColq, refVisibility, event.getTimestamp(), deleteMode);
                BulkIngestKey bKey = new BulkIngestKey(this.getShardTableName(), k);
                values.put(bKey, value);
            }
        }

        return values;
    }

    /**
     * Creates a shard field index column Key and applies masking logic
     *
     * @param event
     *            the event
     * @param values
     *            map of values
     * @param fieldName
     *            the field name
     * @param fieldValue
     *            the field value
     * @param visibility
     *            the visibility
     * @param maskedVisibility
     *            the masked visibility
     * @param maskedFieldHelper
     *            the masked field helper
     * @param shardId
     *            the shard id
     * @param value
     *            the value
     */
    protected void createShardFieldIndexColumn(RawRecordContainer event, Multimap<BulkIngestKey,Value> values, String fieldName, String fieldValue,
                    byte[] visibility, byte[] maskedVisibility, MaskedFieldHelper maskedFieldHelper, byte[] shardId, Value value) {
        // hold on to the helper
        IngestHelperInterface helper = this.getHelper(event.getDataType());
        boolean deleteMode = helper.getDeleteMode();

        if (!StringUtils.isEmpty(fieldValue)) {
            byte[] colf = ShardUtil.joinWithNulls(FI_COLF_PREFIX, ShardUtil.utf8(fieldName));
            byte[] idSuffix = ShardUtil.joinWithNulls(ShardUtil.utf8(event.getDataType().outputName()), ShardUtil.utf8(event.getId().toString()));
            byte[] unmaskedColq = ShardUtil.joinWithNulls(ShardUtil.utf8(fieldValue), idSuffix);

            if (value == null) {
                value = NULL_VALUE;
            }

            if (null != maskedFieldHelper && maskedFieldHelper.contains(fieldName)) {
                // Put unmasked colq with original visibility
                Key k = ShardUtil.createKey(shardId, colf, unmaskedColq, visibility, event.getTimestamp(), deleteMode);
                BulkIngestKey bKey = new BulkIngestKey(this.getShardTableName(), k);
                values.put(bKey, value);

                // We need to use the normalized masked values
                final String normalizedMaskedValue = helper.getNormalizedMaskedValue(fieldName);
                if (!StringUtils.isEmpty(normalizedMaskedValue)) {
                    byte[] maskedColq = ShardUtil.joinWithNulls(ShardUtil.utf8(normalizedMaskedValue), idSuffix);

                    // Put masked colq with masked visibility
                    Key key = ShardUtil.createKey(shardId, colf, maskedColq, maskedVisibility, event.getTimestamp(), deleteMode);
                    BulkIngestKey bulkIngestKey = new BulkIngestKey(this.getShardTableName(), key);
                    values.put(bulkIngestKey, value);
                }
            } else if (!StringUtils.isEmpty(fieldValue)) {
                /**
                 * For values that are not being masked, we use the "unmaskedValue" and the masked visibility e.g. release the value as it was in the event at
                 * the lower visibility
                 */
                byte[] refVisibility = visibility;

                if (null != maskedFieldHelper) {
                    refVisibility = maskedVisibility;
                }

                Key k = ShardUtil.createKey(shardId, colf, unmaskedColq, refVisibility, event.getTimestamp(), deleteMode);
                BulkIngestKey bKey = new BulkIngestKey(this.getShardTableName(), k);
                values.put(bKey, value);
            }
        }
    }

    /**
     * @param uid
     *            the uid
     * @param isDeleted
     *            flag for the delete count
     * @return a value
     */
    protected Value createUidArray(String uid, boolean isDeleted) {
        return new Value(createUidList(uid, isDeleted).toByteArray());
    }

    /**
     * Create a {@link Uid.List} given a uid and delete mode flag
     *
     * @param uid
     *            an event uid
     * @param deleteMode
     *            a flag
     * @return a Uid.List
     */
    protected Uid.List createUidList(String uid, boolean deleteMode) {
        Uid.List.Builder uidBuilder = Uid.List.newBuilder();

        // delete mode takes precedent over disabled uid creation
        if (deleteMode) {
            uidBuilder.setIGNORE(false);
            uidBuilder.setCOUNT(-1);
            uidBuilder.addUID(uid);
        } else if (!shardIndexCreateUids) {
            // uid not created
            uidBuilder.setIGNORE(true);
            uidBuilder.setCOUNT(1);
        } else {
            // uid created
            uidBuilder.setIGNORE(false);
            uidBuilder.setCOUNT(1);
            uidBuilder.addUID(uid);
        }

        return uidBuilder.build();
    }

    /**
     * Creates a dictionary index BulkIngestKey and Value and does apply masking logic
     *
     * @param event
     *            the event
     * @param values
     *            the map of values
     * @param fieldName
     *            the field name
     * @param fieldValue
     *            the field value
     * @param visibility
     *            the visibility
     * @param maskedVisibility
     *            the masked visibility
     * @param maskedFieldHelper
     *            the masked field helper
     * @param directionColFam
     *            the column family direction
     * @param tableName
     *            the table name
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

            // Dont create index entries for empty values
            if (!StringUtils.isEmpty(normalizedMaskedValue)) {
                // Create a key for the masked field value with the masked visibility
                Key k = ShardUtil.createIndexKey(normalizedMaskedValue, directionColFam, fieldName, maskedVisibility, event.getTimestamp(), false);

                BulkIngestKey bkey = new BulkIngestKey(tableName, k);
                values.put(bkey, NULL_VALUE);
            }

            if (!StringUtils.isEmpty(fieldValue)) {
                // Now create a key for the unmasked value with the original visibility
                Key k = ShardUtil.createIndexKey(fieldValue, directionColFam, fieldName, visibility, event.getTimestamp(), deleteMode);
                BulkIngestKey bkey = new BulkIngestKey(tableName, k);
                values.put(bkey, NULL_VALUE);
            }
        } else if (!StringUtils.isEmpty(fieldValue)) {
            // This field is not masked. Add a key with the original field value and masked visibility

            /**
             * For values that are not being masked, we use the "unmaskedValue" and the masked visibility e.g. release the value as it was in the event at the
             * lower visibility
             */
            byte[] refVisibility = visibility;

            if (null != maskedFieldHelper) {
                refVisibility = maskedVisibility;
            }

            Key k = ShardUtil.createIndexKey(fieldValue, directionColFam, fieldName, refVisibility, event.getTimestamp(), deleteMode);
            BulkIngestKey bkey = new BulkIngestKey(tableName, k);
            values.put(bkey, NULL_VALUE);

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

    public Text getShardBitsetIndexTableName() {
        return shardBitsetIndexTableName;
    }

    public void setShardBitsetIndexTableName(Text shardBitsetIndexTableName) {
        this.shardBitsetIndexTableName = shardBitsetIndexTableName;
    }

    public Text getShardDayIndexTableName() {
        return shardDayIndexTableName;
    }

    public void setShardDayIndexTableName(Text shardDayIndexTableName) {
        this.shardDayIndexTableName = shardDayIndexTableName;
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
     * @param reporter
     *            tbe status reporter
     * @return map of indexed (normalized) field names (key) to non-normalized field values (value) or null
     */
    protected abstract Multimap<String,NormalizedContentInterface> getShardNamesAndValues(RawRecordContainer event,
                    Multimap<String,NormalizedContentInterface> eventFields, boolean createGlobalIndexTerms, boolean createGlobalReverseIndexTerms,
                    StatusReporter reporter);

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

    public void setSuppressEventKeys(boolean suppressEventKeys) {
        this.suppressEventKeys = suppressEventKeys;
    }

    public boolean isShardIndexEnabled() {
        return shardIndexEnabled;
    }

    public void setShardIndexEnabled(boolean shardIndexEnabled) {
        this.shardIndexEnabled = shardIndexEnabled;
    }

    public boolean getShardIndexCreateUids() {
        return shardIndexCreateUids;
    }

    public void setShardIndexCreateUids(boolean shardIndexCreateUids) {
        this.shardIndexCreateUids = shardIndexCreateUids;
    }

    public boolean getBitSetIndexEnabled() {
        return bitsetIndexEnabled;
    }

    public void setBitsetIndexEnabled(boolean bitsetIndexEnabled) {
        this.bitsetIndexEnabled = bitsetIndexEnabled;
    }

    public boolean getDayIndexEnabled() {
        return dayIndexEnabled;
    }

    public void setDayIndexEnabled(boolean dayIndexEnabled) {
        this.dayIndexEnabled = dayIndexEnabled;
    }

    public boolean getYearIndexEnabled() {
        return yearIndexEnabled;
    }

    public void setYearIndexEnabled(boolean yearIndexEnabled) {
        this.yearIndexEnabled = yearIndexEnabled;
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

    public Text getShardYearIndexTableName() {
        return shardYearIndexTableName;
    }

    public void setShardYearIndexTableName(Text shardYearIndexTableName) {
        this.shardYearIndexTableName = shardYearIndexTableName;
    }
}
