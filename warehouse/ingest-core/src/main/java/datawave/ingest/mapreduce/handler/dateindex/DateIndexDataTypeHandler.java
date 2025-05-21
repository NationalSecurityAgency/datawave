package datawave.ingest.mapreduce.handler.dateindex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.core.common.logging.ThreadConfigurableLogger;
import datawave.data.normalizer.DateNormalizer;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.mapreduce.handler.DataTypeHandler;
import datawave.ingest.mapreduce.handler.shard.ShardIdFactory;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.metadata.RawRecordMetadata;
import datawave.ingest.table.aggregator.DateIndexDateAggregator;
import datawave.marking.MarkingFunctions;
import datawave.util.StringUtils;

/**
 * <p>
 * When the processBulk method is called on this DataTypeHandler it creates Key/Values for a date index table format. The name of this table needs to be
 * specified in the configuration and are checked upon the call to setup(). This index will support multiple dates being indexed in the same table.
 *
 * <p>
 * This class creates the following Mutations or Key/Values: <br>
 * <br>
 * <table border="1">
 * <caption>DataTypeHandler</caption>
 * <tr>
 * <th>Schema Type</th>
 * <th>Use</th>
 * <th>Row</th>
 * <th>Column Family</th>
 * <th>Column Qualifier</th>
 * <th>Value</th>
 * </tr>
 * <tr>
 * <td>date index</td>
 * <td>mapping date to event date/time or shard</td>
 * <td>date (yyyyMMdd)</td>
 * <td>type (e.g. ACTIVITY)</td>
 * <td>date\0datatype\0field (yyyyMMdd event time \0 datatype \0 field name)</td>
 * <td>shard bit string (see java.util.BitSet)</td>
 * </tr>
 * </table>
 *
 * <p>
 * The table with the name specified by {@link #DATEINDEX_TNAME} will be the date index table.
 * </p>
 *
 *
 * @param <KEYIN>
 *            type of the data type handler
 */
public class DateIndexDataTypeHandler<KEYIN> implements DataTypeHandler<KEYIN>, RawRecordMetadata {

    private static final Logger log = ThreadConfigurableLogger.getLogger(DateIndexDataTypeHandler.class);

    public static final String DATEINDEX_TNAME = "date.index.table.name";
    public static final String DATEINDEX_LPRIORITY = "date.index.table.loader.priority";

    // comma delimited <date type>=<field name> values
    public static final String DATEINDEX_TYPE_TO_FIELDS = ".date.index.type.to.field.map";

    public static final String DATEINDEX_NUM_SHARDS = "date.index.num.shards";

    protected Text dateIndexTableName = null;
    protected int dateIndexNumShards = 10;
    protected Map<String,Multimap<String,String>> dataTypeToTypeToFields = null;
    protected Configuration conf = null;
    protected DateNormalizer dateNormalizer = new DateNormalizer();
    protected ShardIdFactory shardIdFactory = null;
    protected TaskAttemptContext taskAttemptContext = null;

    public Set<Type> getDataTypes() {
        Set<Type> types = new HashSet<>();
        for (String typeName : dataTypeToTypeToFields.keySet()) {
            types.add(TypeRegistry.getType(typeName));
        }
        return Collections.unmodifiableSet(types);
    }

    public Set<String> getTypes(Type dataType) {
        Multimap<String,String> typeToFields = dataTypeToTypeToFields.get(dataType.typeName());
        if (typeToFields != null) {
            return Collections.unmodifiableSet(typeToFields.keySet());
        } else {
            return Collections.emptySet();
        }
    }

    public Set<String> getTypes(Type dataType, String field) {
        Set<String> types = new HashSet<>();
        Multimap<String,String> typeToFields = dataTypeToTypeToFields.get(dataType.typeName());
        if (typeToFields != null) {
            for (String type : typeToFields.keySet()) {
                if (typeToFields.get(type).contains(field)) {
                    types.add(type);
                }
            }
        }
        return Collections.unmodifiableSet(types);
    }

    public Set<String> getFields(Type dataType, String type) {
        Multimap<String,String> typeToFields = dataTypeToTypeToFields.get(dataType.typeName());
        if (typeToFields != null) {
            return Collections.unmodifiableSet(new HashSet<>(typeToFields.get(type)));
        } else {
            return Collections.emptySet();
        }
    }

    public Set<String> getFields(Type dataType) {
        Multimap<String,String> typeToFields = dataTypeToTypeToFields.get(dataType.typeName());
        if (typeToFields != null) {
            return Collections.unmodifiableSet(new HashSet<>(typeToFields.values()));
        } else {
            return Collections.emptySet();
        }
    }

    @Override
    public void setup(TaskAttemptContext context) {
        this.taskAttemptContext = context;
        this.conf = context.getConfiguration();
        this.shardIdFactory = new ShardIdFactory(conf);

        String tableName = conf.get(DATEINDEX_TNAME, null);
        if (null == tableName) {
            log.error(DATEINDEX_TNAME + " not specified, no date index will be created");
        } else {
            setDateIndexTableName(new Text(tableName));
        }

        this.dateIndexNumShards = conf.getInt(DATEINDEX_NUM_SHARDS, this.dateIndexNumShards);

        TypeRegistry registry = TypeRegistry.getInstance(conf);

        // instantiate the mapping of types to fields
        // now get the dates to be index for this datatype
        dataTypeToTypeToFields = new HashMap<>();
        for (Type dataType : registry.getTypes()) {
            Set<String> typeToFieldsSet = new HashSet<>();
            typeToFieldsSet.addAll(conf.getTrimmedStringCollection("all" + DATEINDEX_TYPE_TO_FIELDS));
            typeToFieldsSet.addAll(conf.getTrimmedStringCollection(dataType.typeName() + DATEINDEX_TYPE_TO_FIELDS));
            Multimap<String,String> typeToFields = HashMultimap.create();
            for (String typeToField : typeToFieldsSet) {
                String[] parts = StringUtils.split(typeToField, '=');
                if (parts.length != 2) {
                    throw new IllegalStateException("Improper date index type to field configuration: " + typeToField);
                }
                typeToFields.put(parts[0], parts[1]);
            }
            log.info(this.getClass().getSimpleName() + " configured for " + dataType.typeName() + ": " + typeToFields);
            dataTypeToTypeToFields.put(dataType.typeName(), typeToFields);
        }
    }

    @Override
    public String[] getTableNames(Configuration conf) {
        List<String> tableNames = new ArrayList<>(4);
        String tableName = conf.get(DATEINDEX_TNAME, null);
        if (null != tableName)
            tableNames.add(tableName);

        return tableNames.toArray(new String[tableNames.size()]);
    }

    @Override
    public int[] getTableLoaderPriorities(Configuration conf) {
        int[] priorities = new int[1];
        int index = 0;
        String tableName = conf.get(DATEINDEX_TNAME, null);
        if (null != tableName)
            priorities[index++] = conf.getInt(DATEINDEX_LPRIORITY, 20);

        if (index != 1) {
            return Arrays.copyOf(priorities, index);
        } else {
            return priorities;
        }
    }

    /**
     * Creates entries for the date index table, well not really. The keys are now created by the metadata mechanism as they are considered to be relatively
     * small once reduced. @see RawRecordMetadata implementation contained herein.
     */
    @Override
    public Multimap<BulkIngestKey,Value> processBulk(KEYIN keyin, RawRecordContainer event, Multimap<String,NormalizedContentInterface> eventFields,
                    StatusReporter reporter) {
        return HashMultimap.create();
    }

    /**
     * Get the date index ingest keys and merge them into the provided key multimap
     *
     * @param event
     *            the event
     * @param eventFields
     *            the fields of the event
     * @param index
     *            the ingest index the ingest index
     */
    private void getBulkIngestKeys(RawRecordContainer event, Multimap<String,NormalizedContentInterface> eventFields, Multimap<BulkIngestKey,Value> index) {
        if (dataTypeToTypeToFields.containsKey(event.getDataType().typeName()) && null != eventFields && !eventFields.isEmpty()) {
            // date index Table Structure
            // Row: date
            // Colf: type
            // Colq: date\0datatype\0field
            // Value: shard bit set

            for (Map.Entry<String,String> entry : dataTypeToTypeToFields.get(event.getDataType().typeName()).entries()) {
                String type = entry.getKey();
                String field = entry.getValue();
                for (NormalizedContentInterface nci : eventFields.get(field)) {
                    KeyValue keyValue = getDateIndexEntry(getShardId(event), event.getDataType().outputName(), type, field, nci.getIndexedFieldValue(),
                                    event.getVisibility());

                    if (keyValue != null) {
                        if (log.isDebugEnabled()) {
                            log.debug("Outputting " + keyValue + " to " + getDateIndexTableName());
                        }

                        BulkIngestKey bulkIngestKey = new BulkIngestKey(getDateIndexTableName(), keyValue.getKey());
                        if (index.containsKey(bulkIngestKey)) {
                            index.put(bulkIngestKey, keyValue.getValue());
                            DateIndexDateAggregator aggregator = new DateIndexDateAggregator();
                            Value value = aggregator.reduce(bulkIngestKey.getKey(), index.get(bulkIngestKey).iterator());
                            index.removeAll(bulkIngestKey);
                            index.put(bulkIngestKey, value);
                        } else {
                            index.put(bulkIngestKey, keyValue.getValue());
                        }
                    }
                }
            }
        }
    }

    /**
     * Construct a date index entry
     *
     * @param shardId
     *            the shard id
     * @param dataType
     *            the data type
     * @param type
     *            the type
     * @param dateField
     *            the date field
     * @param dateValue
     *            the date value
     * @param visibility
     *            the visibility
     * @return The key and value
     */
    public KeyValue getDateIndexEntry(String shardId, String dataType, String type, String dateField, String dateValue, ColumnVisibility visibility) {
        Date date = null;
        try {
            // get the date to be indexed
            date = dateNormalizer.denormalize(dateValue);
        } catch (Exception e) {
            log.error("Failed to normalize date value (skipping): " + dateValue, e);
            return null;
        }

        // set the time to 00:00:00 (for key timestamp)
        date = DateUtils.truncate(date, Calendar.DATE);

        // format the date and the shardId date as yyyyMMdd
        String rowDate = DateIndexUtil.format(date);
        String shardDate = ShardIdFactory.getDateString(shardId);

        ColumnVisibility biased = new ColumnVisibility(flatten(visibility));

        // The row is the date plus the shard partition
        String row = rowDate + '_' + getDateIndexShardPartition(rowDate, type, shardDate, dataType, dateField, new String(biased.getExpression()));

        // the colf is the type (e.g. LOAD or ACTIVITY)

        // the colq is the event date yyyyMMdd \0 the datatype \0 the field name
        String colq = shardDate + '\0' + dataType + '\0' + dateField;

        // the value is a bitset denoting the shard
        Value shardList = createDateIndexValue(ShardIdFactory.getShard(shardId));

        // create the key
        Key key = new Key(row, type, colq, biased, date.getTime());

        if (log.isTraceEnabled()) {
            log.trace("Dateate index key: " + key + " for shardId " + shardId);
        }

        return new KeyValue(key, shardList);
    }

    /**
     * Calculates the shard id of the event
     *
     * @param event
     *            the event container
     * @return Shard id
     */
    public String getShardId(RawRecordContainer event) {
        return shardIdFactory.getShardId(event);
    }

    /**
     * Calculates the shard partition for a date index entry given a list of strings
     *
     * @param values
     *            the list of values
     * @return the date index shard partition
     */
    public int getDateIndexShardPartition(String... values) {
        long hashCode = 0;
        for (String value : values) {
            hashCode += value.hashCode();
        }
        return (int) ((Integer.MAX_VALUE & hashCode) % this.dateIndexNumShards);
    }

    /**
     * Calculate a date index value for a date (yyyyMMdd) and the shard (e.g. 10)
     *
     * @param shard
     *            the shard
     * @return the value
     */
    private Value createDateIndexValue(int shard) {
        // Create a DateIndex object for the Value
        return new Value(DateIndexUtil.getBits(shard).toByteArray());
    }

    /**
     * Create a flattened visibility, using the cache if possible
     *
     * @param vis
     *            the visibility
     * @return the flattened visibility
     */
    protected byte[] flatten(ColumnVisibility vis) {
        return MarkingFunctions.Factory.createMarkingFunctions().flatten(vis);
    }

    public Text getDateIndexTableName() {
        return dateIndexTableName;
    }

    public void setDateIndexTableName(Text dateIndexTableNameText) {
        this.dateIndexTableName = dateIndexTableNameText;
    }

    @Override
    public RawRecordMetadata getMetadata() {
        return this;
    }

    /**
     * helper object
     *
     * @param type
     *            the type
     * @return helper object used in the subclass
     */
    @Override
    public IngestHelperInterface getHelper(Type type) {
        return type.getIngestHelper(conf);
    }

    @Override
    public void close(TaskAttemptContext context) {}

    /**
     * This is the metadata which will store the cached date index keys.
     */
    private Multimap<BulkIngestKey,Value> metadata = HashMultimap.create();

    @Override
    public void addEvent(IngestHelperInterface helper, RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields, long loadTimeInMillis) {
        getBulkIngestKeys(event, fields, metadata);
    }

    @Override
    public void addEvent(IngestHelperInterface helper, RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields) {
        getBulkIngestKeys(event, fields, metadata);
    }

    @Override
    public void addEventWithoutLoadDates(IngestHelperInterface helper, RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields) {
        getBulkIngestKeys(event, fields, metadata);
    }

    @Override
    public void addEvent(IngestHelperInterface helper, RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields, boolean frequency) {
        getBulkIngestKeys(event, fields, metadata);
    }

    @Override
    public Multimap<BulkIngestKey,Value> getBulkMetadata() {
        return metadata;
    }

    @Override
    public void clear() {
        metadata = HashMultimap.create();
    }

}
