package datawave.ingest.mapreduce.job.reindex;

import static datawave.ingest.data.config.DataTypeHelper.Properties.DATA_NAME;
import static datawave.ingest.mapreduce.EventMapper.CONTEXT_WRITER_CLASS;
import static datawave.ingest.mapreduce.EventMapper.CONTEXT_WRITER_OUTPUT_TABLE_COUNTERS;
import static org.apache.commons.lang3.StringUtils.reverse;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.data.hash.HashUID;
import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.mapreduce.ContextWrappedStatusReporter;
import datawave.ingest.mapreduce.handler.DataTypeHandler;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.writer.BulkContextWriter;
import datawave.ingest.mapreduce.job.writer.ContextWriter;
import datawave.ingest.mapreduce.partition.MultiTableRangePartitioner;
import datawave.ingest.protobuf.Uid;

public class ShardReindexMapper extends Mapper<Key,Value,BulkIngestKey,Value> {
    public static final String CLEANUP_SHARD = "ShardReindexMapper.cleanupShard";
    public static final String PROPAGATE_DELETES = "ShardReindexMapper.propagateDeletes";
    public static final String DEFAULT_DATA_TYPE = "ShardReindexMapper.defaultDataType";
    public static final String REPROCESS_EVENTS = "ShardReindexMapper.reprocessEvents";
    public static final String FLOOR_TIMESTAMPS = "ShardReindexMapper.floorTimestamps";
    public static final String EVENT_OVERRIDE = "ShardReindexMapper.eventOverride";
    public static final String EXPORT_SHARD = "ShardReindexMapper.exportShard";
    public static final String GENERATE_TF = "ShardReindexMapper.generateTF";
    public static final String DATA_TYPE_HANDLER = "ShardReindexMapper.dataTypeHandler";
    public static final String ENABLE_REINDEX_COUNTERS = "ShardReindexMapper.enableReindexCounters";
    public static final String DUMP_COUNTERS = "ShardReindexMapper.dumpCounters";
    public static final String BATCH_MODE = "ShardReindexMapper.batchMode";
    public static final String GENERATE_METADATA = "ShardReindexMapper.generateMetadata";
    private static final byte[] FI_START_BYTES = ShardReindexJob.FI_START.getBytes();

    private static final Logger log = Logger.getLogger(ShardReindexMapper.class);

    private final Value UID_VALUE = new Value(buildIndexValue().toByteArray());
    private final Value EMPTY_VALUE = new Value();

    private TypeRegistry typeRegistry;
    private Map<String,IngestHelperInterface> datatypeHelperCache;
    private String defaultDataType;
    private IngestHelperInterface defaultHelper;

    // target table names for output
    private Text shardTable;
    private Text indexTable;
    private Text reverseIndexTable;

    // used for caching fi Key data for faster processing
    private byte[] lastFiBytes;
    private String normalizedFieldName;

    // counter processing
    private boolean enableReindexCounters = true;
    private boolean dumpCounters = true;
    private Map<String,Map<String,Long>> counters = null;

    // reprocessing classes
    private DataTypeHandler indexHandler;
    private String eventOverride = RawRecordContainerImpl.class.getCanonicalName();

    // data flags
    private boolean cleanupShard = false;
    private boolean propagateDeletes = false;
    private boolean reprocessEvents = false;
    private boolean exportShard = false;
    private boolean generateTF = false;
    private boolean floorTimestamps = true;
    private boolean generateMetadata = false;

    // data processing/reuse
    private Multimap<String,String> dataMap;
    private RawRecordContainer event;

    // batch field processing
    /**
     * batchMode may be NONE, FIELD, or EVENT
     */
    private BatchMode batchMode = BatchMode.NONE;
    /**
     * Map from each visibility to all fields and values
     */
    private Map<Text,Map<String,List<String>>> batchValues = null;
    private RawRecordContainer batchEvent = null;
    private ContextWriter<BulkIngestKey,Value> contextWriter;

    /**
     * Setup the mapper and check for all required and inconsistent settings
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration config = context.getConfiguration();
        this.typeRegistry = TypeRegistry.getInstance(config);

        for (Type registeredType : this.typeRegistry.values()) {
            log.info("Registered type: " + registeredType.typeName() + " as " + registeredType.outputName());
        }

        this.cleanupShard = config.getBoolean(CLEANUP_SHARD, this.cleanupShard);

        this.shardTable = new Text(config.get(ShardedDataTypeHandler.SHARD_TNAME, "shard"));
        this.indexTable = new Text(config.get(ShardedDataTypeHandler.SHARD_GIDX_TNAME, "shardIndex"));
        this.reverseIndexTable = new Text(config.get(ShardedDataTypeHandler.SHARD_GRIDX_TNAME, "shardReverseIndex"));

        this.propagateDeletes = config.getBoolean(PROPAGATE_DELETES, this.propagateDeletes);

        this.datatypeHelperCache = new HashMap<>();

        this.defaultDataType = config.get(DEFAULT_DATA_TYPE);
        if (this.defaultDataType != null) {
            this.defaultHelper = this.typeRegistry.get(this.defaultDataType).getIngestHelper(config);
            log.info("default data type: " + this.defaultDataType);
        }

        this.reprocessEvents = config.getBoolean(REPROCESS_EVENTS, this.reprocessEvents);
        log.info("reprocessing events: " + this.reprocessEvents);

        this.floorTimestamps = config.getBoolean(FLOOR_TIMESTAMPS, this.floorTimestamps);

        if (this.reprocessEvents) {
            // check for consistency with cleanup shard settings
            if (this.cleanupShard) {
                throw new IllegalStateException(CLEANUP_SHARD + " and " + REPROCESS_EVENTS + " cannot both be set");
            }

            // do this here because it can take awhile
            this.dataMap = HashMultimap.create();

            this.eventOverride = config.get(EVENT_OVERRIDE);

            // must define a defaultDataType if reprocessingEvents
            if (this.defaultDataType == null) {
                throw new IllegalArgumentException("defaultDataType must be set when reprocessEvents is true");
            }

            this.exportShard = config.getBoolean(EXPORT_SHARD, this.exportShard);
            this.generateTF = config.getBoolean(GENERATE_TF, this.generateTF);

            // override the data name
            config.set(DATA_NAME, this.defaultDataType);
            log.info("Overrode " + DATA_NAME + " to " + this.defaultDataType);

            String dataTypeHandler = config.get(DATA_TYPE_HANDLER);
            if (dataTypeHandler == null) {
                throw new IllegalArgumentException("dataTypeHandler must be set when reprocessEvents is true");
            }

            try {
                this.indexHandler = (DataTypeHandler) ReflectionUtils.newInstance(Class.forName(dataTypeHandler), config);
                this.indexHandler.setup(context);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("can not create handler for data type handler: " + dataTypeHandler, e);
            }

            this.generateMetadata = config.getBoolean(GENERATE_METADATA, this.generateMetadata);

            try {
                this.event = new RawRecordContainerImpl();
                if (eventOverride != null) {
                    log.info("creating event override: " + this.eventOverride);
                    this.event = (RawRecordContainer) ReflectionUtils.newInstance(Class.forName(this.eventOverride), config);
                }
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Could not create event of type: " + this.eventOverride, e);
            }

            this.enableReindexCounters = config.getBoolean(ENABLE_REINDEX_COUNTERS, this.enableReindexCounters);
            this.dumpCounters = config.getBoolean(DUMP_COUNTERS, this.dumpCounters);
            if (this.enableReindexCounters) {
                this.counters = new HashMap<>();
            }

            this.batchMode = BatchMode.valueOf(config.get(BATCH_MODE, this.batchMode.toString()));
            if (this.batchMode != BatchMode.NONE) {
                batchValues = new HashMap<>();
            }
        }

        // create a context
        Class<? extends ContextWriter<BulkIngestKey,Value>> contextWriterClass = (Class<ContextWriter<BulkIngestKey,Value>>) context.getConfiguration()
                        .getClass(CONTEXT_WRITER_CLASS, BulkContextWriter.class, ContextWriter.class);
        try {
            contextWriter = contextWriterClass.getDeclaredConstructor().newInstance();
            contextWriter.setup(config, config.getBoolean(CONTEXT_WRITER_OUTPUT_TABLE_COUNTERS, false));
        } catch (InvocationTargetException | InstantiationException | IllegalAccessException | NoSuchMethodException e) {
            throw new IOException("Failed to initialize " + contextWriterClass + " from property " + CONTEXT_WRITER_CLASS, e);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // process any remaining batch data
        if (this.batchMode != BatchMode.NONE && this.batchValues.size() > 0) {
            try {
                processBatch(context);
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException("Could not process final batch", e);
            }
        }

        if (this.generateMetadata && this.indexHandler.getMetadata() != null) {
            for (BulkIngestKey bik : this.indexHandler.getMetadata().getBulkMetadata().keySet()) {
                for (Value v : this.indexHandler.getMetadata().getBulkMetadata().get(bik)) {
                    contextWriter.write(bik, v, context);
                }
            }
        }

        // cleanup the context writer
        contextWriter.cleanup(context);

        // output counters if used
        if (this.enableReindexCounters) {
            for (String counterGroup : this.counters.keySet()) {
                Map<String,Long> groupCounters = this.counters.get(counterGroup);
                for (String counter : groupCounters.keySet()) {
                    Long value = groupCounters.get(counter);
                    // optionally dump the counters to logs instead of the MR framework
                    if (this.dumpCounters) {
                        log.info("COUNTER " + counterGroup + " " + counter + " " + value);
                    } else {
                        context.getCounter(counterGroup, counter).increment(value);
                    }
                }
            }
        }
    }

    /**
     * Only sharded column family length of one is d
     *
     * @param cf
     * @return
     */
    public static boolean isKeyD(ByteSequence cf) {
        return cf.length() == 1;
    }

    /**
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    private void processDKey(Key key, Value value, Context context) throws IOException, InterruptedException {
        if (this.reprocessEvents && this.exportShard) {
            contextWriter.write(new BulkIngestKey(shardTable, key), value, context);
            incrementCounter("export", "d");
        }
    }

    /**
     * Only sharded column family length of two is tf
     *
     * @param cf
     * @return
     */
    public static boolean isKeyTF(ByteSequence cf) {
        return cf.length() == 2;
    }

    private void processTFKey(Key key, Value value, Context context) throws IOException, InterruptedException {
        // get the tf field
        final String tfField = getFieldFromTF(key);

        // if reprocessing events and exporting shard and either not generating tf or this is an index only field write it to the context
        if (this.reprocessEvents && this.exportShard && (!this.generateTF || this.defaultHelper.isIndexOnlyField(tfField))) {
            contextWriter.write(new BulkIngestKey(shardTable, key), value, context);
            incrementCounter("tf", tfField);
            incrementCounter("export", "tf");
        }
    }

    /**
     * The key must be at least 4 characters and begin with {@link #FI_START_BYTES}
     *
     * @param cf
     * @return
     */
    public static boolean isKeyFI(ByteSequence cf) {
        return cf.length() > 3 && WritableComparator.compareBytes(cf.getBackingArray(), 0, 3, FI_START_BYTES, 0, 3) == 0;
    }

    private void processFIKey(Key key, Value value, Context context) throws IOException, InterruptedException {
        if (!this.reprocessEvents || (this.reprocessEvents && this.defaultHelper.isIndexOnlyField(getFieldFromFI(key)))) {
            processFI(context, key);
        }
    }

    private void processEventKey(Key key, Value value, Context context) throws IOException, InterruptedException {
        if (this.reprocessEvents) {
            processEvent(context, key);
            if (exportShard) {
                contextWriter.write(new BulkIngestKey(shardTable, key), value, context);
                incrementCounter("export", "e");
            }
        }
    }

    /**
     * Expects key/value pairs from the shard table {@link ShardedDataTypeHandler}. The key will be parsed to determine Sharded key type and processed according
     * to configuration options. <br>
     *
     * @param key
     *            shard table key
     * @param value
     *            shard table value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(Key key, Value value, Context context) throws IOException, InterruptedException {
        try {
            // this is required for the partitioner, see EventMapper
            MultiTableRangePartitioner.setContext(context);

            ByteSequence cf = key.getColumnFamilyData();
            String keyType;
            if (isKeyD(cf)) {
                keyType = "d";
                processDKey(key, value, context);
            } else if (isKeyTF(cf)) {
                keyType = "tf";
                processTFKey(key, value, context);
            } else if (isKeyFI(cf)) {
                keyType = "fi";
                processFIKey(key, value, context);
            } else {
                keyType = "e";
                processEventKey(key, value, context);
            }

            incrementCounter("shard", key.getRowData().toString());
            incrementCounter("key types", keyType);
        } catch (IOException | InterruptedException e) {
            contextWriter.rollback();
            throw e;
        } finally {
            contextWriter.commit(context);
        }
        context.progress();
    }

    /**
     * To process an FI by looking up the associated {@link IngestHelperInterface} and checking its indexed state. Index only fields will always have index
     * entries generated. Indexed fields that are not index only will only be generated if {@link #reprocessEvents} is false. When {@link #cleanupShard} is true
     * if the field is no longer indexed a delete key will be generated for the fi entry. When {@link #exportShard} is enabled the fi entry will be written as
     * long as the field is still indexed
     *
     * @param context
     * @param key
     * @throws IOException
     * @throws InterruptedException
     */
    private void processFI(Context context, Key key) throws IOException, InterruptedException {
        final byte[] cf = key.getColumnFamilyData().getBackingArray();

        // check if it's the same target field as the last one
        final int fiBaseLength = FI_START_BYTES.length;
        final int fiBaseOffset = fiBaseLength + 1;

        // quickly compare the bytes against the last processed bytes to save on parse time if possible
        if (this.lastFiBytes == null || WritableComparator.compareBytes(cf, fiBaseOffset, cf.length - fiBaseOffset, this.lastFiBytes, fiBaseOffset,
                        this.lastFiBytes.length - fiBaseOffset) != 0) {
            // get the field from the cf
            this.normalizedFieldName = new String(cf, fiBaseLength, cf.length - fiBaseLength);
            this.lastFiBytes = cf;
        }

        // parse the dataType from the cq
        final byte[] cq = key.getColumnQualifierData().getBackingArray();
        String uid = null;
        String dataType = null;
        ByteSequence fieldValue = null;
        int cqLen = cq.length;
        int uidNull = -1;
        for (int i = cqLen - 1; i >= 0; i--) {
            if (cq[i] == '\u0000') {
                if (uid == null) {
                    uid = new String(cq, i + 1, cqLen - i - 1);
                    uidNull = i;
                } else if (dataType == null) {
                    dataType = new String(cq, i + 1, uidNull - i - 1);
                    fieldValue = key.getColumnQualifierData().subSequence(0, i);
                    break;
                }
            }
        }

        // get the type from the registry or create it if not already created. There is a cache inside the Type class
        Configuration config = context.getConfiguration();
        IngestHelperInterface helper = getIngestHelper(dataType, config);

        if (helper == null) {
            log.error("cannot find IngestHelperInterface for dataType: " + dataType + " key: " + key);
            throw new IllegalStateException("cannot find IngestHelperInterface for dataType: " + dataType + " key: " + key);
        }

        incrementCounter("fi.dataTypes", dataType);
        incrementCounter("fi.fields", this.normalizedFieldName);

        Text fieldValueText;
        Text fieldText = null;
        Text indexCq = null;
        boolean indexed = false;

        if (key.isDeleted() && !this.propagateDeletes) {
            incrementCounter("deletes", "skipped");
            return;
        } else if (key.isDeleted()) {
            incrementCounter("deletes", "propagated");
        }

        // if the field is indexed and index only or events aren't being reprocessed
        if (helper.isIndexedField(this.normalizedFieldName) && (!this.reprocessEvents || helper.isIndexOnlyField(this.normalizedFieldName))) {
            // generate the global index key and emit it
            fieldValueText = new Text(fieldValue.toString());
            fieldText = new Text(this.normalizedFieldName);
            StringBuilder docId = new StringBuilder();
            // shard \0 dataType
            docId.append(key.getRowData()).append('\u0000').append(dataType);
            indexCq = new Text(docId.toString());

            Key globalIndexKey = new Key(fieldValueText, fieldText, indexCq, key.getColumnVisibility(), floorTimestamp(key.getTimestamp()));
            globalIndexKey.setDeleted(key.isDeleted());
            BulkIngestKey bik = new BulkIngestKey(this.indexTable, globalIndexKey);
            contextWriter.write(bik, UID_VALUE, context);
            indexed = true;
            incrementCounter("index.fields", this.normalizedFieldName);
        }

        // if the field is reverse indexed and index only or events aren't being reprocessed
        if (helper.isReverseIndexedField(this.normalizedFieldName) && (!this.reprocessEvents || helper.isIndexOnlyField(this.normalizedFieldName))) {
            // reverse the field value
            fieldValueText = new Text(reverse(fieldValue.toString()));
            if (fieldText == null) {
                fieldText = new Text(this.normalizedFieldName);
                StringBuilder docId = new StringBuilder();
                // shard \0 dataType
                docId.append(key.getRowData()).append('\u0000').append(dataType);
                indexCq = new Text(docId.toString());
            }

            Key globalReverseIndexKey = new Key(fieldValueText, fieldText, indexCq, key.getColumnVisibility(), floorTimestamp(key.getTimestamp()));
            globalReverseIndexKey.setDeleted(key.isDeleted());
            // generate the global reverse index key and emit it
            BulkIngestKey bik = new BulkIngestKey(this.reverseIndexTable, globalReverseIndexKey);
            contextWriter.write(bik, UID_VALUE, context);
            indexed = true;
            incrementCounter("reverse index", this.normalizedFieldName);
        }

        if (!indexed && this.cleanupShard) {
            // generate a delete key for this fi entry
            Key deleteKey = new Key(key);
            deleteKey.setDeleted(true);
            BulkIngestKey bik = new BulkIngestKey(this.shardTable, deleteKey);
            contextWriter.write(bik, EMPTY_VALUE, context);
            incrementCounter("shard cleanup", normalizedFieldName);
        } else if (indexed && this.exportShard) {
            // write the FI back out so the export is complete
            BulkIngestKey bik = new BulkIngestKey(this.shardTable, key);
            contextWriter.write(bik, EMPTY_VALUE, context);
            incrementCounter("export", "fi");
        }
    }

    /**
     * Optionally floor a timestamp to the beginning of the day
     *
     * @param timestamp
     * @return the original timestamp if this.floorTimestamps is false, otherwise the timestamp set to the same day at 0:0:0.000
     */
    private long floorTimestamp(long timestamp) {
        if (this.floorTimestamps) {
            Calendar c = Calendar.getInstance();
            c.setTimeInMillis(timestamp);
            c.set(Calendar.HOUR_OF_DAY, 0);
            c.set(Calendar.MINUTE, 0);
            c.set(Calendar.SECOND, 0);
            c.set(Calendar.MILLISECOND, 0);

            return c.getTimeInMillis();
        }

        return timestamp;
    }

    /**
     * Select an ingest helper from the {@link TypeRegistry} that matches the dataType. If the TypeRegistry has a {@link Type} that matches either the
     * outputName or the typeName and can be successfully created cache it for future use of this type. If no type can be instantiated from the TypeRegistry and
     * a {@link #defaultDataType} has been defined, use it.
     *
     * @param dataType
     * @param config
     * @return
     */
    private IngestHelperInterface getIngestHelper(String dataType, Configuration config) {
        // check the cache
        IngestHelperInterface helper = datatypeHelperCache.get(dataType);
        if (helper == null) {
            for (Type registeredType : typeRegistry.values()) {
                if (registeredType.outputName().equals(dataType) || registeredType.typeName().equals(dataType)) {
                    try {
                        log.info("creating type: " + registeredType.typeName() + " for datatype " + dataType);
                        Type type = registeredType;
                        // try to create the type
                        helper = type.getIngestHelper(config);
                        datatypeHelperCache.put(dataType, helper);
                        break;
                    } catch (Exception e) {
                        log.debug("failed to create type " + registeredType.typeName() + " skipping", e);
                    }
                }
            }
        }

        if (helper == null && this.defaultHelper != null) {
            helper = this.defaultHelper;
            datatypeHelperCache.put(dataType, helper);
        }

        return helper;
    }

    private void processEvent(Context context, Key key) throws IOException, InterruptedException {
        // cleanup from any previous processing
        this.dataMap.clear();
        this.event.clear();

        // cf = dataType\0uid
        ByteSequence dataType = null;
        ByteSequence uid = null;
        ByteSequence cfByteSequence = key.getColumnFamilyData();
        byte[] cf = cfByteSequence.getBackingArray();
        for (int i = 0; i < cf.length; i++) {
            if (cf[i] == '\u0000') {
                // split on this index
                dataType = cfByteSequence.subSequence(0, i);
                uid = cfByteSequence.subSequence(i + 1, cf.length);

                // no further processing necessary
                break;
            }
        }

        // cq = field\0value
        ByteSequence field = null;
        ByteSequence value = null;
        ByteSequence cqByteSequence = key.getColumnQualifierData();
        byte[] cq = cqByteSequence.getBackingArray();
        int dotIndex = -1;
        for (int i = 0; i < cq.length; i++) {
            if (cq[i] == '\u0000') {
                // split on the index
                field = cqByteSequence.subSequence(0, i);
                value = cqByteSequence.subSequence(i + 1, cq.length);

                // no further processing necessary
                break;
            } else if (cq[i] == '.' && dotIndex == -1) {
                // save this for later to strip off grouping notation
                dotIndex = i;
            }
        }

        // check for expected data
        if (dataType == null || uid == null || field == null || value == null) {
            log.warn("unexpected Event data " + key);
            incrementCounter("event", "unexpected");
            return;
        }

        incrementCounter("reindex", "event");

        // event data fields are not normalized and may have grouping notation
        // if there was a dotIndex in the field name truncate to the dot to strip off any grouping notation
        if (dotIndex != -1) {
            field = cqByteSequence.subSequence(0, dotIndex);
        }
        String fieldName = field.toString();

        incrementCounter("event", fieldName);

        // setup the event based on the key
        this.event.setId(HashUID.parse(uid.toString()));
        Type type = this.typeRegistry.get(this.defaultDataType);
        type = new Type(type.typeName(), dataType.toString(), type.getHelperClass(), type.getReaderClass(), type.getDefaultDataTypeHandlers(),
                        type.getFilterPriority(), type.getDefaultDataTypeFilters());
        this.event.setDataType(type);
        this.event.setDate(key.getTimestamp());
        this.event.setVisibility(key.getColumnVisibilityParsed());

        // check for different batch modes
        boolean addedToBatch = checkBatch(context, key, dataType, uid, fieldName, value);

        // if the dataMap wasn't populated above, use the current fieldName and value
        if (!addedToBatch && this.dataMap.keySet().size() == 0) {
            // process a single key
            this.dataMap.put(fieldName, value.toString());
        }

        processDataMap(context);
    }

    private boolean batchIncludesField(String field) {
        if (this.batchValues.size() > 0) {
            for (Text vis : this.batchValues.keySet()) {
                for (String batchField : this.batchValues.get(vis).keySet()) {
                    if (batchField.equals(field)) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    private boolean checkBatch(Context context, Key key, ByteSequence dataType, ByteSequence uid, String fieldName, ByteSequence value)
                    throws IOException, InterruptedException {
        if (this.batchMode == BatchMode.NONE) {
            return false;
        } else if (this.batchMode == BatchMode.FIELD) {
            // check the events match
            if (this.batchEvent != null && !(this.event.getId().equals(this.batchEvent.getId()) && this.event.getDate() == this.batchEvent.getDate()
                            && this.event.getDataType().equals(this.batchEvent.getDataType()))) {
                // process the existing batch even though the fields match
                processBatch(context);
            } else if (!batchIncludesField(fieldName)) {
                // fields don't match, process the existing batch
                processBatch(context);
            }

            this.batchEvent = this.event.copy();
            addToBatch(key.getColumnVisibility(), fieldName, value.toString());

            return true;
        } else if (this.batchMode == BatchMode.EVENT) {
            if (!this.batchValues.isEmpty()) {
                if (!this.batchEvent.getId().equals(this.event.getId())) {
                    processBatch(context);
                }
            }

            // set the event
            this.batchEvent = this.event.copy();

            // add to the existing/new batch
            addToBatch(key.getColumnVisibility(), fieldName, value.toString());

            return true;
        }

        return false;
    }

    private void addToBatch(Text visibility, String fieldName, String value) {
        // add to this batch
        Map<String,List<String>> fieldValues = this.batchValues.get(visibility);

        if (fieldValues == null) {
            fieldValues = new HashMap<>();
            this.batchValues.put(visibility, fieldValues);
        }

        List<String> values = fieldValues.get(fieldName);

        if (values == null) {
            values = new ArrayList<>();
            fieldValues.put(fieldName, values);
        }

        values.add(value);
    }

    private void processBatch(Context context) throws IOException, InterruptedException {
        RawRecordContainer previousEvent = this.event;

        this.event = this.batchEvent;
        // populate the dataMap with the previous batch
        for (Text visibility : this.batchValues.keySet()) {
            this.dataMap.clear();

            // override the event vis to match the vis of this data
            ColumnVisibility cv = new ColumnVisibility(visibility);
            this.event.setVisibility(cv);

            Map<String,List<String>> fieldValues = this.batchValues.get(visibility);
            for (String batchField : fieldValues.keySet()) {
                for (String batchValue : fieldValues.get(batchField)) {
                    this.dataMap.put(batchField, batchValue);
                }
            }

            // for each visibility process the event
            processDataMap(context);
        }

        // restore the event
        this.event = previousEvent;
        this.dataMap.clear();
        this.batchValues.clear();
    }

    private void processDataMap(Context context) throws IOException, InterruptedException {
        long startTime = System.currentTimeMillis();
        Multimap<String,NormalizedContentInterface> normalizedMap = this.defaultHelper.normalize(this.dataMap);
        long endTime = System.currentTimeMillis();
        incrementCounter("reindex", "normalizationTime", (endTime - startTime));

        startTime = System.currentTimeMillis();
        Multimap<BulkIngestKey,Value> keys = this.indexHandler.processBulk(null, this.event, normalizedMap, new ContextWrappedStatusReporter(context));
        endTime = System.currentTimeMillis();
        incrementCounter("reindex", "processBulk", (endTime - startTime));

        if (this.generateMetadata && this.indexHandler.getMetadata() != null) {
            this.indexHandler.getMetadata().addEventWithoutLoadDates(this.defaultHelper, this.event, normalizedMap);
        }

        for (BulkIngestKey generated : keys.keySet()) {
            if (!generated.getTableName().equals(shardTable)) {
                // non shard
                incrementCounter("table", generated.getTableName().toString());
                writeKey(context, generated, keys.get(generated));
            } else {
                // shard data
                byte[] cf = generated.getKey().getColumnFamilyData().getBackingArray();
                if (cf.length == 2) {
                    // tf
                    if (this.generateTF) {
                        // write the tf keys
                        writeKey(context, generated, keys.get(generated));
                    }
                } else if (cf.length > 3 && cf[2] == '\u0000') {
                    // fi
                    // write the fi keys
                    writeKey(context, generated, keys.get(generated));
                }
            }
        }
    }

    /**
     * Write all Values for a BulkIngestKey to the context
     *
     * @param context
     * @param bik
     * @param values
     * @throws IOException
     * @throws InterruptedException
     */
    private void writeKey(Context context, BulkIngestKey bik, Collection<Value> values) throws IOException, InterruptedException {
        for (Value v : values) {
            contextWriter.write(bik, v, context);
        }
    }

    /**
     * TF field is held in the last segment of the ColumnQualifier. There is no need to normalize this name because only normalized names will be used in tf
     * Keys
     *
     * @param tf
     * @return the tf field name, or null
     */
    public static String getFieldFromTF(Key tf) {
        final byte[] cq = tf.getColumnQualifierData().getBackingArray();
        int cqLen = cq.length;
        for (int i = cqLen - 1; i >= 0; i--) {
            if (cq[i] == '\u0000') {
                return new String(cq, i + 1, cqLen - i - 1);
            }
        }

        return null;
    }

    /**
     * FI keys will always have a column family of the form fi\x00NORMALIZED_FIELD_NAME
     *
     * @param fi
     * @return
     */
    public static String getFieldFromFI(Key fi) {
        return fi.getColumnFamilyData().subSequence(3, fi.getColumnFamilyData().length()).toString();
    }

    private void incrementCounter(String group, String counter) {
        incrementCounter(group, counter, 1l);
    }

    private void incrementCounter(String group, String counter, long increment) {
        if (this.counters != null) {
            Map<String,Long> groupCounters = this.counters.get(group);
            if (groupCounters == null) {
                groupCounters = new HashMap<>();
                this.counters.put(group, groupCounters);
            }

            Long count = groupCounters.get(counter);
            if (count == null) {
                count = 0l;
            }

            count += increment;
            groupCounters.put(counter, count);
        }
    }

    // create a uid value with no count
    private static Uid.List buildIndexValue() {
        Uid.List.Builder uidBuilder = Uid.List.newBuilder();

        uidBuilder.setIGNORE(true);
        uidBuilder.setCOUNT(1);

        return uidBuilder.build();
    }

    public enum BatchMode {
        NONE, FIELD, EVENT
    }
}
