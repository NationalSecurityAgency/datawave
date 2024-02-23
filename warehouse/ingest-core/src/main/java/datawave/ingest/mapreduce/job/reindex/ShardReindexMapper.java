package datawave.ingest.mapreduce.job.reindex;

import static datawave.ingest.data.config.DataTypeHelper.Properties.DATA_NAME;
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
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.data.hash.HashUID;
import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.AbstractContentIngestHelper;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.mapreduce.ContextWrappedStatusReporter;
import datawave.ingest.mapreduce.handler.DataTypeHandler;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.partition.MultiTableRangePartitioner;
import datawave.ingest.protobuf.Uid;

public class ShardReindexMapper extends Mapper<Key,Value,BulkIngestKey,Value> {
    public static final String CLEANUP_SHARD = "cleanupShard";
    public static final String PROPAGATE_DELETES = "propagateDeletes";
    public static final String DEFAULT_DATA_TYPE = "defaultDataType";
    public static final String REPROCESS_EVENTS = "reprocessEvents";
    public static final String FLOOR_TIMESTAMPS = "floorTimestamps";
    public static final String EVENT_OVERRIDE = "eventOverride";
    public static final String EXPORT_SHARD = "exportShard";
    public static final String GENERATE_TF = "generateTF";
    public static final String DATA_TYPE_HANDLER = "dataTypeHandler";
    public static final String ENABLE_REINDEX_COUNTERS = "enableReindexCounters";
    public static final String DUMP_COUNTERS = "dumpCounters";

    private static final Logger log = Logger.getLogger(ShardReindexMapper.class);
    public static final String BATCH_PROCESSING = "batchProcessing";

    private final byte[] FI_START_BYTES = ShardReindexJob.FI_START.getBytes();
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
    // TODO process datawaveMetadata Keys
    private Text datawaveMetadataTable;

    // used for caching fi Key data for faster processing
    private byte[] lastFiBytes;
    private String normalizedFieldName;

    // counter processing
    private boolean enableReindexCounters = true;
    private boolean dumpCounters = true;
    private Map<String,Map<String,Long>> counters;

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

    // data processing/reuse
    private Multimap<String,String> dataMap;
    private RawRecordContainer event;

    // batch field processing
    /**
     * When batchProcessing is enabled, fields that are tokenized will be processed together instead of independently
     */
    private boolean batchProcessing = false;
    private String batchField = null;
    /**
     * Map contains Visibility -> Values
     */
    private Map<Text,List<String>> batchValues = null;
    private RawRecordContainer batchEvent = null;

    // TODO javadoc
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
        this.datawaveMetadataTable = new Text(config.get(ShardedDataTypeHandler.METADATA_TABLE_NAME, "DatawaveMetadata"));

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
                Class dataTypeHandlerClass = Class.forName(dataTypeHandler);
                this.indexHandler = (DataTypeHandler) dataTypeHandlerClass.getDeclaredConstructor().newInstance();
                this.indexHandler.setup(context);
            } catch (ClassNotFoundException | InvocationTargetException | InstantiationException | IllegalAccessException | NoSuchMethodException e) {
                throw new IllegalArgumentException("can not create handler for data type handler: " + dataTypeHandler, e);
            }

            try {
                this.event = new RawRecordContainerImpl();
                if (eventOverride != null) {
                    log.info("creating event override: " + this.eventOverride);
                    this.event = (RawRecordContainer) Class.forName(this.eventOverride).getDeclaredConstructor().newInstance();
                    if (this.event instanceof Configurable) {
                        ((Configurable) this.event).setConf(config);
                    }
                }
            } catch (ClassNotFoundException | InvocationTargetException | InstantiationException | IllegalAccessException | NoSuchMethodException e) {
                throw new RuntimeException("Could not create event of type: " + this.eventOverride, e);
            }

            this.enableReindexCounters = config.getBoolean(ENABLE_REINDEX_COUNTERS, this.enableReindexCounters);
            this.dumpCounters = config.getBoolean(DUMP_COUNTERS, this.dumpCounters);
            if (this.enableReindexCounters) {
                this.counters = new HashMap<>();
            }

            this.batchProcessing = config.getBoolean(BATCH_PROCESSING, this.batchProcessing);
            if (this.batchProcessing) {
                batchValues = new HashMap<>();
            }
        }
    }

    @Override
    protected void cleanup(Context context) {
        // process any remaining batch data
        if (this.batchProcessing && this.batchValues.size() > 0) {
            try {
                processBatch(context);
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException("Could not process final batch for field: " + batchField);
            }
        }

        // output counters if used
        if (this.enableReindexCounters) {
            for (String counterGroup : this.counters.keySet()) {
                Map<String,Long> groupCounters = this.counters.get(counterGroup);
                for (String counter : groupCounters.keySet()) {
                    Long value = groupCounters.get(counter);
                    // optionally dump the counters to logs
                    if (this.dumpCounters) {
                        log.info("COUNTER " + counterGroup + " " + counter + " " + value);
                    } else {
                        context.getCounter(counterGroup, counter).increment(value);
                    }
                }
            }
        }
    }

    @Override
    protected void map(Key key, Value value, Context context) throws IOException, InterruptedException {
        // TODO rewrite all of this to make sense
        // This is all a bit awkward since DatawaveKey already parses the Key, but is in the datawave-query-core package. Minimally parse the Key for three
        // purposes
        // 1. Is it an FI key. Ranges should be created to defeat non fi ranges, but no verify and skip when possible
        // 2. Get the field name. Necessary for checking how a field is indexed
        // 3. Get the data type. The data type is used to get the correct IngestHelperInterface which is used to make decisions regarding how a field is
        // indexed. This may vary from data type to data type

        // this is required for the partitioner, see EventMapper
        MultiTableRangePartitioner.setContext(context);

        // ensure the key is an fi
        final byte[] cf = key.getColumnFamilyData().getBackingArray();
        final int fiBaseLength = FI_START_BYTES.length;
        if (cf.length <= fiBaseLength || WritableComparator.compareBytes(cf, 0, fiBaseLength, FI_START_BYTES, 0, fiBaseLength) != 0) {
            // increment count of non-fi key
            incrementCounter("key types", "non-fi");
            if (cf.length == 2) {
                // tf is the only shard Key of length 2
                incrementCounter("key types", "tf");

                // get the tf field
                final String tfField = getFieldFromTF(key);

                // if reprocessing events and exporting shard and either not generating tf or this is an index only field write it to the context
                if (this.reprocessEvents && this.exportShard && (!this.generateTF || this.defaultHelper.isIndexOnlyField(tfField))) {
                    context.write(new BulkIngestKey(new Text("shard"), key), value);
                    incrementCounter("tf", tfField);
                }

                context.progress();
                // nothing else to do for this Key
                return;
            }

            // check for d column
            if (cf.length == 1 && cf[0] == 'd') {
                incrementCounter("key types", "d");
                if (this.reprocessEvents && this.exportShard) {
                    context.write(new BulkIngestKey(new Text("shard"), key), value);
                    incrementCounter("export", "d");
                }
                context.progress();
                return;
            }

            // everything else should be an event
            if (this.reprocessEvents) {
                try {
                    processEvent(context, key);
                    if (exportShard) {
                        context.write(new BulkIngestKey(new Text("shard"), key), value);
                        incrementCounter("export", "event");
                    }
                } catch (ClassNotFoundException | InvocationTargetException | InstantiationException | IllegalAccessException | NoSuchMethodException e) {
                    throw new RuntimeException("Could not process event for: " + key, e);
                }
            }
            context.progress();
            return;
        }

        // if none of the key types above, Key is an fi Key type
        processFI(context, key);
        context.progress();
    }

    // TODO add javadoc
    private void processFI(Context context, Key key) throws IOException, InterruptedException {
        final byte[] cf = key.getColumnFamilyData().getBackingArray();

        // check if it's the same target field as the last one
        final int fiBaseLength = FI_START_BYTES.length;
        final int fiBaseOffset = fiBaseLength + 1;
        // TODO add comment here
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

        // TODO review all counter names for consistency
        incrementCounter("type", dataType);
        incrementCounter("fi", this.normalizedFieldName);
        incrementCounter("shard", key.getRowData().toString());

        Text fieldValueText = null;
        Text fieldText = null;
        Text indexCq = null;
        boolean indexed = false;

        if (key.isDeleted() && !this.propagateDeletes) {
            incrementCounter("deletes", "skipped");
            return;
        } else if (key.isDeleted()) {
            incrementCounter("deletes", "propagated");
        }

        // TODO update comment
        // test if the field should have a global index built for it and write to context
        if ((!this.reprocessEvents && helper.isIndexedField(this.normalizedFieldName)) || helper.isIndexOnlyField(this.normalizedFieldName)) {
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
            context.write(bik, UID_VALUE);
            indexed = true;
            incrementCounter("index", this.normalizedFieldName);
        }

        // TODO update comment
        // TODO make logic uniform with forward index check
        // test if the field should have a reverse global index built for it and write to context
        if (helper.isReverseIndexedField(this.normalizedFieldName) && (helper.isIndexOnlyField(this.normalizedFieldName) || !this.reprocessEvents)) {
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
            context.write(bik, UID_VALUE);
            indexed = true;
            incrementCounter("reverse index", this.normalizedFieldName);
        }

        if (!indexed && this.cleanupShard) {
            // generate a delete key for this fi entry
            Key deleteKey = new Key(key);
            deleteKey.setDeleted(true);
            BulkIngestKey bik = new BulkIngestKey(this.shardTable, deleteKey);
            context.write(bik, EMPTY_VALUE);
            incrementCounter("shard cleanup", "fi");
        } else if (indexed && this.exportShard) {
            // write the FI back out so the export is complete
            BulkIngestKey bik = new BulkIngestKey(this.shardTable, key);
            context.write(bik, EMPTY_VALUE);
            incrementCounter("export", "fi");
            incrementCounter("fi", this.normalizedFieldName);
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

    // todo document
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

    private void processEvent(Context context, Key key) throws ClassNotFoundException, InvocationTargetException, InstantiationException,
                    IllegalAccessException, NoSuchMethodException, IOException, InterruptedException {
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

        // TODO check if the batch should be run because
        // A. new field
        // B. different event
        // C. ???

        // determine if this is a tokenized field. If a tokenized field buffer the Key so that all multivalued Keys can be processed together for correct token
        // offsetting
        if (this.batchProcessing && this.defaultHelper instanceof AbstractContentIngestHelper) {
            AbstractContentIngestHelper tokenHelper = (AbstractContentIngestHelper) this.defaultHelper;
            if (tokenHelper.isContentIndexField(fieldName) || tokenHelper.isReverseContentIndexField(fieldName)) {
                // delay processing this field
                // this value needs to be processed with all other values for the same field
                // check if there is a previous batch that needs to be processed
                if (this.batchValues.size() > 0) {
                    if (this.batchField.equals(fieldName) && this.batchEvent.getId().equals(this.event.getId())) {
                        addToBatch(key.getColumnVisibility(), fieldName, value.toString());

                        // nothing else to do this will be processed later
                        return;
                    } else {
                        // process the previous batch
                        processBatch(context);
                    }
                }

                // define a new batch and add a value to it for future processing
                this.batchField = fieldName;
                this.batchEvent = this.event.copy();
                addToBatch(key.getColumnVisibility(), fieldName, value.toString());

                // nothing else to do
                return;
            }
        }

        if (this.batchProcessing && this.batchValues.size() > 0) {
            // process the batch
            processBatch(context);
        }

        // if the dataMap wasn't populated above, use the current fieldName and value
        if (this.dataMap.keySet().size() == 0) {
            // process a single key
            this.dataMap.put(fieldName, value.toString());
        }

        processDataMap(context);
    }

    private void addToBatch(Text visibility, String fieldName, String value) {
        // add to this batch
        List<String> values = this.batchValues.get(visibility);

        if (values == null) {
            values = new ArrayList<>();
            this.batchValues.put(visibility, values);
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

            List<String> values = this.batchValues.get(visibility);
            for (String batchValue : values) {
                this.dataMap.put(this.batchField, batchValue);
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

        for (BulkIngestKey generated : keys.keySet()) {
            if (!generated.getTableName().toString().equals("shard")) {
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
            context.write(bik, v);
        }
    }

    /**
     * TF field is held in the last segment of the ColumnQualifier. There is no need to normalize this name because only normalized names will be used in tf
     * Keys
     *
     * @param tf
     * @return the tf field name, or null
     */
    private String getFieldFromTF(Key tf) {
        final byte[] cq = tf.getColumnQualifierData().getBackingArray();
        int cqLen = cq.length;
        for (int i = cqLen - 1; i >= 0; i--) {
            if (cq[i] == '\u0000') {
                return new String(cq, i + 1, cqLen - i - 1);
            }
        }

        return null;
    }

    private void incrementCounter(String group, String counter) {
        incrementCounter(group, counter, 1l);
    }

    private void incrementCounter(String group, String counter, long increment) {
        if (counters != null) {
            Map<String,Long> groupCounters = counters.get(group);
            if (groupCounters == null) {
                groupCounters = new HashMap<>();
                counters.put(group, groupCounters);
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
}
