package datawave.ingest.mapreduce.handler.error;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.data.hash.UID;
import datawave.ingest.config.IngestConfiguration;
import datawave.ingest.config.IngestConfigurationFactory;
import datawave.ingest.data.RawDataErrorNames;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.ConfigurationHelper;
import datawave.ingest.data.config.MarkingsHelper;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.data.config.ingest.ErrorShardedIngestHelper;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.mapreduce.ContextWrappedStatusReporter;
import datawave.ingest.mapreduce.handler.ExtendedDataTypeHandler;
import datawave.ingest.mapreduce.handler.shard.AbstractColumnBasedHandler;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.writer.ContextWriter;
import datawave.marking.MarkingFunctions;

/**
 * Handler that take events with processing errors or fatal errors and dumps them into a processing error table. This table will be used for subsequent
 * debugging and reprocessing.
 *
 * <p>
 * This class creates the following Mutations or Key/Values: <br>
 * <br>
 * <table border="1">
 * <caption>ErrorShardedDataType</caption>
 * <tr>
 * <th>Schema Type</th>
 * <th>Use</th>
 * <th>Row</th>
 * <th>Column Family</th>
 * <th>Column Qualifier</th>
 * <th>Value</th>
 * </tr>
 * <tr>
 * <td>ProcessingError</td>
 * <td>capture event</td>
 * <td>JobName\0DataType\0UID</td>
 * <td>'e'</td>
 * <td>EventDate(yyyyMMdd)\0UUID 1\0UUID 2\0 ...</td>
 * <td>Event (using Writable interface)</td>
 * </tr>
 * <tr>
 * <td>ProcessingError</td>
 * <td>capture processing error</td>
 * <td>JobName\0DataType\0UID</td>
 * <td>'info'</td>
 * <td>ErrorContext\0ErrorDate(yyyyMMdd)</td>
 * <td>stack trace</td>
 * </tr>
 * <tr>
 * <td>ProcessingError</td>
 * <td>capture event fields</td>
 * <td>JobName\0DataType\0UID</td>
 * <td>'f'</td>
 * <td>eventFieldName</td>
 * <td>eventFieldValue\0indexedFieldName\0indexedFieldValue</td>
 * </tr>
 * </table>
 * <p>
 * Notes:
 * </p>
 * <ul>
 * <li>The ErrorContext in the info entries is either a RawDataError name, or the name of the event field that failed</li>
 * <li>The event date will be the empty string if unknown.</li>
 * <li>The visibility will be set if known. If no visibility marking is given, then the configured default will be applied</li>
 * <li>The timestamp will be set to the time at which the error occurred.</li>
 * <li>The UID will be set to UID("", rawdata) when not available in the event.</li>
 * </ul>
 *
 *
 *
 * @param <KEYIN>
 *            type of input key
 */
public class ErrorShardedDataTypeHandler<KEYIN,KEYOUT,VALUEOUT> extends AbstractColumnBasedHandler<KEYIN>
                implements ExtendedDataTypeHandler<KEYIN,KEYOUT,VALUEOUT> {
    private static final Logger log = Logger.getLogger(ErrorShardedDataTypeHandler.class);

    public static final String ERROR_PROP_PREFIX = "error.";

    public static final String JOB_NAME_FIELD = "JOB_NAME";
    public static final String JOB_ID_FIELD = "JOB_ID";
    public static final String UUID_FIELD = "EVENT_UUID";
    public static final String ERROR_FIELD = "ERROR";
    public static final String STACK_TRACE_FIELD = "STACKTRACE";
    public static final String EVENT_CONTENT_FIELD = "EVENT";
    public static final String PROCESSED_COUNT = ErrorDataTypeHandler.PROCESSED_COUNT;

    protected MarkingsHelper markingsHelper;

    private ErrorShardedIngestHelper errorHelper = null;

    private byte[] defaultVisibility = null;

    private Configuration conf = null;

    @Override
    public void setup(TaskAttemptContext context) {
        IngestConfiguration ingestConfiguration = IngestConfigurationFactory.getIngestConfiguration();
        markingsHelper = ingestConfiguration.getMarkingsHelper(context.getConfiguration(), TypeRegistry.getType(TypeRegistry.ERROR_PREFIX));

        super.setup(context);

        this.errorHelper = (ErrorShardedIngestHelper) (TypeRegistry.getType("error").getIngestHelper(context.getConfiguration()));
        this.errorHelper.setDelegateHelper(this.helper);
        this.helper = this.errorHelper;

        this.conf = context.getConfiguration();

        this.setupDictionaryCache(conf.getInt(ERROR_PROP_PREFIX + SHARD_DICTIONARY_CACHE_ENTRIES, ShardedDataTypeHandler.SHARD_DINDEX_CACHE_DEFAULT_SIZE));

        setShardTableName(new Text(ConfigurationHelper.isNull(conf, ERROR_PROP_PREFIX + SHARD_TNAME, String.class)));
        String tableName = conf.get(ERROR_PROP_PREFIX + SHARD_GIDX_TNAME);
        setShardIndexTableName(tableName == null ? null : new Text(tableName));
        tableName = conf.get(ERROR_PROP_PREFIX + SHARD_GRIDX_TNAME);
        setShardReverseIndexTableName(tableName == null ? null : new Text(tableName));
        tableName = conf.get(ERROR_PROP_PREFIX + METADATA_TABLE_NAME);
        if (tableName == null) {
            setMetadataTableName(null);
            setMetadata(null);
        } else {
            setMetadataTableName(new Text(tableName));
            setMetadata(ingestConfiguration.createMetadata(getShardTableName(), getMetadataTableName(), null /* no load date table */, getShardIndexTableName(),
                            getShardReverseIndexTableName(), conf.getBoolean(ERROR_PROP_PREFIX + METADATA_TERM_FREQUENCY, false)));
        }
        tableName = conf.get(ERROR_PROP_PREFIX + SHARD_DINDX_NAME);
        setShardDictionaryIndexTableName(tableName == null ? null : new Text(tableName));

        try {
            defaultVisibility = flatten(markingFunctions.translateToColumnVisibility(markingsHelper.getDefaultMarkings()));
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse security marking configuration", e);
        }

        log.info("ShardedErrorDataTypeHandler configured.");
    }

    @Override
    public String[] getTableNames(Configuration conf) {
        List<String> tables = new ArrayList<>();
        tables.add(ConfigurationHelper.isNull(conf, ERROR_PROP_PREFIX + SHARD_TNAME, String.class));
        String table = conf.get(ERROR_PROP_PREFIX + SHARD_GIDX_TNAME);
        if (table != null) {
            tables.add(table);
        }
        table = conf.get(ERROR_PROP_PREFIX + SHARD_GRIDX_TNAME);
        if (table != null) {
            tables.add(table);
        }
        table = conf.get(ERROR_PROP_PREFIX + METADATA_TABLE_NAME);
        if (table != null) {
            tables.add(table);
        }
        table = conf.get(ERROR_PROP_PREFIX + SHARD_DINDX_NAME);
        if (table != null) {
            tables.add(table);
        }

        return tables.toArray(new String[tables.size()]);
    }

    @Override
    public int[] getTableLoaderPriorities(Configuration conf) {
        int[] priorities = new int[5];
        int i = 0;
        priorities[i++] = ConfigurationHelper.isNull(conf, ERROR_PROP_PREFIX + SHARD_LPRIORITY, Integer.class);
        if (conf.get(ERROR_PROP_PREFIX + SHARD_GIDX_TNAME) != null) {
            priorities[i++] = ConfigurationHelper.isNull(conf, ERROR_PROP_PREFIX + SHARD_GIDX_LPRIORITY, Integer.class);
        }
        if (conf.get(ERROR_PROP_PREFIX + SHARD_GRIDX_TNAME) != null) {
            priorities[i++] = ConfigurationHelper.isNull(conf, ERROR_PROP_PREFIX + SHARD_GRIDX_LPRIORITY, Integer.class);
        }
        if (conf.get(ERROR_PROP_PREFIX + METADATA_TABLE_NAME) != null) {
            priorities[i++] = ConfigurationHelper.isNull(conf, ERROR_PROP_PREFIX + METADATA_TABLE_LOADER_PRIORITY, Integer.class);
        }
        if (conf.get(ERROR_PROP_PREFIX + SHARD_DINDX_NAME) != null) {
            priorities[i++] = ConfigurationHelper.isNull(conf, ERROR_PROP_PREFIX + SHARD_DINDX_LPRIORITY, Integer.class);
        }

        if (i != priorities.length) {
            return Arrays.copyOf(priorities, i);
        } else {
            return priorities;
        }
    }

    @Override
    public long process(KEYIN key, RawRecordContainer record, Multimap<String,NormalizedContentInterface> eventFields,
                    TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context, ContextWriter<KEYOUT,VALUEOUT> contextWriter)
                    throws IOException, InterruptedException {

        // write out the event into a value before we muck with it
        DataOutputBuffer buffer = new DataOutputBuffer();
        record.write(buffer);
        Value value = new Value(buffer.getData(), 0, buffer.getLength());
        buffer.reset();

        // make a copy of the event to avoid side effects
        record = record.copy();

        // set the event date to now to enable keeping track of when this error occurred (determines date for shard)
        record.setDate(System.currentTimeMillis());

        // TODO: May want to check validity of record's security markings here and set defaults if necessary

        // add the error fields to our list of fields
        Multimap<String,NormalizedContentInterface> allFields = HashMultimap.create();
        if (eventFields != null) {
            for (NormalizedContentInterface n : eventFields.values()) {
                /* TODO: May want to check validity of the field's security markings here and set defaults if necessary */

                // if we had an error, then add a field for that
                if (n.getError() != null) {
                    String fieldName = n.getEventFieldName() + '_' + STACK_TRACE_FIELD;
                    getStackTrace(buffer, n.getError());
                    allFields.put(fieldName, new NormalizedFieldAndValue(fieldName, new String(buffer.getData(), 0, buffer.getLength())));
                    allFields.put(ERROR_FIELD, new NormalizedFieldAndValue(ERROR_FIELD, n.getEventFieldName()));
                    buffer.reset();
                }
            }
        }
        // job name
        allFields.put(JOB_NAME_FIELD, new NormalizedFieldAndValue(JOB_NAME_FIELD, context.getJobName()));
        // job id
        allFields.put(JOB_ID_FIELD, new NormalizedFieldAndValue(JOB_ID_FIELD, context.getJobID().getJtIdentifier()));
        // uuids
        if (record.getAltIds() != null) {
            for (String uuid : record.getAltIds()) {
                allFields.put(UUID_FIELD, new NormalizedFieldAndValue(UUID_FIELD, uuid));
            }
        }

        // processed count if any
        if (record.getAuxProperty(PROCESSED_COUNT) != null) {
            allFields.put(PROCESSED_COUNT, new NormalizedFieldAndValue(PROCESSED_COUNT, record.getAuxProperty(PROCESSED_COUNT)));
        }

        // event errors
        for (String error : record.getErrors()) {
            allFields.put(ERROR_FIELD, new NormalizedFieldAndValue(ERROR_FIELD, error));
        }

        // event runtime exception if any
        if (record.getAuxData() instanceof Exception) {
            allFields.put(ERROR_FIELD, new NormalizedFieldAndValue(ERROR_FIELD, RawDataErrorNames.RUNTIME_EXCEPTION));
            getStackTrace(buffer, (Exception) (record.getAuxData()));
            allFields.put(STACK_TRACE_FIELD, new NormalizedFieldAndValue(STACK_TRACE_FIELD, new String(buffer.getData(), 0, buffer.getLength())));
            buffer.reset();
        }

        // normalize the new set of fields.
        allFields = errorHelper.normalizeMap(allFields);

        // add metadata for the new fields
        getMetadata().addEvent(errorHelper, record, allFields);

        // include the original fields
        if (eventFields != null) {
            allFields.putAll(eventFields);
        }

        if (record instanceof Configurable) {
            ((Configurable) record).setConf(conf);
        }

        // now that we have captured the fields, revalidate the event
        try {
            this.errorHelper.getEmbeddedHelper().getPolicyEnforcer().validate(record);
        } catch (Throwable e) {
            log.error("Failed to re-validate", e);
        }

        // ensure we get a uid with a time element
        record.setId(UID.builder().newId(record.getRawData(), new Date(record.getDate())));

        // empty the errors to enable the super.processBulk to process normally.
        record.clearErrors();

        // now get the sharded event keys
        Multimap<BulkIngestKey,Value> shardedKeys = super.processBulk(key, record, allFields, new ContextWrappedStatusReporter(context));
        contextWriter.write(shardedKeys, context);

        // ShardId 'd' DataType\0UID\0Name for document content event using Event.Writable
        String colq = record.getDataType().outputName() + '\0' + record.getId() + '\0' + EVENT_CONTENT_FIELD;
        Key k = createKey(getShardId(record), new Text(ExtendedDataTypeHandler.FULL_CONTENT_COLUMN_FAMILY), new Text(colq), getVisibility(record, null),
                        record.getTimestamp(), this.helper.getDeleteMode());
        BulkIngestKey ebKey = new BulkIngestKey(getShardTableName(), k);
        contextWriter.write(ebKey, value, context);

        return allFields.size();
    }

    /**
     * Get the stack trace for a throwable
     *
     * @param buffer
     *            the writing buffer
     * @param e
     *            the exception throwable
     */
    public static void getStackTrace(DataOutputBuffer buffer, Throwable e) {
        PrintStream stream = new PrintStream(buffer);
        e.printStackTrace(stream); // Prints to DataOutputBuffer
        stream.flush();
    }

    /**
     * A helper routine to determine the visibility for a field.
     *
     * @param event
     *            the event container
     * @param value
     *            the value
     * @return the visibility
     */
    @Override
    public byte[] getVisibility(RawRecordContainer event, NormalizedContentInterface value) {
        byte[] visibility;
        if (value != null && value.getMarkings() != null && !value.getMarkings().isEmpty()) {
            try {
                visibility = flatten(markingFunctions.translateToColumnVisibility(value.getMarkings()));
            } catch (MarkingFunctions.Exception e) {
                log.error("Failed to create visibility from markings, using default", e);
                visibility = defaultVisibility;
            }
        } else if (event.getVisibility() != null) {
            visibility = flatten(event.getVisibility());
        } else {
            visibility = defaultVisibility;
        }
        return visibility;
    }

    @Override
    public Multimap<BulkIngestKey,Value> processBulk(KEYIN key, RawRecordContainer event, Multimap<String,NormalizedContentInterface> eventFields,
                    StatusReporter reporter) {
        throw new UnsupportedOperationException("processBulk is not supported, please use process");
    }

    @Override
    public IngestHelperInterface getHelper(Type datatype) {
        IngestHelperInterface helper = datatype.getIngestHelper(conf);
        this.errorHelper.setDelegateHelper(helper);
        return this.errorHelper;
    }

    @Override
    public void close(TaskAttemptContext context) {
        // does nothing
    }

    public byte[] getDefaultVisibility() {
        return defaultVisibility;
    }

    @Override
    protected boolean hasIndexTerm(String fieldName) {
        // we want field index terms for everything, so return true
        return true;
    }

    @Override
    protected boolean hasReverseIndexTerm(String fieldName) {
        // we want field index terms for everything, so return true
        return true;
    }

}
