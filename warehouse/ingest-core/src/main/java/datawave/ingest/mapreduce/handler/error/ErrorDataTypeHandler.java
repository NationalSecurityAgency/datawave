package datawave.ingest.mapreduce.handler.error;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Date;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.log4j.Logger;

import com.google.common.collect.Multimap;

import datawave.data.hash.UID;
import datawave.data.hash.UIDBuilder;
import datawave.ingest.config.IngestConfiguration;
import datawave.ingest.config.IngestConfigurationFactory;
import datawave.ingest.data.RawDataErrorNames;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.ConfigurationHelper;
import datawave.ingest.data.config.MarkingsHelper;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.mapreduce.handler.ExtendedDataTypeHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.writer.ContextWriter;
import datawave.ingest.metadata.RawRecordMetadata;
import datawave.marking.MarkingFunctions;
import datawave.util.TextUtil;
import datawave.util.time.DateHelper;

/**
 * Handler that take events with processing errors or fatal errors and dumps them into a processing error table. This table will be used for subsequent
 * debugging and reprocessing.
 *
 * <p>
 * This class creates the following Mutations or Key/Values: <br>
 * <br>
 * <table border="1">
 * <caption>ErrorDataType</caption>
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
 * <li>The visibility will be set if known. The default is defined by the MarkingsHelper class.</li>
 * <li>The timestamp will be set to the time at which the error occurred.</li>
 * <li>The UID will be set to UID("", rawdata) when not available in the event.</li>
 * </ul>
 *
 *
 *
 * @param <KEYIN>
 *            type of the input key
 */
public class ErrorDataTypeHandler<KEYIN,KEYOUT,VALUEOUT> implements ExtendedDataTypeHandler<KEYIN,KEYOUT,VALUEOUT> {

    private static final Logger log = Logger.getLogger(ErrorDataTypeHandler.class);

    public static final String ERROR_TABLE = "error.table";
    public static final String ERROR_TABLE_NAME = ERROR_TABLE + ".name";
    public static final String ERROR_TABLE_LOADER_PRIORITY = ERROR_TABLE + ".loader.priority";

    public static final String PROCESSED_COUNT = "EDTH.processedCount";

    private byte[] defaultVisibility = null;
    protected MarkingsHelper markingsHelper;
    protected MarkingFunctions markingFunctions;

    private String tableName = null;

    private Configuration conf = null;

    // Initialize a default (hash-based) UID builder
    private UIDBuilder<UID> uidBuilder = UID.builder();

    public static final Text EVENT_COLF = new Text("e");
    public static final Text INFO_COLF = new Text("info");
    public static final Text FIELD_COLF = new Text("f");

    public static final Value NULL_VALUE = new Value(new byte[0]);

    @Override
    public void setup(TaskAttemptContext context) {
        markingFunctions = MarkingFunctions.Factory.createMarkingFunctions();
        IngestConfiguration ingestConfiguration = IngestConfigurationFactory.getIngestConfiguration();
        markingsHelper = ingestConfiguration.getMarkingsHelper(context.getConfiguration(), TypeRegistry.getType(TypeRegistry.ERROR_PREFIX));
        tableName = ConfigurationHelper.isNull(context.getConfiguration(), ERROR_TABLE_NAME, String.class);

        this.conf = context.getConfiguration();

        try {
            Map<String,String> defaultMarkings = markingsHelper.getDefaultMarkings();
            defaultVisibility = flatten(markingFunctions.translateToColumnVisibility(defaultMarkings));
        } catch (MarkingFunctions.Exception e) {
            throw new IllegalArgumentException("Failed to convert default markings to a ColumnVisibility.", e);
        }

        // Initialize a UID builder based on the configuration
        uidBuilder = UID.builder(conf);

        log.info("ErrorDataTypeHandler configured.");
    }

    @Override
    public String[] getTableNames(Configuration conf) {
        return new String[] {ConfigurationHelper.isNull(conf, ERROR_TABLE_NAME, String.class)};
    }

    @Override
    public int[] getTableLoaderPriorities(Configuration conf) {
        return new int[] {ConfigurationHelper.isNull(conf, ERROR_TABLE_LOADER_PRIORITY, Integer.class)};
    }

    @Override
    public long process(KEYIN key, RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields,
                    TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context, ContextWriter<KEYOUT,VALUEOUT> contextWriter)
                    throws IOException, InterruptedException {
        int count = 0;
        Text tname = new Text(tableName);
        long ts = System.currentTimeMillis();

        // generate the row
        String uid = (event.getId() == null ? uidBuilder.newId(event.getRawData(), new Date(event.getDate())) : event.getId()).toString();
        String row = context.getJobName() + '\0' + event.getDataType().typeName() + '\0' + uid;

        // ***** event column *****
        String eventDate = "";
        if (event.isTimestampSet()) {
            eventDate = DateHelper.format(event.getDate());
        }
        Text colq = safeAppend(null, eventDate);
        for (String uuid : event.getAltIds()) {
            safeAppend(colq, uuid);
        }

        DataOutputBuffer buffer = new DataOutputBuffer();
        event.write(buffer);
        Value value = new Value(buffer.getData(), 0, buffer.getLength());
        buffer.reset();

        Key eKey = createKey(row, EVENT_COLF, colq, getVisibility(event, null), ts);
        BulkIngestKey ebKey = new BulkIngestKey(tname, eKey);
        contextWriter.write(ebKey, value, context);
        count++;

        // ***** info columns *****
        String errorDate;
        errorDate = DateHelper.format(ts);
        for (String error : event.getErrors()) {
            colq = safeAppend(null, StringUtils.isEmpty(error) ? StringUtils.EMPTY : error);
            TextUtil.textAppend(colq, errorDate);

            eKey = createKey(row, INFO_COLF, colq, getVisibility(event, null), ts);
            ebKey = new BulkIngestKey(tname, eKey);
            contextWriter.write(ebKey, NULL_VALUE, context);
            count++;
        }
        if (fields != null) {
            for (NormalizedContentInterface n : fields.values()) {
                // noinspection ThrowableResultOfMethodCallIgnored
                if (n.getError() != null) {
                    colq = safeAppend(null, n.getEventFieldName());
                    TextUtil.textAppend(colq, errorDate);
                    getStackTrace(buffer, n.getError());
                    value = new Value(buffer.getData(), 0, buffer.getLength());
                    buffer.reset();

                    eKey = createKey(row, INFO_COLF, colq, getVisibility(event, n), ts);
                    ebKey = new BulkIngestKey(tname, eKey);
                    contextWriter.write(ebKey, value, context);
                    count++;
                }
            }
        }
        if (event.getAuxData() instanceof Exception) {
            colq = new Text(RawDataErrorNames.RUNTIME_EXCEPTION);
            TextUtil.textAppend(colq, errorDate);
            getStackTrace(buffer, (Exception) (event.getAuxData()));
            value = new Value(buffer.getData(), 0, buffer.getLength());
            buffer.reset();

            eKey = createKey(row, INFO_COLF, colq, getVisibility(event, null), ts);
            ebKey = new BulkIngestKey(tname, eKey);
            contextWriter.write(ebKey, value, context);
            count++;
        }

        // ***** field columns *****
        if (fields != null) {
            for (NormalizedContentInterface n : fields.values()) {
                colq = safeAppend(null, n.getEventFieldName());
                Text valueText = safeAppend(null, n.getEventFieldValue());
                safeAppend(valueText, n.getIndexedFieldName());
                safeAppend(valueText, n.getIndexedFieldValue());
                value = new Value(valueText.getBytes(), 0, valueText.getLength());

                eKey = createKey(row, FIELD_COLF, colq, getVisibility(event, n), ts);
                ebKey = new BulkIngestKey(tname, eKey);
                contextWriter.write(ebKey, value, context);
                count++;
            }
        }

        // also create the processingCount field
        if (event.getAuxProperty(PROCESSED_COUNT) != null) {
            colq = safeAppend(null, PROCESSED_COUNT);
            Text valueText = safeAppend(null, event.getAuxProperty(PROCESSED_COUNT));
            safeAppend(valueText, PROCESSED_COUNT);
            safeAppend(valueText, event.getAuxProperty(PROCESSED_COUNT));
            value = new Value(valueText.getBytes(), 0, valueText.getLength());

            eKey = createKey(row, FIELD_COLF, colq, getVisibility(event, null), ts);
            ebKey = new BulkIngestKey(tname, eKey);
            contextWriter.write(ebKey, value, context);
            count++;
        }

        return count;
    }

    private static Text safeAppend(Text text, String value) {
        if (text == null) {
            try {
                return new Text(value == null ? "" : value);
            } catch (Exception e) {
                return new Text();
            }
        } else {
            try {
                TextUtil.textAppend(text, value == null ? "" : value);
                return text;
            } catch (Exception e) {
                // if the textAppend failed (e.g. malformed exception), then at least
                // the null byte will have been appended
                return text;
            }
        }
    }

    public static void getStackTrace(DataOutputBuffer buffer, Throwable e) {
        PrintStream stream = new PrintStream(buffer);
        e.printStackTrace(stream); // Prints to DataOutputBuffer
        stream.flush();
    }

    /**
     * Create Key from input parameters
     *
     * @param row
     *            the row
     * @param colf
     *            the column family
     * @param colq
     *            the column qualifier
     * @param vis
     *            the column visibility
     * @param ts
     *            the timestamp
     * @return Accumulo Key object
     */
    private Key createKey(String row, Text colf, Text colq, byte[] vis, long ts) {
        // Note: we can never reverse ingest from the error table
        return new Key(row.getBytes(), colf.toString().getBytes(), colq.toString().getBytes(), vis, ts, false);
    }

    /**
     * A helper routine to determine the visibility for a field.
     *
     * @param event
     *            the evenht container
     * @param value
     *            the value holding the markings
     * @return the visibility
     */
    protected byte[] getVisibility(RawRecordContainer event, NormalizedContentInterface value) {
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

    /**
     * Create a flattened visibility, using the cache if possible
     *
     * @param vis
     *            the visibility
     * @return the flattened visibility
     */
    protected byte[] flatten(ColumnVisibility vis) {
        return markingFunctions.flatten(vis);
    }

    @Override
    public Multimap<BulkIngestKey,Value> processBulk(KEYIN key, RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields,
                    StatusReporter reporter) {
        throw new UnsupportedOperationException("processBulk is not supported, please use process");
    }

    @Override
    public IngestHelperInterface getHelper(Type datatype) {
        return datatype.getIngestHelper(this.conf);
    }

    @Override
    public void close(TaskAttemptContext context) {
        // does nothing
    }

    public byte[] getDefaultVisibility() {
        return defaultVisibility;
    }

    @Override
    public RawRecordMetadata getMetadata() {
        return null;
    }
}
