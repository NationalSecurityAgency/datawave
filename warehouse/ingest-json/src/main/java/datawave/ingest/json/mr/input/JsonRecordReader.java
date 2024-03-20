package datawave.ingest.json.mr.input;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.io.input.CountingInputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import datawave.data.hash.UID;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.ingest.input.reader.AbstractEventRecordReader;
import datawave.ingest.json.config.helper.JsonDataTypeHelper;
import datawave.ingest.json.config.helper.JsonIngestFlattener;
import datawave.ingest.json.util.JsonObjectFlattener;

/**
 * <p>
 * Simple Json RecordReader that provides parsing of arbitrary Json objects in order to extract desired metadata.
 *
 * <p>
 * Various flattening modes may be configured via the internal {@link JsonDataTypeHelper} instance.
 *
 * <p>
 * For example... <blockquote> {@link JsonIngestFlattener.FlattenMode#SIMPLE}<br>
 * {@link JsonIngestFlattener.FlattenMode#NORMAL}<br>
 * {@link JsonIngestFlattener.FlattenMode#GROUPED}<br>
 * </blockquote>
 *
 * <p>
 * For custom parsing requirements, extend this class and override the 'parseCurrentValue' method to suit your needs.
 */
public class JsonRecordReader extends AbstractEventRecordReader<BytesWritable> {

    private static final Logger logger = Logger.getLogger(JsonRecordReader.class);

    // RecordReader stuff

    protected CountingInputStream countingInputStream;
    protected final LongWritable currentKey = new LongWritable();
    protected URI fileURI;
    protected long counter = 0;
    protected long start;
    protected long pos;
    protected long end;

    // Json parser-related stuff

    protected Multimap<String,String> currentValue = HashMultimap.create();
    protected Iterator<JsonElement> jsonIterator;
    protected JsonReader reader;
    protected JsonElement currentJsonObj;
    protected boolean parseHeaderOnly = true;
    protected JsonDataTypeHelper jsonHelper = null;
    protected JsonObjectFlattener jsonFlattener = null;

    @Override
    public synchronized void close() throws IOException {
        reader.close();
        countingInputStream.close();
    }

    @Override
    public LongWritable getCurrentKey() {
        currentKey.set(counter);
        return currentKey;
    }

    @Override
    public BytesWritable getCurrentValue() {
        if (currentJsonObj != null) {
            return new BytesWritable(currentJsonObj.toString().getBytes());
        } else {
            return null;
        }
    }

    public Multimap<String,String> getCurrentFields() {
        return currentValue;
    }

    @Override
    public float getProgress() {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {

        super.initialize(split, context);

        if (!(split instanceof FileSplit)) {
            throw new IOException("Cannot handle split type " + split.getClass().getName());
        }

        FileSplit fsplit = (FileSplit) split;
        Path file = fsplit.getPath();
        rawFileName = file.getName();
        fileURI = file.toUri();
        FileSystem fs = file.getFileSystem(context.getConfiguration());
        InputStream is = fs.open(file);
        start = fsplit.getStart();
        end = start + fsplit.getLength();
        pos = start;

        String normURI = fileURI.getScheme() + "://" + fileURI.getPath();

        setupReader(is);

        if (logger.isInfoEnabled()) {
            logger.info("Reading Json records from " + normURI + " via " + is.getClass().getName());
        }

        jsonHelper = (JsonDataTypeHelper) createHelper(context.getConfiguration());
        this.parseHeaderOnly = !jsonHelper.processExtraFields();
        jsonFlattener = jsonHelper.newFlattener();

        if (logger.isInfoEnabled()) {
            logger.info("Json flattener mode: " + jsonFlattener.getFlattenMode().name());
        }
    }

    protected void setupReader(InputStream is) {
        countingInputStream = new CountingInputStream(is);
        reader = new JsonReader(new InputStreamReader(countingInputStream));
        reader.setLenient(true);
        setupIterator(reader);
    }

    protected void setupIterator(JsonReader reader) {
        JsonParser parser = new JsonParser();
        JsonElement root = parser.parse(reader);

        if (root.isJsonArray()) {
            // Currently positioned to read a set of objects
            jsonIterator = root.getAsJsonArray().iterator();
        } else {
            // Currently positioned to read a single object
            jsonIterator = IteratorUtils.singletonIterator(root);
        }
    }

    protected void parseCurrentValue(JsonObject jsonObject) {
        jsonFlattener.flatten(jsonObject, currentValue);
    }

    @Override
    public boolean nextKeyValue() throws IOException {

        event.clear();
        currentKey.set(pos);
        currentValue.clear();
        counter++;

        if (!jsonIterator.hasNext()) {
            /*
             * Note that for streaming purposes we support files containing multiple distinct json objects concatenated together, where each object will
             * represent a distinct event/document in our shard table. For example, the file might look like the following...
             *
             * { "doc1": ... }{ "doc2": ... }...{ "docN": ... }
             *
             * As a whole, this would represent an invalid json document, but it is useful for streaming large numbers of objects in batch. Therefore, we simply
             * check here to see if the reader has more objects to read, and if so we keep going
             */
            if (reader.peek() == JsonToken.END_DOCUMENT) {
                return false;
            }
            setupIterator(reader);
        }

        if (jsonIterator.hasNext()) {
            JsonElement jsonElement = jsonIterator.next();

            parseCurrentValue(jsonElement.getAsJsonObject());
            pos = countingInputStream.getByteCount();

            // Save ref to the current json element, to be used when writing the raw data to the record in getEvent
            currentJsonObj = jsonElement;
            return true;
        }

        return false;
    }

    @Override
    public RawRecordContainer getEvent() {
        super.getEvent();

        if (StringUtils.isEmpty(eventDateFieldName)) {
            event.setDate(this.inputDate);
        }

        for (Map.Entry<String,String> entry : currentValue.entries()) {
            String fieldName = entry.getKey();
            String fieldValue = entry.getValue();
            if (fieldValue != null) {
                checkField(fieldName, fieldValue);
            }
        }

        decorateEvent();

        event.setRawData(currentJsonObj.toString().getBytes());

        if (!event.isTimestampSet()) {
            event.setDate(System.currentTimeMillis());
        }

        UID newUID = uidOverride(event);
        if (null != newUID) {
            event.setId(newUID);
        } else {
            event.generateId(null);
        }

        checkSecurityMarkings();
        enforcePolicy(event);

        return event;
    }

    /**
     * If needed, modify/update event after parsing has occurred but before policy enforcement and UID assignment
     */
    protected void decorateEvent() {
        // no-op by default
    }

    /**
     * Make sure that security markings are set on the event prior to its return in {@link #getEvent()}
     */
    protected void checkSecurityMarkings() {
        if (null == event.getSecurityMarkings() || event.getSecurityMarkings().isEmpty()) {
            event.setSecurityMarkings(this.markingsHelper.getDefaultMarkings());
        }
    }

    @Override
    protected DataTypeHelper createHelper(final Configuration conf) {
        JsonDataTypeHelper helper = new JsonDataTypeHelper();
        helper.setup(conf);
        return helper;
    }

    /**
     * If the specified field name was configured for this data type to be the {@link JsonDataTypeHelper.Properties#COLUMN_VISIBILITY_FIELD}, then this method
     * will set the column visibility on {@link #event} to the specified value
     *
     * @param fieldName
     *            name of the field to check
     * @param fieldValue
     *            value of the specified field
     */
    @Override
    protected void checkField(final String fieldName, final String fieldValue) {
        super.checkField(fieldName, fieldValue);
        if (fieldName.equals(jsonHelper.getColumnVisibilityField())) {
            event.setVisibility(new ColumnVisibility(fieldValue));
        }
    }

    /**
     * <p>
     * Defaults to true, but may be overridden via the configured {@link JsonDataTypeHelper}, i.e., via the negation of
     * {@link JsonDataTypeHelper#processExtraFields()}. Note that the helper instance in this case is acquired during
     * {@link #initialize(InputSplit, TaskAttemptContext)} via the {@link #createHelper(Configuration)} factory method.
     *
     * <p>
     * That is, here in the record reader, we likely do not need to parse any fields beyond our defined "header", because typically the header fields should
     * convey all the metadata that is required, i.e., metadata fields necessary to create a valid RawRecordContainer instance for the EventMapper, such as
     * event date, column viz, etc. If required metadata happens to be conveyed through fields outside the header for whatever reason, then the configured
     * {@link JsonDataTypeHelper} must announce that fact, as described above.
     *
     * @return true, indicating that only the header fields need to be parsed. Otherwise false
     */
    public boolean isParseHeaderOnly() {
        return parseHeaderOnly;
    }
}
