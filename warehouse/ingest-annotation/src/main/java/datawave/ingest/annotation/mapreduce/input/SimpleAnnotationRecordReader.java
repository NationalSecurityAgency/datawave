package datawave.ingest.annotation.mapreduce.input;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Iterator;

import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.io.input.CountingInputStream;
import org.apache.commons.lang.StringUtils;
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
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.input.reader.AbstractEventRecordReader;

/**
 * Simple annotation record reader that will process concatenated annotation JSON This is similar to JSONRecordReader, but it doesn't try to parse the JSON into
 * fields
 */
public class SimpleAnnotationRecordReader extends AbstractEventRecordReader<BytesWritable> {
    private static final Logger logger = Logger.getLogger(SimpleAnnotationRecordReader.class);

    // RecordReader stuff

    protected CountingInputStream countingInputStream;
    protected final LongWritable currentKey = new LongWritable();
    protected URI fileURI;
    protected long counter = 0;
    protected long start;
    protected long pos;
    protected long end;

    protected Multimap<String,String> currentValue = HashMultimap.create();
    protected Iterator<JsonElement> jsonIterator;
    protected JsonReader reader;
    protected String currentJsonString;

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
            logger.info("Reading Annotation records from " + normURI + " via " + is.getClass().getName());
        }
    }

    protected void setupReader(InputStream is) {
        countingInputStream = new CountingInputStream(is);
        reader = new JsonReader(new InputStreamReader(countingInputStream));
        reader.setLenient(true);
        setupIterator(reader);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
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

            pos = countingInputStream.getByteCount();

            // Save ref to the current json element, to be used when writing the raw data to the record in getEvent
            currentJsonString = jsonElement.toString();
            return true;
        }

        return false;
    }

    protected void setupIterator(JsonReader reader) {
        JsonElement root = JsonParser.parseReader(reader);

        if (root.isJsonArray()) {
            // Currently positioned to read a set of objects
            jsonIterator = root.getAsJsonArray().iterator();
        } else {
            // Currently positioned to read a single object
            jsonIterator = IteratorUtils.singletonIterator(root);
        }
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        currentKey.set(counter);
        return currentKey;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        if (currentJsonString != null) {
            return new BytesWritable(currentJsonString.getBytes());
        } else {
            return null;
        }
    }

    @Override
    public RawRecordContainer getEvent() {
        super.getEvent();

        if (StringUtils.isEmpty(eventDateFieldName)) {
            event.setTimestamp(this.inputDate);
        }

        event.setRawData(currentJsonString.getBytes());

        if (!event.isTimestampSet()) {
            event.setTimestamp(System.currentTimeMillis());
        }

        return event;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }
}
