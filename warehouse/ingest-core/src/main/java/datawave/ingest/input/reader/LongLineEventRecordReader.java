package datawave.ingest.input.reader;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import datawave.ingest.util.io.GzipDetectionUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * A copy of {@link org.apache.hadoop.mapred.LineRecordReader} which does not discard lines longer than "mapred.linerecordreader.maxlength". Instead, it returns
 * them, leaving it to the mapper to decide what to do with it. It also does not treat '\r' (CR) characters as new lines -- it uses {@link LfLineReader} instead
 * of {@link org.apache.hadoop.util.LineReader} to read lines. It also does not keep the newline if "longline.newline.included" is set to true.
 */
public class LongLineEventRecordReader extends AbstractEventRecordReader<Text> implements LineReader {
    
    protected long end = Long.MAX_VALUE;
    protected LfLineReader in;
    protected LongWritable key = new LongWritable();
    protected int maxLineLength;
    protected boolean newLineIncluded = false; // indicates if newline is included
    protected long pos = 0;
    protected long start = 0;
    protected Text value = null;
    
    @Override
    public synchronized void close() throws IOException {
        if (in != null) {
            in.close();
        }
        super.close();
    }
    
    /**
     * Super class returns the position in bytes in the file as the key. This returns the record number.
     */
    @Override
    public LongWritable getCurrentKey() {
        return key;
    }
    
    public void setCurrentKey(LongWritable newKey) {
        key = newKey;
    }
    
    @Override
    public Text getCurrentValue() {
        return value;
    }
    
    public void setCurrentValue(Text newValue) {
        value = newValue;
    }
    
    /**
     * Get the progress within the split
     */
    @Override
    public float getProgress() {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }
    
    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        super.initialize(genericSplit, context);
        
        initializeLineReader(genericSplit, context);
    }
    
    public void setCompressionCodecFactory(CompressionCodecFactory codecFactory) {
        compressionCodecs = codecFactory;
    }
    
    /**
     * @param genericSplit
     *            the split to examine
     * @param context
     *            the context containing the configuration
     * @throws IOException
     *             if there is an issue reading the file
     */
    public void initializeLineReader(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        FileSplit split = (FileSplit) genericSplit;
        Configuration job = context.getConfiguration();
        
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();
        final CompressionCodec codec = compressionCodecs.getCodec(file);
        
        // open the file and seek to the start of the split
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(split.getPath());
        boolean skipFirstLine = false;
        if (codec != null) {
            in = new LfLineReader(codec.createInputStream(fileIn), job);
            in.setNewLineIncluded(newLineIncluded);
            end = Long.MAX_VALUE;
        } else {
            if (start != 0) {
                skipFirstLine = true;
                --start;
                fileIn.seek(start);
            }
            
            // Hadoop CodecFactory only checks the file suffix, let's double check for gzip since some data producers
            // may not append .gz to their files.
            InputStream iStream = GzipDetectionUtil.decompressTream(fileIn);
            Class streamClass = iStream.getClass();
            if (GZIPInputStream.class == streamClass) {
                end = Long.MAX_VALUE;
            }
            
            in = new LfLineReader(iStream, job);
            in.setNewLineIncluded(newLineIncluded);
        }
        if (skipFirstLine) { // skip first line and re-establish "start".
            start += in.readLine(new Text(), 0, (int) Math.min(Integer.MAX_VALUE, end - start));
        }
        this.pos = start;
    }
    
    @Override
    public void initializeEvent(Configuration conf) throws IOException {
        super.initializeEvent(conf);
        initializeMaxLineLength(conf);
        initializeNewLineIncluded(conf);
    }
    
    /**
     * @param conf
     *            Configuration to update
     */
    public void initializeNewLineIncluded(Configuration conf) {
        this.newLineIncluded = conf.getBoolean(LineReader.Properties.LONGLINE_NEWLINE_INCLUDED, false);
    }
    
    /**
     * @param conf
     *            Configuration to update
     */
    public void initializeMaxLineLength(Configuration conf) {
        this.maxLineLength = conf.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
    }
    
    @Override
    public boolean nextKeyValue() throws IOException {
        if (key == null) {
            key = new LongWritable();
        }
        key.set(pos);
        if (value == null) {
            value = new Text();
        }
        int newSize = 0;
        if (pos < end) {
            newSize = in.readLine(value, maxLineLength, Math.max((int) Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));
            if (newSize != 0) {
                pos += newSize;
            }
        }
        if (newSize == 0) {
            key = null;
            value = null;
            return false;
        } else {
            return true;
        }
    }
    
    public void setLfLineReader(LfLineReader lfLineReader) {
        in = lfLineReader;
    }
    
    public LfLineReader getLfLineReader() {
        return in;
    }
    
    public boolean isNewLineIncluded() {
        return newLineIncluded;
    }
    
    public long getStart() {
        return start;
    }
    
    public void setStart(long start) {
        this.start = start;
    }
    
    public long getPos() {
        return pos;
    }
    
    public void setPos(long newPos) {
        pos = newPos;
    }
    
    public void setNewLineIncluded(boolean newLineIncluded) {
        this.newLineIncluded = newLineIncluded;
    }
    
    public int getMaxLineLength() {
        return maxLineLength;
    }
    
    public Text getValue() {
        return value;
    }
    
    public LongWritable getKey() {
        return key;
    }
    
    public long getEnd() {
        return end;
    }
    
    public void setEnd(long end) {
        this.end = end;
    }
}
