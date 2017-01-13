package nsa.datawave.mr.bulk;

import java.io.IOException;
import java.util.Collection;

import nsa.datawave.ingest.mapreduce.job.RFileRecordReader;
import nsa.datawave.mr.bulk.split.TabletSplitSplit;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

public class RangeRecordReader extends RFileRecordReader {
    
    public static final String ITER_CLASSES = "range.record.reader.iterz";
    
    protected Collection<FileSKVIterator> fileIterators = null;
    
    private boolean readFirstKeyValue = false;
    
    protected Key startKey = null;
    
    protected Key endKey = null;
    
    protected RecordIterator splitReference = null;
    
    private static final Logger log = Logger.getLogger(RangeRecordReader.class);
    
    protected static final String PREFIX = BulkInputFormat.class.getSimpleName();
    protected static final String ITERATORS = PREFIX + ".iterators";
    protected static final String ITERATORS_OPTIONS = PREFIX + ".iterators.options";
    protected static final String ITERATORS_DELIM = ",";
    
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        
        splitReference = new RecordIterator((TabletSplitSplit) split, context.getConfiguration());
    }
    
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        // Iterators start out on the first key, whereas record readers are
        // assumed to start on nothing and move to the first key, so we don't
        // want to advance the iterator the first time through.
        if (readFirstKeyValue) {
            splitReference.next();
        }
        readFirstKeyValue = true;
        return splitReference.hasTop();
    }
    
    @Override
    public Key getCurrentKey() throws IOException, InterruptedException {
        return splitReference.getTopKey();
    }
    
    @Override
    public Value getCurrentValue() throws IOException, InterruptedException {
        return splitReference.getTopValue();
    }
    
    @Override
    public void close() throws IOException {
        if (null != splitReference)
            splitReference.close();
    }
    
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return splitReference.getProgress();
    }
    
}
