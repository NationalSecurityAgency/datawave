package nsa.datawave.mr.bulk;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import nsa.datawave.mr.bulk.BulkInputFormat.AccumuloIterator;
import nsa.datawave.mr.bulk.BulkInputFormat.AccumuloIteratorOption;
import nsa.datawave.mr.bulk.split.FileRangeSplit;
import nsa.datawave.mr.bulk.split.RangeSplit;
import nsa.datawave.mr.bulk.split.TabletSplitSplit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.rfile.RFileOperations;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.DeletingIterator;
import org.apache.accumulo.core.iterators.system.MultiIterator;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class RecordIterator extends RangeSplit implements SortedKeyValueIterator<Key,Value> {
    
    protected TabletSplitSplit fileSplit;
    
    protected Deque<Range> rangeQueue;
    
    protected Collection<FileRangeSplit> fileRangeSplits;
    
    protected Collection<FileSKVIterator> fileIterators;
    
    protected SortedKeyValueIterator<Key,Value> globalIter;
    
    protected Boolean isOpen = false;
    
    protected Key lastSeenKey = null;
    
    protected Configuration conf;
    
    private volatile int failureCount = 0;
    
    private final int failureMax;
    
    private final long failureSleep;
    
    protected Range currentRange;
    
    private static final Logger log = Logger.getLogger(RecordIterator.class);
    
    public RecordIterator(final TabletSplitSplit fileSplit, Configuration conf) {
        super();
        
        this.conf = conf;
        
        failureMax = conf.getInt("recorditer.failure.count.max", 5);
        
        failureSleep = conf.getLong("recorditer.failure.sleep.interval", 3000L);
        
        this.fileSplit = fileSplit;
        
        try {
            fileRangeSplits = buildRangeSplits(fileSplit);
            
            initialize(conf, true);
            
            if (rangeQueue.isEmpty()) {
                throw new RuntimeException("Queue of ranges is empty");
            }
            
            seekToNextKey();
            
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
    }
    
    protected Collection<FileRangeSplit> buildRangeSplits(TabletSplitSplit split) throws IOException {
        Collection<FileRangeSplit> splits = Lists.newArrayList();
        for (int i = 0; i < split.getLength(); i++) {
            FileRangeSplit fileSplit = (FileRangeSplit) split.get(i);
            
            splits.add(fileSplit);
            
        }
        
        return splits;
    }
    
    protected synchronized void initialize(Configuration conf, boolean initRanges) throws IOException {
        
        if (isOpen) {
            close();
            isOpen = false;
        }
        
        fileIterators = Lists.newArrayList();
        
        List<SortedKeyValueIterator<Key,Value>> iterators = Lists.newArrayList();
        
        Set<Path> pathSet = Sets.newHashSet();
        
        for (FileRangeSplit split : fileRangeSplits) {
            Range myRange = split.getRange();
            if (initRanges)
                addRange(myRange);
            
            pathSet.add(split.getPath());
            
        }
        
        for (Path path : pathSet) {
            SortedKeyValueIterator<Key,Value> rfileIter = null;
            
            try {
                rfileIter = iterFromFile(path, conf);
            } catch (Exception e) {
                
            }
            
            if (null != rfileIter)
                
                iterators.add(rfileIter);
        }
        
        SortedKeyValueIterator<Key,Value> topIter = new MultiIterator(iterators, true);
        
        boolean applyDeletingIterator = conf.getBoolean("range.record.reader.apply.delete", true);
        if (applyDeletingIterator)
            topIter = new DeletingIterator(topIter, false);
        try {
            globalIter = buildTopIterators(topIter, conf);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new IOException(e);
        }
        
        if (initRanges) {
            rangeQueue = new LinkedList<Range>();
            rangeQueue.addAll(ranges);
        }
    }
    
    /**
     * @param topIter
     * @param clazzes
     * @return
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws IOException
     */
    protected SortedKeyValueIterator<Key,Value> buildTopIterators(SortedKeyValueIterator<Key,Value> topIter, Configuration conf) throws ClassNotFoundException,
                    InstantiationException, IllegalAccessException, IOException {
        
        List<AccumuloIterator> iterators = BulkInputFormat.getIterators(conf);
        List<AccumuloIteratorOption> options = Lists.newArrayList();
        
        Map<String,IteratorSetting> scanIterators = new HashMap<String,IteratorSetting>();
        for (AccumuloIterator iterator : iterators) {
            scanIterators.put(iterator.getIteratorName(), new IteratorSetting(iterator.getPriority(), iterator.getIteratorName(), iterator.getIteratorClass()));
        }
        for (AccumuloIteratorOption option : options) {
            scanIterators.get(option.getIteratorName()).addOption(option.getKey(), option.getValue());
        }
        
        SortedKeyValueIterator<Key,Value> newIter = topIter;
        
        BulkIteratorEnvironment myData = new BulkIteratorEnvironment();
        for (AccumuloIterator iterator : iterators) {
            
            IteratorSetting settings = scanIterators.get(iterator.getIteratorName());
            
            ByteArrayOutputStream byteOutStream = new ByteArrayOutputStream();
            GZIPOutputStream gzipOut = new GZIPOutputStream(byteOutStream);
            DataOutputStream dataOutStream = new DataOutputStream(gzipOut);
            
            conf.write(dataOutStream);
            
            dataOutStream.close();
            Class<? extends SortedKeyValueIterator> iter = Class.forName(settings.getIteratorClass()).asSubclass(SortedKeyValueIterator.class);
            
            settings.addOption("conf", new String(Base64.encodeBase64(byteOutStream.toByteArray())));
            SortedKeyValueIterator<Key,Value> newInstance = iter.newInstance();
            
            newInstance.init(newIter, settings.getOptions(), myData);
            
            newIter = newInstance;
        }
        return newIter;
    }
    
    protected SortedKeyValueIterator<Key,Value> iterFromFile(final Path path, Configuration conf) throws TableNotFoundException, IOException {
        
        final FileOperations ops = RFileOperations.getInstance();
        
        final FileSystem fs = path.getFileSystem(conf);
        
        log.info("opening " + path.toUri());
        
        FileSKVIterator fileIterator = ops.openReader(path.toUri().toString(), true, fs, conf, AccumuloConfiguration.getDefaultConfiguration());
        
        fileIterators.add(fileIterator);
        
        return fileIterator;
    }
    
    protected synchronized void fail(Throwable t) {
        
        if (++failureCount >= failureMax)
            throw new RuntimeException("Failure count of " + failureCount + " + exceeded failureMax. Last known error: " + t);
        
        try {
            Thread.sleep(failureSleep);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        
        final String tableName = BulkInputFormat.getTablename(conf);
        
        final List<Range> rangeList = Lists.newArrayList(ranges.iterator().next());
        
        try {
            
            Collection<InputSplit> splits = MultiRfileInputformat.computeSplitPoints(conf, tableName, rangeList);
            
            Preconditions.checkArgument(splits.size() == 1);
            
            TabletSplitSplit tabletSplit = (TabletSplitSplit) splits.iterator().next();
            
            fileRangeSplits = buildRangeSplits(tabletSplit);
            
            initialize(conf, false);
            
            seekLastSeen();
            
        } catch (TableNotFoundException | AccumuloException | AccumuloSecurityException | IOException | InterruptedException e) {
            fail(e);
        }
        
    }
    
    public synchronized void close() throws IOException {
        if (null != fileIterators) {
            for (FileSKVIterator iter : fileIterators) {
                try {
                    iter.close();
                } catch (Exception e) {
                    log.warn(e);
                }
            }
        }
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        throw new UnsupportedOperationException("Init is not supported in RecordIterator");
        
    }
    
    @Override
    public boolean hasTop() {
        boolean hasTop = false;
        try {
            hasTop = globalIter.hasTop();
        } catch (Exception e) {
            fail(e);
            hasTop = hasTop();
            failureCount = 0;
        }
        return hasTop;
    }
    
    @Override
    public void next() throws IOException {
        try {
            globalIter.next();
        } catch (Exception e) {
            // don't need to call next() here as we'll see
            // or fail, depending on the situation
            fail(e);
        }
        if (!globalIter.hasTop()) {
            seekToNextKey();
        }
    }
    
    /**
     * @throws IOException
     * 
     */
    private void seekLastSeen() throws IOException {
        
        try {
            if (lastSeenKey != null) {
                Range newRange = new Range(lastSeenKey, false, currentRange.getEndKey(), currentRange.isEndKeyInclusive());
                currentRange = newRange;
            }
        } catch (Exception e) {
            log.error(e);
        }
        
        globalIter.seek(currentRange, Collections.<ByteSequence> emptyList(), false);
    }
    
    /**
     * Seek to the next range containing a key.
     */
    private void seekToNextKey() throws IOException {
        try {
            seekRange();
        } catch (Exception e) {
            fail(e);
        }
        while (!globalIter.hasTop() && !rangeQueue.isEmpty()) {
            try {
                seekRange();
            } catch (Exception e) {
                fail(e);
            }
        }
    }
    
    /**
     * @throws IOException
     * 
     */
    private void seekRange() throws IOException {
        
        if (!rangeQueue.isEmpty()) {
            currentRange = rangeQueue.removeFirst();
            if (currentRange != null) {
                // now that we have a completely new range, ensure that a failure scenario (which calls seekLastSeen) does not put
                // us back into the previous range.
                lastSeenKey = null;
                
                globalIter.seek(currentRange, Collections.<ByteSequence> emptyList(), false);
            }
            
        }
    }
    
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        // now that we have a completely new range, ensure that a failure scenario (which calls seekLastSeen) does not put
        // us back into the previous range.
        lastSeenKey = null;
        currentRange = range;
        globalIter.seek(range, columnFamilies, inclusive);
    }
    
    @Override
    public Key getTopKey() {
        
        Key topKey = null;
        try {
            topKey = globalIter.getTopKey();
        } catch (Exception e) {
            fail(e);
            topKey = getTopKey();
            failureCount = 0;
        }
        lastSeenKey = topKey;
        return topKey;
    }
    
    @Override
    public Value getTopValue() {
        Value topValue = null;
        try {
            topValue = globalIter.getTopValue();
        } catch (Exception e) {
            fail(e);
            topValue = getTopValue();
            failureCount = 0;
        }
        return topValue;
    }
    
    private long MAX_COUNT = 1;
    protected long count = 0;
    private float lastProgress = 0.0f;
    // our goal is a precision of 0.1%, so set the precision to half of that given we double MAX_COUNT each time
    private static final float PROGRESS_PRECISION = 0.0005f;
    
    /**
     * Get the progress for the last key returned. This will try to minimize the underlying progress calculations by reusing the last progress until the
     * progress had changed significantly enough. This is done by estimating how many keys to cache the last value. If the progress does not change
     * significantly over a set of keys, then the number of keys for which to cache the value is doubled.
     */
    public float getProgress() {
        count++;
        if (count >= MAX_COUNT) {
            float progress;
            // There is bug in the HeapIterator that will cause a NPE when it is out of iterators instead of simply
            // returning null.
            try {
                progress = getProgress(globalIter.getTopKey());
            } catch (NullPointerException npe) {
                progress = 1.0f;
            }
            if (progress >= lastProgress) {
                if ((progress - lastProgress) < PROGRESS_PRECISION) {
                    MAX_COUNT = MAX_COUNT * 2;
                }
                count = 0;
                lastProgress = progress;
            } else {
                // here we have progress that has gone backwards....keep the last process and try again later
                count = 0;
            }
        }
        return lastProgress;
    }
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new RecordIterator(fileSplit, conf);
    }
    
}
