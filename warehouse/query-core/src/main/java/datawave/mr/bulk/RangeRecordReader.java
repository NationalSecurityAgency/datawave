package datawave.mr.bulk;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import datawave.ingest.mapreduce.job.RFileRecordReader;
import datawave.mr.bulk.split.FileRangeSplit;
import datawave.mr.bulk.split.TabletSplitSplit;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

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
        
        final long failureSleep = context.getConfiguration().getLong(RecordIterator.RECORDITER_FAILURE_SLEEP_INTERVAL, RecordIterator.DEFAULT_FAILURE_SLEEP);
        int retries = 0;
        int maxRetries = context.getConfiguration().getInt(RecordIterator.RECORDITER_FAILURE_COUNT_MAX, RecordIterator.FAILURE_MAX_DEFAULT);
        TabletSplitSplit tabletSplit = (TabletSplitSplit) split;
        do {
            try {
                splitReference = new RecordIterator(tabletSplit, context.getConfiguration());
            } catch (RuntimeException e) {
                
                log.info(e);
                
                splitReference = null;
                // an exception has occurred that won't allow us to open the files. perhaps one was moved
                // immediately upon opening the tablet.
                if (++retries > maxRetries) {
                    log.info("Giving up because" + retries + " >= " + maxRetries);
                    throw e;
                } else if (log.isTraceEnabled()) {
                    log.trace("Retrying " + split);
                }
                MultiRfileInputformat.clearMetadataCache();
                
                Thread.sleep(failureSleep);
                
                /**
                 * Proper initialization requires that we set the tabletsplit table. This is generally done for us, but in the event that these are built
                 * manually we will throw an exception since we can't re-compute the split points without knowing the table.
                 */
                if (!tabletSplit.getTable().equals(TabletSplitSplit.TABLE_NOT_SET)) {
                    try {
                        
                        /**
                         * Compute the list of ranges again. We know that since we failed on initialization we can simply use all ranges. Internally
                         * RecordIterator will have its own failure mechanism restarting from the failure point.
                         */
                        List<Range> ranges = Lists.newArrayList();
                        for (int i = 0; i < tabletSplit.getLength(); i++) {
                            FileRangeSplit rfileSplit = (FileRangeSplit) (tabletSplit.get(i));
                            ranges.addAll(rfileSplit.getRanges());
                        }
                        
                        Collection<InputSplit> splits = MultiRfileInputformat.computeSplitPoints(context.getConfiguration(), tabletSplit.getTable(), ranges);
                        
                        /**
                         * Ensure that we only have one split, otherwise splits were created underneath this table.
                         */
                        Preconditions.checkArgument(splits.size() == 1);
                        
                        tabletSplit = (TabletSplitSplit) splits.iterator().next();
                        
                    } catch (TableNotFoundException | AccumuloException | AccumuloSecurityException e1) {
                        throw new RuntimeException(e1);
                    }
                } else {
                    throw new RuntimeException("Cannot recompute splits points because we are not properly initialized");
                }
                
            }
        } while (splitReference == null);
        
    }
    
    /**
     * merge tablet splits since we will receive one to many from MultiRfileInputformat.computeSplitPoints( when re-computing split points.
     * 
     * @param newSplits
     *            new list of splits
     * @param table
     *            table that we're accessing.
     * @return
     * @throws IOException
     *             cannot build TabletSplitSplit
     * @throws InterruptedException
     *             cannot build TabletSplitSplit
     */
    private TabletSplitSplit mergeTabletSplits(List<InputSplit> newSplits, String table) throws IOException, InterruptedException {
        int size = 0;
        
        for (InputSplit split : newSplits) {
            size += ((TabletSplitSplit) split).getLength();
        }
        TabletSplitSplit newSplit = new TabletSplitSplit(size);
        newSplit.setTable(table);
        for (InputSplit split : newSplits) {
            newSplit.add(((TabletSplitSplit) split));
        }
        return newSplit;
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
