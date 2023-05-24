package datawave.query.tables;

import com.google.common.collect.Queues;
import datawave.webservice.query.Query;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Purpose: Range stream scanner that simplifies the logic and removes day ranges produced when thresholds are met.
 *
 * Design: extends RangeStream scanner and overrides the creation of the executor and the scannerInvariant methods.
 */
public class RangeStreamScannerLimitDays extends RangeStreamScanner {
    private static final Logger log = Logger.getLogger(RangeStreamScanner.class);
    
    private Future<RangeStreamScanner> currentFuture =null;

    public RangeStreamScannerLimitDays(String tableName, Set<Authorizations> auths, ResourceQueue delegator, int maxResults, Query settings) {
        super(tableName, auths, delegator, maxResults, settings);
    }

    @Override
    protected String serviceName() {
        String id = "NoQueryId";
        if (null != settings && null != settings.getId()) {
            id = settings.getId().toString();
        }
        return "RangeStreamScannerLimitDays (" + id + ")";
    }


    /**
     * @param tableName
     * @param auths
     * @param delegator
     * @param maxResults
     */
    public RangeStreamScannerLimitDays(String tableName, Set<Authorizations> auths, ResourceQueue delegator, int maxResults, Query settings, SessionOptions options,
                                       Collection<Range> ranges) {
        super(tableName, auths, delegator, maxResults, settings, options, ranges);
    }

    public RangeStreamScannerLimitDays(ScannerSession other) {
        super(other);
    }



    /**
     * Seek the underlying iterator to the specified shard.
     *
     * First ensures the seek shard does not fall within the currentEntry, resultQueue, or currentQueue.
     *
     * The queues are setup not unlike [resultQ.head] - [currentQ.first - currentQ.last] - lastSeenKey
     *
     * @param seekShard
     *            the shard to seek to.
     * @return the shard we seek'd to.
     */
    @Override
    public String seek(String seekShard) {
        //dequeue(true);
        if (currentEntry == null && resultQueue.isEmpty() && finished) {
            if (log.isTraceEnabled()){
                log.trace("Ending early for " + seekShard);
            }
            return null;
        }
        if (log.isTraceEnabled()){

            if (!resultQueue.isEmpty()){
                log.trace("Advancing queue resultqueue " + resultQueue.peek() + " " + seekShard);
            }
            else{
                log.trace("Advancing queue " + currentEntry + " " + seekShard);
            }
        }
        String seekedShard = advanceQueues(seekShard);
        if (log.isTraceEnabled()){
            log.trace("Seek'd shard is " + seekedShard);
        }
        if (seekedShard == null) {

            if (null != currentFuture) {
                currentFuture.cancel(true);
                try {
                    currentFuture.get();
                } catch (InterruptedException | CancellationException e) {
                } catch (ExecutionException e) {
                }
                writeLock.lock();
                writeLock.unlock();
                if (log.isTraceEnabled()){
                    log.trace("Canceling for " + lastRange + " " + currentFuture.isCancelled());
                }
            }
            this.seekShard = seekShard;
            this.seeking = true;

            // Clear queues before calling findTop().
            this.currentEntry = null;
            if (log.isTraceEnabled()){
                log.trace("Clearing resultQueue " + seekShard + " " +  (resultQueue.size() > 0 ? resultQueue.peek().getKey() : " no resultqueue"));
            }
            this.resultQueue.clear();
            this.currentQueue.clear();

            // Call to hasNext() with empty queues and a null currentEntry triggers a new run of the iterator.
            if (hasNext()) {
                // Check to see if the shard exists within the queues.

                if (log.isTraceEnabled()){
                    log.trace("Continuing after calling hasnext on new range ");
                }
                seekedShard = advanceQueues(seekShard);
            } else {
                if (log.isTraceEnabled()){
                    log.trace("Returning null on seek " + seekedShard + " " + seekShard);
                }
                return null;
            }
        }
        if (log.isTraceEnabled()){

            log.trace("sought " + seekedShard + " on " + seekShard);

        }
        return seekedShard;
    }

    @Override
    protected void submitTask() {
        // wait on results. submit the task if we can

        if (log.isTraceEnabled())
            log.trace("Submitting tasks for" + currentRange);
        currentFuture = myExecutor.submit(this);
        while (resultQueue.isEmpty() && !currentFuture.isDone() && !currentFuture.isCancelled()) {
            try {
                currentFuture.get(100, TimeUnit.NANOSECONDS);
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            } catch (TimeoutException e) {
                continue;
            } catch (CancellationException e){
                break;
            }
        }
        if (log.isTraceEnabled())
            log.trace("Tasks are submitted");
    }

    @Override
    protected int scannerInvariant(final Iterator<Map.Entry<Key,Value>> iter) {
        PeekingIterator<Map.Entry<Key,Value>> kvIter = new PeekingIterator<>(iter);

        int retrievalCount = 0;

        Map.Entry<Key,Value> myEntry;

        if (null != prevDay) {
            try {
                if (log.isTraceEnabled())
                    log.trace("Attempting to insert " + prevDay);
                if (!resultQueue.offer(prevDay, 1, TimeUnit.SECONDS)) {
                    return 0;
                }
                prevDay = null;
            } catch (InterruptedException e) {
                return 0;
            }
        }

        writeLock.lock();
        try {

            while (kvIter.hasNext() && !Thread.currentThread().isInterrupted()) {
                Map.Entry<Key,Value> currentKeyValue = kvIter.peek();

                // become a pass-through if we've seen an unexpected key.
                if (seenUnexpectedKey) {
                    if (log.isTraceEnabled()) {
                        log.trace("Breaking because we've seen an unexpected key");
                    }
                    resultQueue.offer(trimTrailingUnderscore(currentKeyValue));

                    break;
                }


                resultQueue.offer(trimTrailingUnderscore(currentKeyValue));

                lastSeenKey = kvIter.next().getKey();
            }
            retrievalCount += dequeue();

        } finally {
            writeLock.unlock();
        }
        return retrievalCount;
    }

    @Override
    protected int dequeue(boolean forceAll) {
        int count = 0;

        Queue<Map.Entry<Key,Value>> kvIter = Queues.newArrayDeque(currentQueue);
        currentQueue.clear();
        boolean result = true;
        for (Map.Entry<Key,Value> top : kvIter) {

            if (result) {
                do {
                    result = resultQueue.offer(top);

                    if (!result) {
                        if (forceAll)
                            continue;
                    }

                    break;
                } while (!Thread.currentThread().isInterrupted() && !finished && forceAll);
            }

            if (!result && !(!finished && forceAll)) {
                currentQueue.add(top);
            }
            count++;
        }

        if (log.isTraceEnabled()) {
            log.trace("we have " + currentQueue.size() + " " + kvIter.size());
        }

        return count;
    }

    @Override
    protected void flush() {
    }

    @Override
    protected boolean flushNeeded() {
        readLock.lock();
        try {
            return false;
        }finally{
            readLock.unlock();
        }
    }



    @Override
    protected void shutDown() throws Exception {
        super.shutDown();
    }
}
