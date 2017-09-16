package datawave.query.planner;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import datawave.query.CloseableIterable;
import datawave.query.iterator.QueryIterator;
import datawave.query.iterator.QueryOptions;
import datawave.query.tld.TLDQueryIterator;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

import datawave.core.iterators.ColumnQualifierRangeIterator;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.QueryData;

/**
 * 
 */
public class ThreadedRangeBundlerIterator implements Iterator<QueryData>, Closeable {
    private static final Logger log = ThreadConfigurableLogger.getLogger(ThreadedRangeBundlerIterator.class);
    
    private final long maxWaitValue;
    private final TimeUnit maxWaitUnit;
    private final QueryData original;
    private final long maxRanges;
    private final Query settings;
    
    private final ArrayBlockingQueue<QueryPlan> rangeQueue;
    
    private QueryData next = null;
    private Object producerLock = new Object();
    
    private RangeConsumer rangeConsumer;
    private Thread rangeConsumerThread;
    
    private int producerCount = 0;
    private long rangesProcessed = 0;
    private int docsToCombine = -1;
    
    private final Text holder = new Text();
    private long eventRanges = 0, shardDatatypeRanges = 0, shardRanges = 0, dayRanges = 0;
    
    private ASTJexlScript queryTree;
    
    private boolean docSpecificLimitOverride = false;
    
    protected boolean isTld = false;
    
    public ThreadedRangeBundlerIterator(QueryData original, ASTJexlScript queryTree, CloseableIterable<QueryPlan> ranges, final long maxRanges,
                    long maxWaitValue, TimeUnit maxWaitUnit, Query settings, boolean docSpecificLimitOverride) {
        this(original, queryTree, ranges, maxRanges, -1, maxWaitValue, maxWaitUnit, settings, docSpecificLimitOverride);
    }
    
    /**
     * @param original
     * @param queryTree
     * @param ranges
     * @param maxRanges
     * @param docToCombine
     * @param maxWaitValue
     * @param maxWaitUnit
     * @param settings
     * @param docSpecificLimitOverride
     */
    public ThreadedRangeBundlerIterator(QueryData original, ASTJexlScript queryTree, CloseableIterable<QueryPlan> ranges, final long maxRanges,
                    final int docToCombine, long maxWaitValue, TimeUnit maxWaitUnit, Query settings, boolean docSpecificLimitOverride) {
        
        this.original = original;
        
        if (isTld(this.original.getSettings())) {
            isTld = true;
        }
        this.maxRanges = maxRanges;
        this.settings = settings;
        this.queryTree = queryTree;
        
        this.docsToCombine = docToCombine;
        
        this.maxWaitValue = maxWaitValue;
        this.maxWaitUnit = maxWaitUnit;
        
        this.docSpecificLimitOverride = docSpecificLimitOverride;
        
        // TODO Make this smarter based on num-concurrent queries, 'max' size of
        // a range, etc
        rangeQueue = new ArrayBlockingQueue<QueryPlan>((int) maxRanges > 0 ? (int) maxRanges : 1000);
        
        rangeConsumer = new RangeConsumer(ranges);
        rangeConsumerThread = new Thread(rangeConsumer);
        if (settings.getId() != null)
            rangeConsumerThread.setName("RangeBundlerIterator for " + settings.getId().toString());
        else
            rangeConsumerThread.setName("RangeBundlerIterator for ");
        rangeConsumerThread.setUncaughtExceptionHandler(settings.getUncaughtExceptionHandler());
        
        rangeConsumerThread.start();
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#hasNext()
     */
    @Override
    public boolean hasNext() {
        synchronized (producerLock) {
            if (null != next) {
                return true;
            }
            
            try {
                while ((!this.rangeQueue.isEmpty() || (!this.rangeConsumer.isStopped() || this.rangeConsumerThread.isAlive()))) {
                    if (log.isTraceEnabled())
                        log.trace(" has next " + rangeQueue.isEmpty() + " is stopped? " + rangeConsumer.isStopped() + " isalive "
                                        + rangeConsumerThread.isAlive());
                    QueryPlan plan = this.rangeQueue.poll(this.maxWaitValue, this.maxWaitUnit);
                    if (null == plan) {
                        if (!rangeConsumer.isStopped()) {
                            if (log.isTraceEnabled())
                                log.trace("Continuing because should probably wait");
                            continue;
                        }
                        if (log.isTraceEnabled())
                            log.trace("null from rangeQueue");
                        next = null;
                        break;
                    }
                    
                    // if the generated query is larger, use the original
                    if (null != queryTree && (plan.getQueryString().length() > original.getQuery().length())) {
                        plan.setQuery(original.getQuery(), queryTree);
                    }
                    if (log.isTraceEnabled())
                        log.trace("size of ranges is " + plan.getRanges());
                    // if the generated query is larger, use the original
                    
                    boolean docSpecific = true;
                    Text row = null;
                    for (Range r : plan.getRanges()) {
                        if (log.isTraceEnabled())
                            log.trace("Adding range" + r);
                        if (null == r) {
                            
                            if (!this.rangeConsumer.isStopped()) {
                                log.warn("Consumer is still running, but could not fetch a range in " + this.maxWaitValue + this.maxWaitUnit);
                            }
                            
                        } else {
                            Key sk = r.getStartKey();
                            sk.getColumnFamily(holder);
                            if (holder.getLength() > 0) {
                                if (holder.find("\0") > 0) {
                                    eventRanges++;
                                    row = sk.getRow();
                                    docSpecific &= true;
                                } else {
                                    docSpecific &= false;
                                    shardDatatypeRanges++;
                                }
                            } else {
                                docSpecific &= false;
                                sk.getRow(holder);
                                if (holder.find("_") > 0) {
                                    shardRanges++;
                                } else {
                                    dayRanges++;
                                }
                            }
                            
                        }
                    }
                    
                    if (docsToCombine > 1 && docSpecific) {
                        List<QueryPlan> plansToCombine = Lists.newArrayList();
                        plansToCombine.add(plan);
                        boolean matchedDocumentRange = true;
                        // wait 1 ms before we pull the rest
                        LockSupport.parkNanos(1000);
                        if (Thread.interrupted()) {
                            throw new InterruptedException("Interrupted while parking");
                        }
                        do {
                            /**
                             * We use an arbitrarily small poll time so that we don't cause pull ids too quickly if they aren't there.
                             */
                            QueryPlan nextPlan = this.rangeQueue.peek();
                            // attempt to merge upcoming ranges
                            if (null != nextPlan) {
                                for (Range r : nextPlan.getRanges()) {
                                    if (log.isTraceEnabled())
                                        log.trace("Adding range" + r);
                                    if (null == r) {
                                        
                                        if (!this.rangeConsumer.isStopped()) {
                                            log.warn("Consumer is still running, but could not fetch a range in " + this.maxWaitValue + this.maxWaitUnit);
                                        }
                                        
                                    } else {
                                        Key sk = r.getStartKey();
                                        sk.getColumnFamily(holder);
                                        if (holder.getLength() > 0) {
                                            if (holder.find("\0") > 0) {
                                                // ensure we are within the same
                                                // row
                                                if (row != null && sk.getRow().equals(row)) {
                                                    matchedDocumentRange &= true;
                                                } else
                                                    matchedDocumentRange &= false;
                                            } else {
                                                matchedDocumentRange &= false;
                                                break;
                                            }
                                        } else {
                                            matchedDocumentRange &= false;
                                            break;
                                        }
                                    }
                                }
                                if (!matchedDocumentRange) {
                                    if (plansToCombine.size() > 1) {
                                        plan = combineDocSpecificPlans(plansToCombine);
                                    }
                                    plansToCombine = null;
                                    break;
                                    // combine doc specific
                                } else {
                                    plansToCombine.add(nextPlan);
                                    // pop the previous new plan off
                                    this.rangeQueue.poll();
                                }
                                
                            } else {
                                
                                if (plansToCombine.size() > 1) {
                                    plan = combineDocSpecificPlans(plansToCombine);
                                }
                                plansToCombine = null;
                                matchedDocumentRange = false;
                                break;
                            }
                        } while (matchedDocumentRange == true && plansToCombine.size() < docsToCombine);
                        
                        if (null != plansToCombine && plansToCombine.size() > 1) {
                            plan = combineDocSpecificPlans(plansToCombine);
                        }
                    }
                    
                    next = createNewQueryData(plan);
                    if (log.isTraceEnabled()) {
                        if (null != next) {
                            log.trace("Built QueryData with " + next.getRanges().size() + " range(s)");
                            log.trace("Built QueryData " + next.getQuery());
                        } else
                            log.trace("Invalid query data object built");
                    }
                    break;
                }
            } catch (Exception e) {
                log.error("Exception in ThreadedRangeBundlerIterator", e);
                throw new RuntimeException(e);
            }
            
            if (log.isTraceEnabled())
                log.trace(" range queue " + rangeQueue.isEmpty() + " is stopped? " + rangeConsumer.isStopped() + " isalive " + rangeConsumerThread.isAlive());
            
            // next is still null due to the check at the beginning of
            // the synchronized block
            
            return null != next;
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#next()
     */
    @Override
    public QueryData next() {
        QueryData current = null;
        
        synchronized (producerLock) {
            if (hasNext()) {
                current = this.next;
                this.next = null;
                this.rangesProcessed += current.getRanges().size();
                this.producerCount++;
                
                if (log.isTraceEnabled() && 10 % this.producerCount == 0) {
                    log.trace("Produced " + this.producerCount + " QueryData objects with " + this.rangesProcessed + " total range(s)");
                }
            }
        }
        
        return current;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#remove()
     */
    @Override
    public void remove() {
        throw new UnsupportedOperationException(this.getClass().getName() + " does not implement Iterator#remove().");
    }
    
    /**
     * It is expected that the ranges supplied by plans are in sorted order. In the ThreadedRAngeBundlerIterator, this will always be the case
     * 
     * @param plans
     *            incoming list of plans to combine.
     * @return combined plans
     * @throws IOException
     *             Exception produced by the encode range function
     */
    private QueryPlan combineDocSpecificPlans(List<QueryPlan> plans) throws IOException {
        int count = 0;
        QueryPlan firstPlan = null;
        IteratorSetting firstQueryIterator = null;
        if (isTld) {
            firstQueryIterator = new IteratorSetting(1, TLDQueryIterator.class);
        } else {
            firstQueryIterator = new IteratorSetting(1, QueryIterator.class);
        }
        for (QueryPlan plan : plans) {
            if (null == firstPlan) {
                firstPlan = plan;
                
                if (null == firstQueryIterator) {
                    throw new RuntimeException("Expect query iterator");
                }
                
                String prevQuery = plan.getQueryString();
                for (Range range : plan.getRanges()) {
                    
                    firstQueryIterator.addOption(QueryOptions.BATCHED_QUERY_PREFIX + count, prevQuery);
                    firstQueryIterator.addOption(QueryOptions.BATCHED_QUERY_RANGE_PREFIX + count, ColumnQualifierRangeIterator.encodeRange(range));
                    count++;
                }
            } else {
                String query = plan.getQueryString();
                for (Range range : plan.getRanges()) {
                    firstQueryIterator.addOption(QueryOptions.BATCHED_QUERY_PREFIX + count, query);
                    firstQueryIterator.addOption(QueryOptions.BATCHED_QUERY_RANGE_PREFIX + count, ColumnQualifierRangeIterator.encodeRange(range));
                    count++;
                }
                
            }
        }
        if (null != firstPlan) {
            Text row = firstPlan.getRanges().iterator().next().getStartKey().getRow();
            firstPlan.setRanges(Collections.singleton(new Range(row)));
        }
        firstQueryIterator.addOption(QueryOptions.BATCHED_QUERY, Integer.valueOf(count).toString());
        firstPlan.getSettings().add(firstQueryIterator);
        return firstPlan;
        
    }
    
    /**
     * Determines if we are running a tld query
     * 
     * @param settings
     *            original query settings.
     * @return
     */
    private boolean isTld(List<IteratorSetting> settings) {
        for (IteratorSetting setting : this.original.getSettings()) {
            String iterClazz = setting.getIteratorClass();
            
            if (iterClazz.equals(TLDQueryIterator.class.getCanonicalName())) {
                return true;
            }
            
        }
        return false;
    }
    
    /**
     * @param plan
     * @return
     */
    private QueryData createNewQueryData(QueryPlan plan) {
        
        final String queryString = plan.getQueryString();
        List<IteratorSetting> settings = Lists.newArrayList();
        
        IteratorSetting querySettings = null;
        for (IteratorSetting setting : plan.getSettings()) {
            String iterClazz = setting.getIteratorClass();
            
            IteratorSetting newSetting = new IteratorSetting(setting.getPriority(), setting.getName(), iterClazz);
            newSetting.addOptions(setting.getOptions());
            if (iterClazz.equals(QueryIterator.class.getCanonicalName()) || iterClazz.equals(TLDQueryIterator.class.getCanonicalName())) {
                querySettings = setting;
                break;
            }
        }
        
        for (IteratorSetting setting : this.original.getSettings()) {
            String iterClazz = setting.getIteratorClass();
            
            IteratorSetting newSetting = new IteratorSetting(setting.getPriority(), setting.getName(), iterClazz);
            newSetting.addOptions(setting.getOptions());
            if (iterClazz.equals(QueryIterator.class.getCanonicalName()) || iterClazz.equals(TLDQueryIterator.class.getCanonicalName())) {
                if (iterClazz.equals(QueryIterator.class.getCanonicalName())) {
                    if (docSpecificLimitOverride) {
                        newSetting.addOption(QueryOptions.LIMIT_OVERRIDE, "true");
                    }
                }
                if (null != querySettings) {
                    if (querySettings.getOptions().get(QueryOptions.BATCHED_QUERY) != null) {
                        newSetting.addOption(QueryOptions.QUERY, "true==false");
                        int batches = Integer.valueOf(querySettings.getOptions().get(QueryOptions.BATCHED_QUERY));
                        newSetting.addOption(QueryOptions.BATCHED_QUERY, querySettings.getOptions().get(QueryOptions.BATCHED_QUERY));
                        for (int i = 0; i < batches; i++) {
                            newSetting.addOption(QueryOptions.BATCHED_QUERY_PREFIX + i, querySettings.getOptions().get(QueryOptions.BATCHED_QUERY_PREFIX + i));
                            newSetting.addOption(QueryOptions.BATCHED_QUERY_RANGE_PREFIX + i,
                                            querySettings.getOptions().get(QueryOptions.BATCHED_QUERY_RANGE_PREFIX + i));
                        }
                        
                    } else {
                        newSetting.addOption(QueryOptions.QUERY, queryString);
                    }
                } else
                    newSetting.addOption(QueryOptions.QUERY, queryString);
                
            }
            settings.add(newSetting);
        }
        return new QueryData(queryString, Lists.newArrayList(plan.getRanges()), settings, plan.getColumnFamilies());
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.io.Closeable#close()
     */
    @Override
    public void close() throws IOException {
        synchronized (producerLock) {
            if (null != this.rangeConsumer && !this.rangeConsumer.isStopped()) {
                if (log.isTraceEnabled())
                    log.trace("closing range consumer");
                this.rangeConsumer.stop();
                
                try {
                    this.rangeConsumerThread.join(500);
                } catch (InterruptedException e) {
                    log.warn(e);
                    this.rangeConsumerThread.interrupt();
                }
            }
            
            if (this.rangeConsumerThread.isAlive()) {
                this.rangeConsumerThread.interrupt();
            }
            
            if (log.isDebugEnabled()) {
                final StringBuilder sb = new StringBuilder(1024);
                sb.append("Range summary:{");
                sb.append("Produced ").append(this.producerCount).append(" QueryData objects with ").append(this.rangesProcessed).append(" total ranges");
                sb.append(", Event Ranges: ").append(eventRanges);
                sb.append(", Shard-Datatype Ranges: ").append(shardDatatypeRanges);
                sb.append(", Shard Ranges: ").append(shardRanges);
                sb.append(", Day Ranges: ").append(dayRanges).append("}");
                log.debug(sb.toString());
            }
        }
    }
    
    private class RangeConsumer implements Runnable {
        private CloseableIterable<QueryPlan> rangeIterable;
        private volatile boolean running = true;
        int count = 0;
        
        public RangeConsumer(CloseableIterable<QueryPlan> rangeIterable) {
            this.rangeIterable = rangeIterable;
        }
        
        public synchronized void stop() {
            if (log.isTraceEnabled())
                log.trace("Call called on stop");
            running = false;
            try {
                rangeIterable.close();
            } catch (IOException e) {
                log.error(e);
            }
        }
        
        public boolean isStopped() {
            return !running;
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Runnable#run()
         */
        @Override
        public void run() {
            try {
                Iterator<QueryPlan> ranges = rangeIterable.iterator();
                while (running && ranges.hasNext()) {
                    count++;
                    
                    QueryPlan nextPlan = ranges.next();
                    if (log.isTraceEnabled())
                        log.trace("RangeConsumer count is " + count + " " + nextPlan.getRanges());
                    rangeQueue.put(nextPlan);
                    
                }
                
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                rangeConsumer.stop();
            }
            running = false;
        }
    }
}
