package datawave.query.planner;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

import datawave.common.util.MultiComparator;
import datawave.common.util.concurrent.BoundedBlockingQueue;
import datawave.core.common.logging.ThreadConfigurableLogger;
import datawave.core.query.configuration.QueryData;
import datawave.microservice.query.Query;
import datawave.query.CloseableIterable;
import datawave.query.tld.TLDQueryIterator;

public class ThreadedRangeBundlerIterator implements Iterator<QueryData>, Closeable {
    private static final Logger log = ThreadConfigurableLogger.getLogger(ThreadedRangeBundlerIterator.class);

    private final long maxWaitValue;
    private final TimeUnit maxWaitUnit;
    private final QueryData original;
    private final long maxRanges;
    private final Query settings;

    private final BlockingQueue<QueryPlan> rangeQueue;

    private QueryData next = null;
    private Object producerLock = new Object();

    private RangeConsumer rangeConsumer;
    private Thread rangeConsumerThread;

    private int producerCount = 0;
    private long rangesProcessed = 0;

    private final Text holder = new Text();
    private long eventRanges = 0, shardDatatypeRanges = 0, shardRanges = 0, dayRanges = 0;

    private ASTJexlScript queryTree;

    protected boolean isTld = false;

    protected int numRangesToBuffer;
    protected long rangeBufferTimeoutMillis;
    protected long rangeBufferPollMillis;
    protected long startTimeMillis;

    private ThreadedRangeBundlerIterator(Builder builder) {

        this.original = builder.getOriginal();

        if (isTld(this.original.getSettings())) {
            isTld = true;
        }
        this.maxRanges = builder.getMaxRanges();
        this.settings = builder.getSettings();
        this.queryTree = builder.getQueryTree();

        this.maxWaitValue = builder.getMaxWaitValue();
        this.maxWaitUnit = builder.getMaxWaitUnit();

        // TODO Make this smarter based on num-concurrent queries, 'max' size of
        // a range, etc
        int maxCapacity = (int) maxRanges > 0 ? (int) maxRanges : 1000;
        if (builder.getQueryPlanComparators() != null && !builder.getQueryPlanComparators().isEmpty()) {
            Comparator<QueryPlan> comparator = (builder.getQueryPlanComparators().size() > 1) ? new MultiComparator<>(builder.getQueryPlanComparators())
                            : builder.getQueryPlanComparators().iterator().next();

            PriorityBlockingQueue<QueryPlan> nonblockingRangeQueue = new PriorityBlockingQueue<>(maxCapacity, comparator);
            rangeQueue = new BoundedBlockingQueue<>(maxCapacity, nonblockingRangeQueue);
        } else {
            rangeQueue = new ArrayBlockingQueue<>(maxCapacity);
        }

        this.numRangesToBuffer = builder.getNumRangesToBuffer();
        this.rangeBufferTimeoutMillis = builder.getRangeBufferTimeoutMillis();
        this.rangeBufferPollMillis = builder.getRangeBufferPollMillis();

        rangeConsumer = new RangeConsumer(builder.getRanges());
        rangeConsumerThread = new Thread(rangeConsumer);
        if (settings.getId() != null)
            rangeConsumerThread.setName("RangeBundlerIterator for " + settings.getId());
        else
            rangeConsumerThread.setName("RangeBundlerIterator for ");
        rangeConsumerThread.setUncaughtExceptionHandler(settings.getUncaughtExceptionHandler());

        this.startTimeMillis = System.currentTimeMillis();
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

                    // wait until we have a minimum number of ranges buffered OR the buffer is full OR the specified
                    // amount of time to wait has elapsed OR we have processed all of our ranges before continuing
                    while (this.rangeQueue.size() < numRangesToBuffer && this.rangeQueue.remainingCapacity() > 0
                                    && (startTimeMillis + rangeBufferTimeoutMillis) > System.currentTimeMillis() && !rangeConsumer.isStopped()) {
                        Thread.sleep(rangeBufferPollMillis);
                    }

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
                                } else {
                                    shardDatatypeRanges++;
                                }
                            } else {
                                sk.getRow(holder);
                                if (holder.find("_") > 0) {
                                    shardRanges++;
                                } else {
                                    dayRanges++;
                                }
                            }

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
     * Determines if we are running a tld query
     *
     * @param settings
     *            original query settings.
     * @return if we are running a tld query
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
     *            the query plan
     * @return new query data
     */
    private QueryData createNewQueryData(QueryPlan plan) {

        final String queryString = plan.getQueryString();
        List<IteratorSetting> settings = Lists.newArrayList();
        for (IteratorSetting setting : this.original.getSettings()) {
            String iterClazz = setting.getIteratorClass();

            IteratorSetting newSetting = new IteratorSetting(setting.getPriority(), setting.getName(), iterClazz);
            newSetting.addOptions(setting.getOptions());
            settings.add(newSetting);
        }
        return new QueryData(plan.getTableName(), queryString, Lists.newArrayList(plan.getRanges()), settings, plan.getColumnFamilies());
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
                // only propogate the exception if we weren't being shutdown.
                if (running) {
                    throw new RuntimeException(e);
                }
            } finally {
                rangeConsumer.stop();
            }
            running = false;
        }
    }

    public static class Builder {
        protected QueryData original;
        protected ASTJexlScript queryTree;
        protected CloseableIterable<QueryPlan> ranges;
        protected long maxRanges;
        protected long maxWaitValue;
        protected TimeUnit maxWaitUnit;
        protected Query settings;
        protected Collection<Comparator<QueryPlan>> queryPlanComparators = null;
        protected int numRangesToBuffer = 0;
        protected long rangeBufferTimeoutMillis = 0;
        protected long rangeBufferPollMillis = 100;

        public QueryData getOriginal() {
            return original;
        }

        public Builder setOriginal(QueryData original) {
            this.original = original;
            return this;
        }

        public ASTJexlScript getQueryTree() {
            return queryTree;
        }

        public Builder setQueryTree(ASTJexlScript queryTree) {
            this.queryTree = queryTree;
            return this;
        }

        public CloseableIterable<QueryPlan> getRanges() {
            return ranges;
        }

        public Builder setRanges(CloseableIterable<QueryPlan> ranges) {
            this.ranges = ranges;
            return this;
        }

        public long getMaxRanges() {
            return maxRanges;
        }

        public Builder setMaxRanges(long maxRanges) {
            this.maxRanges = maxRanges;
            return this;
        }

        public long getMaxWaitValue() {
            return maxWaitValue;
        }

        public Builder setMaxWaitValue(long maxWaitValue) {
            this.maxWaitValue = maxWaitValue;
            return this;
        }

        public TimeUnit getMaxWaitUnit() {
            return maxWaitUnit;
        }

        public Builder setMaxWaitUnit(TimeUnit maxWaitUnit) {
            this.maxWaitUnit = maxWaitUnit;
            return this;
        }

        public Query getSettings() {
            return settings;
        }

        public Builder setSettings(Query settings) {
            this.settings = settings;
            return this;
        }

        public Collection<Comparator<QueryPlan>> getQueryPlanComparators() {
            return queryPlanComparators;
        }

        public Builder setQueryPlanComparators(Collection<Comparator<QueryPlan>> queryPlanComparators) {
            this.queryPlanComparators = queryPlanComparators;
            return this;
        }

        public int getNumRangesToBuffer() {
            return numRangesToBuffer;
        }

        public Builder setNumRangesToBuffer(int numRangesToBuffer) {
            this.numRangesToBuffer = numRangesToBuffer;
            return this;
        }

        public long getRangeBufferTimeoutMillis() {
            return rangeBufferTimeoutMillis;
        }

        public Builder setRangeBufferTimeoutMillis(long rangeBufferTimeoutMillis) {
            this.rangeBufferTimeoutMillis = rangeBufferTimeoutMillis;
            return this;
        }

        public long getRangeBufferPollMillis() {
            return rangeBufferPollMillis;
        }

        public Builder setRangeBufferPollMillis(long rangeBufferPollMillis) {
            this.rangeBufferPollMillis = rangeBufferPollMillis;
            return this;
        }

        public ThreadedRangeBundlerIterator build() {
            return new ThreadedRangeBundlerIterator(this);
        }
    }
}
