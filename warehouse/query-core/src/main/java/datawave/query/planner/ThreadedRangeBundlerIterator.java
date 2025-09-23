package datawave.query.planner;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

import datawave.common.util.MultiComparator;
import datawave.core.common.logging.ThreadConfigurableLogger;
import datawave.core.query.configuration.QueryData;
import datawave.microservice.query.Query;
import datawave.query.CloseableIterable;
import datawave.query.iterator.QueryOptions;
import datawave.query.tables.ConsumerIterator;
import datawave.query.tables.ProducerConsumerBuffer;
import datawave.query.tld.TLDQueryIterator;
import datawave.query.util.count.CountMapSerDe;

/**
 * This class creates a decoupled producer/consumer of QueryData where the producer and/or consumer may be slow. The only bundling going on is the async
 * fetching of QueryPlan from the producer
 * <p>
 * The producer will begin producing immediately on construction by creating a new Thread running a RangeConsumer. The RangeConsumer will continually put to the
 * queue, blocking if the queue is currently full (maxRanges)
 * <p>
 * The consumer will be lazy initialized when hasNext() is called.
 */
public class ThreadedRangeBundlerIterator implements Iterator<QueryData>, Closeable {
    private static final Logger log = ThreadConfigurableLogger.getLogger(ThreadedRangeBundlerIterator.class);

    private final QueryData original;

    /**
     * Used to configure the max size of the BlockingQueue which sits between the producer and consumer. Default is 1000 if &lt;= 0 RangeConsumer will block if
     * full.
     */
    private final long maxRanges;
    private final Query settings;

    /**
     * The blocking queue to pass data between the producer and consumer
     */
    private final ProducerConsumerBuffer<QueryPlan> rangeQueue;
    private final ConsumerIterator<QueryPlan> rangeQueueConsumer;

    private RangeConsumer rangeConsumer;
    private Thread rangeConsumerThread;

    private int producerCount = 0;
    private long rangesProcessed = 0;

    private final Text holder = new Text();
    private long eventRanges = 0, shardDatatypeRanges = 0, shardRanges = 0, dayRanges = 0;

    private ASTJexlScript queryTree;

    protected boolean isTld = false;

    /**
     * Tracks when the rangeConsumer started
     */
    protected long startTimeMillis;

    private CountMapSerDe mapSerDe;

    private ThreadedRangeBundlerIterator(Builder builder) {

        this.original = builder.getOriginal();

        if (isTld(this.original.getSettings())) {
            isTld = true;
        }
        this.maxRanges = builder.getMaxRanges();
        this.settings = builder.getSettings();
        this.queryTree = builder.getQueryTree();

        // TODO Make this smarter based on num-concurrent queries, 'max' size of
        // a range, etc
        int maxCapacity = (int) maxRanges > 0 ? (int) maxRanges : 1000;
        if (builder.getQueryPlanComparators() != null && !builder.getQueryPlanComparators().isEmpty()) {
            Comparator<QueryPlan> comparator = (builder.getQueryPlanComparators().size() > 1) ? new MultiComparator<>(builder.getQueryPlanComparators())
                            : builder.getQueryPlanComparators().iterator().next();
            PriorityQueue<QueryPlan> priorityQueue = new PriorityQueue<>(comparator);
            rangeQueue = new ProducerConsumerBuffer<>(maxCapacity, priorityQueue, maxCapacity);
        } else {
            rangeQueue = new ProducerConsumerBuffer<>(maxCapacity);
        }

        // this will be used to read off the queue
        rangeQueueConsumer = new ConsumerIterator<>(rangeQueue);
        // this will be used to populate the queue
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
        return rangeQueueConsumer.hasNext();
    }

    /*
     * (non-Javadoc)
     *
     * @see java.util.Iterator#next()
     */
    @Override
    public QueryData next() {
        QueryData current = null;

        if (hasNext()) {
            current = getNext();
            this.rangesProcessed += current.getRanges().size();
            this.producerCount++;
            if (log.isTraceEnabled() && 10 % this.producerCount == 0) {
                log.trace("Produced " + this.producerCount + " QueryData objects with " + this.rangesProcessed + " total range(s)");
            }
        }

        return current;
    }

    private QueryData getNext() {
        QueryPlan plan = rangeQueueConsumer.next();
        if (null == plan) {
            // sanity check
            if (!rangeQueue.isClosed()) {
                throw new IllegalStateException("got null element from non-closed queue");
            }
            return null;
        }

        // if the generated query is larger, use the original
        if (null != queryTree && (plan.getQueryString().length() > original.getQuery().length())) {
            plan.setQueryTree(queryTree);
            plan.withQueryString(original.getQuery());
        }
        if (log.isTraceEnabled())
            log.trace("size of ranges is " + plan.getRanges());
        // if the generated query is larger, use the original

        for (Range r : plan.getRanges()) {
            if (log.isTraceEnabled())
                log.trace("Adding range" + r);
            if (null == r) {

                if (!this.rangeConsumer.isStopped()) {
                    log.warn("Consumer is still running, but could not fetch a range in " + (System.currentTimeMillis() - this.startTimeMillis));
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

        QueryData newData = createNewQueryData(plan);
        if (log.isTraceEnabled()) {
            if (null != newData) {
                log.trace("Built QueryData with " + newData.getRanges().size() + " range(s)");
                log.trace("Built QueryData " + newData.getQuery());
            } else
                log.trace("Invalid query data object built");
        }

        return newData;
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

            if (plan.getFieldCounts() != null && !plan.getFieldCounts().isEmpty()) {
                newSetting.addOption(QueryOptions.FIELD_COUNTS, getMapSerDe().serializeToString(plan.getFieldCounts()));
            }

            if (plan.getTermCounts() != null && !plan.getTermCounts().isEmpty()) {
                newSetting.addOption(QueryOptions.TERM_COUNTS, getMapSerDe().serializeToString(plan.getTermCounts()));
            }

            settings.add(newSetting);
        }

        //  @formatter:off
        return new QueryData()
                        .withTableName(plan.getTableName())
                        .withQuery(queryString)
                        .withRanges(Lists.newArrayList(plan.getRanges()))
                        .withColumnFamilies(plan.getColumnFamilies())
                        .withSettings(settings);
        //  @formatter:on
    }

    private CountMapSerDe getMapSerDe() {
        if (mapSerDe == null) {
            mapSerDe = new CountMapSerDe();
        }
        return mapSerDe;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.io.Closeable#close()
     */
    @Override
    public void close() throws IOException {
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
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                // only propogate the exception if we weren't being shutdown.
                if (running) {
                    throw new RuntimeException(e);
                }
            } finally {
                rangeQueue.close();
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
        protected Query settings;
        protected Collection<Comparator<QueryPlan>> queryPlanComparators = null;

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

        public ThreadedRangeBundlerIterator build() {
            return new ThreadedRangeBundlerIterator(this);
        }
    }
}
