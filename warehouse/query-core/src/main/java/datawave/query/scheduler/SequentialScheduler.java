package datawave.query.scheduler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

import datawave.core.common.logging.ThreadConfigurableLogger;
import datawave.core.query.configuration.QueryData;
import datawave.core.query.configuration.Result;
import datawave.core.query.logic.QueryCheckpoint;
import datawave.core.query.logic.QueryKey;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.ScannerFactory;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.stats.ScanSessionStats;

/**
 *
 */
public class SequentialScheduler extends Scheduler {
    private static final Logger log = ThreadConfigurableLogger.getLogger(SequentialScheduler.class);

    protected final ShardQueryConfiguration config;
    protected final ScannerFactory scannerFactory;
    protected final AtomicInteger count = new AtomicInteger(0);

    protected SequentialSchedulerIterator iterator = null;

    /**
     * Statistics used for validation.
     */
    protected int rangesSeen = 0;

    public SequentialScheduler(ShardQueryConfiguration config, ScannerFactory scannerFactory) {
        this.config = config;
        this.scannerFactory = scannerFactory;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Iterable#iterator()
     */
    @Override
    public Iterator<Result> iterator() {
        if (null == this.config) {
            throw new IllegalArgumentException("Null configuration provided");
        }

        this.iterator = new SequentialSchedulerIterator(this.config, this.scannerFactory);

        return this.iterator;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.io.Closeable#close()
     */
    @Override
    public void close() throws IOException {
        if (null != this.iterator) {
            this.iterator.close();
        }

        log.debug("Ran " + count.get() + " queries for a single user query");
    }

    /*
     * (non-Javadoc)
     *
     * @see Scheduler#createBatchScanner(ShardQueryConfiguration, datawave.query.tables.ScannerFactory, datawave.webservice.query.configuration.QueryData)
     */
    @Override
    public BatchScanner createBatchScanner(ShardQueryConfiguration config, ScannerFactory scannerFactory, QueryData qd) throws TableNotFoundException {
        return ShardQueryLogic.createBatchScanner(config, scannerFactory, qd);
    }

    public class SequentialSchedulerIterator implements Iterator<Result> {
        protected final ShardQueryConfiguration config;
        protected final ScannerFactory scannerFactory;

        protected Iterator<QueryData> queries = null;
        protected QueryData currentQuery = null;
        protected Result currentEntry = null;
        protected Result lastEntry = null;
        protected BatchScanner currentBS = null;
        protected Iterator<Result> currentIter = null;

        protected volatile boolean closed = false;

        public SequentialSchedulerIterator(ShardQueryConfiguration config, ScannerFactory scannerFactory) {
            this.config = config;
            this.scannerFactory = scannerFactory;
            this.queries = config.getQueriesIter();
            if (this.config.isCheckpointable()) {
                this.queries = new SingleRangeQueryDataIterator(this.queries);
            }
        }

        /*
         * (non-Javadoc)
         *
         * @see java.util.Iterator#hasNext()
         */
        @Override
        public boolean hasNext() {
            while (true) {
                if (closed) {
                    return false;
                }

                if (null != this.currentEntry) {
                    return true;
                } else if (null != this.currentBS && null != this.currentIter) {
                    if (this.currentIter.hasNext()) {
                        this.currentEntry = this.currentIter.next();
                        continue;
                    } else {
                        this.currentBS.close();
                        this.currentBS = null;
                        this.currentIter = null;
                    }
                }

                lastEntry = null;
                currentQuery = null;
                if (this.queries.hasNext()) {
                    // Keep track of how many QueryData's we make
                    QueryData qd = this.queries.next();
                    if (null != qd.getRanges()) {
                        rangesSeen += qd.getRanges().size();
                    }
                    count.incrementAndGet();
                    currentQuery = qd;
                }

                if (null != currentQuery) {

                    try {
                        this.currentBS = createBatchScanner(this.config, this.scannerFactory, currentQuery);
                    } catch (TableNotFoundException e) {
                        throw new RuntimeException(e);
                    }

                    this.currentIter = Result.resultIterator(currentQuery, this.currentBS.iterator());
                } else {
                    return false;
                }
            }
        }

        /*
         * (non-Javadoc)
         *
         * @see java.util.Iterator#next()
         */
        @Override
        public Result next() {
            if (closed) {
                return null;
            }

            if (hasNext()) {
                this.lastEntry = this.currentEntry;
                this.currentEntry = null;
                return this.lastEntry;
            }

            return null;
        }

        public List<QueryCheckpoint> checkpoint(QueryKey queryKey) {
            close();
            List<QueryCheckpoint> checkpoints = new ArrayList<>();
            if (currentQuery != null) {
                checkpoints.add(new QueryCheckpoint(queryKey, Collections.singletonList(currentQuery)));
                currentQuery = null;
            }
            while (queries.hasNext()) {
                checkpoints.add(new QueryCheckpoint(queryKey, Collections.singletonList(queries.next())));
            }
            config.setQueries(null);
            config.setQueriesIter(null);
            return checkpoints;
        }

        /*
         * (non-Javadoc)
         *
         * @see java.util.Iterator#remove()
         */
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        public void close() {
            if (!closed) {
                closed = true;
            }
            if (null != this.currentBS) {
                this.currentBS.close();
            }
        }
    }

    @Override
    public List<QueryCheckpoint> checkpoint(QueryKey queryKey) {
        if (null == this.config) {
            throw new IllegalArgumentException("Null configuration provided");
        }
        if (!config.isCheckpointable()) {
            throw new UnsupportedOperationException("Cannot checkpoint a scheduler which is not checkpointable");
        }
        if (this.iterator != null) {
            return this.iterator.checkpoint(queryKey);
        } else {
            return Lists.newArrayList(new QueryCheckpoint(queryKey, config.getQueries()));
        }
    }

    @Override
    public ScanSessionStats getSchedulerStats() {
        return null;
    }

    public int getRangesSeen() {
        return rangesSeen;
    }

    public int getQueryDataSeen() {
        return count.get();
    }

}
