package datawave.query.scheduler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Logger;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.ScannerFactory;
import datawave.query.tables.stats.ScanSessionStats;
import datawave.webservice.query.configuration.QueryData;

/**
 * Provides a means to chain multiple schedulers together.
 */
public class ChainedScheduler extends Scheduler {

    private static final Logger log = Logger.getLogger(ChainedScheduler.class);
    private final List<Scheduler> schedulers = new ArrayList<>();

    public void addScheduler(Scheduler scheduler) {
        this.schedulers.add(scheduler);
    }

    @Override
    public BatchScanner createBatchScanner(ShardQueryConfiguration config, ScannerFactory scannerFactory, QueryData qd) throws TableNotFoundException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ScanSessionStats getSchedulerStats() {
        // TODO - think about how to approach this. Maybe retain a list for each scheduler with a get method.
        return null;
    }

    @Override
    public void close() throws IOException {
        for (Scheduler scheduler : schedulers) {
            scheduler.close();
        }
        this.schedulers.clear();
    }

    @Override
    public Iterator<Entry<Key,Value>> iterator() {
        log.debug("total schedulers: " + schedulers.size());
        return new Iter();
    }

    private class Iter implements Iterator<Entry<Key,Value>> {

        // Iterator that traverses over the schedulers.
        private final Iterator<Scheduler> schedulerIterator = schedulers.iterator();

        // The current scheduler iterator.
        private Iterator<Entry<Key,Value>> currentSchedulerIterator;

        // Whether seekToNext has ever been called.
        private boolean everSeeked = false;

        @Override
        public boolean hasNext() {
            seekToNext();
            return currentSchedulerIterator != null && currentSchedulerIterator.hasNext();
        }

        @Override
        public Entry<Key,Value> next() {
            if (!everSeeked) {
                seekToNext();
            }

            if (currentSchedulerIterator == null) {
                throw new NoSuchElementException();
            } else {
                return currentSchedulerIterator.next();
            }
        }

        /**
         * Seek to the next scheduler that has an entry remaining in it.
         */
        private void seekToNext() {
            log.debug("chained: seekToNext");
            everSeeked = true;

            // If the current scheduler iterator is null, attempt the get the next scheduler iterator, or return early if there are no more iterators.
            if (currentSchedulerIterator == null) {
                if (schedulerIterator.hasNext()) {
                    log.debug("chain: Updated iterator to next scheduler 1");
                    currentSchedulerIterator = schedulerIterator.next().iterator();
                } else {
                    log.debug("chain: No more schedulers");
                    return;
                }
            }

            // If the current sub-iterator does not have any more elements remaining, move to the next sub-iterator that does have elements.
            if (!currentSchedulerIterator.hasNext()) {
                log.debug("chain: current scheduler does not have next");
                while (schedulerIterator.hasNext()) {
                    log.debug("chain: updated iterator to next scheduler 2");
                    currentSchedulerIterator = schedulerIterator.next().iterator();
                    if (currentSchedulerIterator.hasNext()) {
                        log.debug("chain: new scheduler has next");
                        return;
                    } else {
                        log.debug("chain: new scheduler does not have next");
                    }
                }
            }
        }
    }

}
