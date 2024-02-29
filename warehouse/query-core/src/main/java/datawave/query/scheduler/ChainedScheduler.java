package datawave.query.scheduler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

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
        return new Iter();
    }

    private class Iter implements Iterator<Entry<Key,Value>> {

        // Iterator that traverses over the schedulers.
        private final Iterator<Scheduler> schedulerIterator = schedulers.iterator();

        // The current scheduler iterator.
        private Iterator<Entry<Key,Value>> currIterator;

        @Override
        public boolean hasNext() {
            seekToNextAvailableEntry();
            return currIterator != null && currIterator.hasNext();
        }

        @Override
        public Entry<Key,Value> next() {
            return currIterator.next();
        }

        /**
         * Seek to the next scheduler that has an entry remaining in it.
         */
        private void seekToNextAvailableEntry() {
            if (currIterator == null) {
                if (schedulerIterator.hasNext()) {
                    currIterator = schedulerIterator.next().iterator();
                    if (!currIterator.hasNext()) {
                        seekToNextAvailableEntry();
                    }
                } else {
                    return;
                }
            }

            if (!currIterator.hasNext()) {
                while (schedulerIterator.hasNext()) {
                    currIterator = schedulerIterator.next().iterator();
                    if (currIterator.hasNext()) {
                        return;
                    }
                }
            }
        }
    }
}
