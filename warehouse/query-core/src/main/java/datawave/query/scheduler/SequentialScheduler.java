package datawave.query.scheduler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.iterator.QueryIterator;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.ScannerFactory;
import datawave.query.tables.stats.ScanSessionStats;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.configuration.QueryData;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Logger;

/**
 * 
 */
public class SequentialScheduler extends Scheduler<Entry<Key,Value>> {
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
    public Iterator<Entry<Key,Value>> iterator() {
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
    
    public class SequentialSchedulerIterator implements Iterator<Entry<Key,Value>> {
        
        protected final ShardQueryConfiguration config;
        protected final ScannerFactory scannerFactory;
        
        protected Iterator<QueryData> queries = null;
        protected Entry<Key,Value> currentEntry = null;
        protected BatchScanner currentBS = null;
        protected Iterator<Entry<Key,Value>> currentIter = null;
        
        protected volatile boolean closed = false;
        
        public SequentialSchedulerIterator(ShardQueryConfiguration config, ScannerFactory scannerFactory) {
            this.config = config;
            this.scannerFactory = scannerFactory;
            this.queries = config.getQueries();
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see java.util.Iterator#hasNext()
         */
        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            
            if (null != this.currentEntry) {
                return true;
            } else if (null != this.currentBS && null != this.currentIter) {
                if (this.currentIter.hasNext()) {
                    this.currentEntry = this.currentIter.next();
                    return hasNext();
                } else {
                    this.currentBS.close();
                }
            }
            
            QueryData newQueryData = null;
            while (true) {
                if (this.queries.hasNext()) {
                    // Keep track of how many QueryData's we make
                    if (log.isTraceEnabled()){
                        log.trace("queries has next");
                    }
                    QueryData qd = this.queries.next();
                    if (null != qd.getRanges())
                        rangesSeen += qd.getRanges().size();
                    count.incrementAndGet();
                    if (null == newQueryData) {
                        newQueryData = new QueryData(qd);
                        if (log.isTraceEnabled()){
                            log.trace("Setting query to " + config.getTransformedQuery());
                        }
                        newQueryData.setQuery(config.getTransformedQuery());

                        List<IteratorSetting> newSettings = new ArrayList<>();
                        for (IteratorSetting setting : newQueryData.getSettings()) {
                            IteratorSetting newSetting = new IteratorSetting(setting.getPriority(), setting.getName(), setting.getIteratorClass());
                            newSetting.addOptions(setting.getOptions());
                            if (newSetting.getOptions().containsKey(QueryIterator.QUERY)) {
                                newSetting.addOption(QueryIterator.QUERY, config.getTransformedQuery());
                            }
                            newSettings.add(newSetting);

                        }

                        newQueryData.setSettings(newSettings);
                    }
                    else {
                        newQueryData.getRanges().addAll(qd.getRanges());
                    }
                    
                } else
                    break;
            }
            
            if (null != newQueryData) {
                
                try {
                    this.currentBS = createBatchScanner(this.config, this.scannerFactory, newQueryData);
                } catch (TableNotFoundException e) {
                    throw new RuntimeException(e);
                }
                
                this.currentIter = this.currentBS.iterator();
                
                return hasNext();
            }
            
            return false;
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see java.util.Iterator#next()
         */
        @Override
        public Entry<Key,Value> next() {
            if (closed) {
                return null;
            }
            
            if (hasNext()) {
                Entry<Key,Value> cur = this.currentEntry;
                this.currentEntry = null;
                return cur;
            }
            
            return null;
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
