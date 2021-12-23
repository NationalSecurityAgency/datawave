package datawave.query.scheduler.document.batch;

import datawave.query.attributes.Document;
import datawave.query.config.DocumentQueryConfiguration;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.iterator.QueryIterator;
import datawave.query.scheduler.Scheduler;
import datawave.query.tables.DocumentBatchScannerSession;
import datawave.query.tables.document.batch.DocumentLogic;
import datawave.query.tables.document.batch.DocumentScannerBase;
import datawave.query.tables.document.batch.DocumentScannerImpl;
import datawave.query.tables.MyScannerFactory;
import datawave.query.tables.ScannerFactory;
import datawave.query.tables.serialization.SerializedDocumentIfc;
import datawave.query.tables.stats.ScanSessionStats;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.configuration.QueryData;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class DocumentScheduler extends Scheduler<SerializedDocumentIfc> {
    private static final Logger log = ThreadConfigurableLogger.getLogger(DocumentScheduler.class);

    protected final DocumentQueryConfiguration config;
    protected final ScannerFactory scannerFactory;
    protected final AtomicInteger count = new AtomicInteger(0);

    protected DocumentSchedulerIterator iterator = null;

    /**
     * Statistics used for validation.
     */
    protected int rangesSeen = 0;

    public DocumentScheduler(DocumentQueryConfiguration config, ScannerFactory scannerFactory) {
        this.config = config;
        this.scannerFactory = scannerFactory;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Iterable#iterator()
     */
    @Override
    public Iterator<SerializedDocumentIfc> iterator() {
        if (null == this.config) {
            throw new IllegalArgumentException("Null configuration provided");
        }

        try {
            this.iterator = new DocumentSchedulerIterator(this.config, this.scannerFactory);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

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
    public DocumentScannerBase createBatchScanner(ShardQueryConfiguration config, ScannerFactory scannerFactory, QueryData qd) throws TableNotFoundException {
        return DocumentLogic.createDocumentScanner(DocumentQueryConfiguration.class.cast(config), MyScannerFactory.class.cast(scannerFactory), qd, config.getReturnType());
    }

    public class DocumentSchedulerIterator implements Iterator<SerializedDocumentIfc> {

        protected final ShardQueryConfiguration config;
        protected final ScannerFactory scannerFactory;

        protected Iterator<QueryData> queries = null;
        protected SerializedDocumentIfc currentDocument = null;
        protected DocumentScannerBase currentBS = null;
        protected Iterator<SerializedDocumentIfc> currentIter = null;
        protected DocumentBatchScannerSession session = null;

        protected volatile boolean closed = false;

        public DocumentSchedulerIterator(DocumentQueryConfiguration config, ScannerFactory scannerFactory) throws Exception {
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
            
            if (null != this.currentDocument) {
                return true;
            } else if (null != this.currentBS && null != this.currentIter) {
                if (this.currentIter.hasNext()) {
                    this.currentDocument = this.currentIter.next();
                    
                    return hasNext();
                } else {
                    this.currentBS.close();
                }
            }
            
            QueryData newQueryData = null;
            while (true) {
                if (this.queries.hasNext()) {
                    // Keep track of how many QueryData's we make
                    QueryData qd = this.queries.next();
                    if (null != qd.getRanges())
                        rangesSeen += qd.getRanges().size();
                    count.incrementAndGet();
                    if (null == newQueryData) {
                        newQueryData = new QueryData(qd);
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
                
                this.currentIter = this.currentBS.getDocumentIterator();
                
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
        public SerializedDocumentIfc next() {
            if (closed) {
                return null;
            }
            
            if (hasNext()) {
                SerializedDocumentIfc cur = this.currentDocument;
                this.currentDocument = null;
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
