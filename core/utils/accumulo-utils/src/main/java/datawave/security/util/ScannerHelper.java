package datawave.security.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.security.iterator.ConfigurableVisibilityFilter;
import datawave.webservice.common.connection.ScannerBaseDelegate;

public class ScannerHelper {
    private static final Logger logger = LoggerFactory.getLogger(ScannerHelper.class);
    
    public static Scanner createScanner(AccumuloClient connector, String tableName, Collection<Authorizations> authorizations) throws TableNotFoundException {
        if (authorizations == null || authorizations.isEmpty())
            throw new IllegalArgumentException("Authorizations must not be empty.");
        
        Iterator<Authorizations> iter = AuthorizationsMinimizer.minimize(authorizations).iterator();
        Scanner scanner = connector.createScanner(tableName, iter.next());
        addVisibilityFilters(iter, scanner);
        return scanner;
    }
    
    public static BatchScanner createBatchScanner(AccumuloClient connector, String tableName, Collection<Authorizations> authorizations, int numQueryThreads)
                    throws TableNotFoundException {
        if (authorizations == null || authorizations.isEmpty())
            throw new IllegalArgumentException("Authorizations must not be empty.");
        
        Iterator<Authorizations> iter = AuthorizationsMinimizer.minimize(authorizations).iterator();
        BatchScanner batchScanner = connector.createBatchScanner(tableName, iter.next(), numQueryThreads);
        addVisibilityFilters(iter, batchScanner);
        return batchScanner;
    }
    
    public static BatchDeleter createBatchDeleter(AccumuloClient connector, String tableName, Collection<Authorizations> authorizations, int numQueryThreads,
                    long maxMemory, long maxLatency, int maxWriteThreads) throws TableNotFoundException {
        if (authorizations == null || authorizations.isEmpty())
            throw new IllegalArgumentException("Authorizations must not be empty.");
        
        Iterator<Authorizations> iter = AuthorizationsMinimizer.minimize(authorizations).iterator();
        BatchWriterConfig bwCfg = new BatchWriterConfig().setMaxLatency(maxLatency, TimeUnit.MILLISECONDS).setMaxMemory(maxMemory)
                        .setMaxWriteThreads(maxWriteThreads);
        BatchDeleter batchDeleter = connector.createBatchDeleter(tableName, iter.next(), numQueryThreads, bwCfg);
        addVisibilityFilters(iter, batchDeleter);
        return batchDeleter;
    }
    
    protected static void addVisibilityFilters(Iterator<Authorizations> iter, ScannerBase scanner) {
        for (int priority = 10; iter.hasNext(); priority++) {
            IteratorSetting cfg = new IteratorSetting(priority, ConfigurableVisibilityFilter.class);
            cfg.setName("visibilityFilter" + priority);
            cfg.addOption(ConfigurableVisibilityFilter.AUTHORIZATIONS_OPT, iter.next().toString());
            // Set the visibility filter as a "system" iterator, which means that normal modify, remove, clear operations performed
            // on the scanner will not modify/remove/clear this iterator. This way, if a query logic attempts to reconfigure the
            // scanner's iterators, then this iterator will remain intact.
            if (scanner instanceof ScannerBaseDelegate) {
                ((ScannerBaseDelegate) scanner).addSystemScanIterator(cfg);
            } else {
                logger.warn("Adding system visibility filter to non-wrapped scanner {}.", scanner.getClass(), new Exception());
                scanner.addScanIterator(cfg);
            }
        }
    }
}
