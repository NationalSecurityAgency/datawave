package datawave.query.util;

import datawave.query.iterator.QueryInformationIterator;
import datawave.query.tables.BatchScannerImpl;
import datawave.security.iterator.ConfigurableVisibilityFilter;
import datawave.security.util.AuthorizationsMinimizer;
import datawave.security.util.ScannerHelper;
import datawave.webservice.common.connection.ScannerBaseDelegate;
import datawave.webservice.query.Query;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.security.Authorizations;
import java.util.Collection;
import java.util.Iterator;

/**
 * 
 */
public class QueryScannerHelper {
    
    public static Scanner createScanner(AccumuloClient client, String tableName, Collection<Authorizations> authorizations, Query query)
                    throws TableNotFoundException {
        Scanner scanner = ScannerHelper.createScanner(client, tableName, authorizations);
        
        scanner.addScanIterator(getQueryInfoIterator(query, false));
        
        return scanner;
    }
    
    public static Scanner createScannerWithoutInfo(AccumuloClient client, String tableName, Collection<Authorizations> authorizations, Query query)
                    throws TableNotFoundException {
        
        return ScannerHelper.createScanner(client, tableName, authorizations);
    }


    public static BatchScanner createBatchScanner(AccumuloClient client, String tableName, Collection<Authorizations> authorizations, int numQueryThreads,
                    Query query) throws TableNotFoundException {
        return createBatchScanner(client, tableName, authorizations, numQueryThreads, query, false, false);
    }
    
    public static BatchScanner createBatchScanner(AccumuloClient client, String tableName, Collection<Authorizations> authorizations, int numQueryThreads,
                    Query query, boolean reportErrors, boolean customScanner) throws TableNotFoundException {
        BatchScanner batchScanner = null;
        if (authorizations != null && !authorizations.isEmpty()) {
                batchScanner =ScannerHelper.createBatchScanner(client, tableName, authorizations, numQueryThreads);
        } else {
            throw new IllegalArgumentException("Authorizations must not be empty.");
        }

        batchScanner.addScanIterator(getQueryInfoIterator(query, reportErrors));
        
        return batchScanner;
    }

    
    public static IteratorSetting getQueryInfoIterator(Query query, boolean reportErrors) {
        return getQueryInfoIterator(query, reportErrors, null);
    }
    
    public static IteratorSetting getQueryInfoIterator(Query query, boolean reportErrors, String querystring) {
        QueryInformation info = new QueryInformation(query, querystring);
        IteratorSetting cfg = new IteratorSetting(Integer.MAX_VALUE, QueryInformationIterator.class, info.toMap());
        if (reportErrors)
            QueryInformationIterator.setErrorReporting(cfg);
        return cfg;
    }
    
    public static IteratorSetting getQueryInfoIterator(Query query) {
        return getQueryInfoIterator(query, false, null);
    }

    public static void addVisibilityFilters(Iterator<Authorizations> iter, ScannerBase scanner) {
        for(int priority = 10; iter.hasNext(); ++priority) {
            IteratorSetting cfg = new IteratorSetting(priority, ConfigurableVisibilityFilter.class);
            cfg.setName("visibilityFilter" + priority);
            cfg.addOption("authorizations", ((Authorizations)iter.next()).toString());
            if (scanner instanceof ScannerBaseDelegate) {
                ((ScannerBaseDelegate)scanner).addSystemScanIterator(cfg);
            } else {
                scanner.addScanIterator(cfg);
            }
        }

    }
    
}
