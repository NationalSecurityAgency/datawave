package datawave.webservice.common.connection;

import java.util.Collection;
import java.util.Iterator;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;

import datawave.security.util.AuthorizationsMinimizer;
import datawave.security.util.ScannerHelper;

/**
 * Scanner factory for {@link WrappedAccumuloClient} that allows the table cache to be bypassed, if desired
 */
public class WrappedScannerHelper extends ScannerHelper {
    
    public static Scanner createScanner(WrappedAccumuloClient connector, String tableName, Collection<Authorizations> authorizations, boolean skipCache)
                    throws TableNotFoundException {
        if (authorizations == null || authorizations.isEmpty()) {
            throw new IllegalArgumentException("Authorizations must not be empty.");
        }
        Iterator<Authorizations> iter = AuthorizationsMinimizer.minimize(authorizations).iterator();
        Scanner scanner = connector.createScanner(tableName, iter.next(), skipCache);
        addVisibilityFilters(iter, scanner);
        return scanner;
    }
    
    public static BatchScanner createBatchScanner(WrappedAccumuloClient connector, String tableName, Collection<Authorizations> authorizations,
                    int numQueryThreads, boolean skipCache) throws TableNotFoundException {
        if (authorizations == null || authorizations.isEmpty()) {
            throw new IllegalArgumentException("Authorizations must not be empty.");
        }
        Iterator<Authorizations> iter = AuthorizationsMinimizer.minimize(authorizations).iterator();
        BatchScanner batchScanner = connector.createBatchScanner(tableName, iter.next(), numQueryThreads, skipCache);
        addVisibilityFilters(iter, batchScanner);
        return batchScanner;
    }
}
