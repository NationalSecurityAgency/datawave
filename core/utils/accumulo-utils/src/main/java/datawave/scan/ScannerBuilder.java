package datawave.scan;

import java.util.Iterator;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;

import com.google.common.base.Preconditions;

import datawave.security.util.AuthorizationsMinimizer;
import datawave.security.util.ScannerHelper;
import datawave.webservice.common.connection.ScannerDelegate;

/**
 * The builder <b>must</b> specify the AccumuloClient, the table name, and the authorizations
 * <p>
 * The builder may specify a consistency level that determines if the scan executes in a tablet server or a scan server.
 * <p>
 * The builder may specify a scan type to determine if the scan executes in a specific executor pool.
 * <p>
 * The builder may specify a scan priority to determine if the scan is prioritized ahead of or behind other scans.
 */
public class ScannerBuilder extends ScanBuilder<ScannerBuilder> {

    /**
     * Private constructor that accepts a non-null {@link AccumuloClient}
     *
     * @param client
     *            the Accumulo client
     */
    private ScannerBuilder(AccumuloClient client) {
        super(client);
    }

    /**
     * Static entry point, requires an {@link AccumuloClient}
     *
     * @param client
     *            the Accumulo client
     * @return this builder
     */
    public static ScannerBuilder create(AccumuloClient client) {
        return new ScannerBuilder(client);
    }

    /**
     * Build the scanner
     *
     * @return the scanner
     */
    @Override
    public Scanner build() {
        Preconditions.checkNotNull(tableName, "Table name must be set");
        Preconditions.checkNotNull(authorizations, "Authorizations must be set");

        try {
            // the first auth set is used to create the scanner, additional auths are added to the iterator stack
            Iterator<Authorizations> iter = AuthorizationsMinimizer.minimize(authorizations).iterator();
            Scanner scanner = client.createScanner(tableName, iter.next());
            ScannerDelegate delegate = new ScannerDelegate(scanner);
            ScannerHelper.addVisibilityFilters(iter, delegate);

            if (consistencyLevel != null) {
                delegate.setConsistencyLevel(consistencyLevel);
            }

            if (!executionHints.isEmpty()) {
                delegate.setExecutionHints(executionHints);
            }

            return delegate;
        } catch (TableNotFoundException e) {
            throw new RuntimeException("ScannerBuilder could not create scanner", e);
        }
    }

    /**
     * Allows child-specific methods to be picked up by the generic builder
     *
     * @return this builders
     */
    @Override
    protected ScannerBuilder self() {
        return this;
    }
}
