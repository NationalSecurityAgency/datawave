package datawave.scan;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;

import com.google.common.base.Preconditions;

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
            Scanner scanner = client.createScanner(tableName, authorizations);

            if (consistencyLevel != null) {
                scanner.setConsistencyLevel(consistencyLevel);
            }

            if (!executionHints.isEmpty()) {
                scanner.setExecutionHints(executionHints);
            }

            return scanner;
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
