package datawave.query.scan;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
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
public class BatchScannerBuilder extends ScanBuilder<BatchScannerBuilder> {

    private static final int DEFAULT_NUM_QUERY_THREADS = 8;
    private int numQueryThreads = DEFAULT_NUM_QUERY_THREADS;

    /**
     * Default constructor, delegates to {@link ScanBuilder}
     *
     * @param client
     *            the Accumulo client
     */
    private BatchScannerBuilder(AccumuloClient client) {
        super(client);
    }

    /**
     * Create the builder. AccumuloClient required.
     *
     * @param client
     *            the Accumulo client
     * @return the builder
     */
    public static BatchScannerBuilder create(AccumuloClient client) {
        return new BatchScannerBuilder(client);
    }

    /**
     * Set the number of query threads. The default value is {@link #DEFAULT_NUM_QUERY_THREADS}
     *
     * @param numQueryThreads
     *            the number threads used by the batch scanner
     * @return this builder
     */
    public BatchScannerBuilder setNumQueryThreads(int numQueryThreads) {
        this.numQueryThreads = numQueryThreads;
        return self();
    }

    /**
     * Create the {@link BatchScanner}
     *
     * @return the batch scanner
     */
    @Override
    public BatchScanner build() {
        Preconditions.checkNotNull(tableName, "Table name must be set");
        Preconditions.checkNotNull(authorizations, "Authorizations must be set");

        try {
            BatchScanner scanner = client.createBatchScanner(tableName, authorizations, numQueryThreads);

            if (consistencyLevel != null) {
                scanner.setConsistencyLevel(consistencyLevel);
            }

            if (!executionHints.isEmpty()) {
                scanner.setExecutionHints(executionHints);
            }

            return scanner;
        } catch (TableNotFoundException e) {
            throw new RuntimeException("Could not create scanner", e);
        }
    }

    /**
     * Get the builder implementation
     *
     * @return the builder
     */
    @Override
    public BatchScannerBuilder self() {
        return this;
    }

    /**
     * Get the number of query threads used by the batch scanner
     *
     * @return the number of query threads
     */
    public int getNumQueryThreads() {
        return numQueryThreads;
    }
}
