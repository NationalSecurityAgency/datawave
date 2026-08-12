package datawave.query.tables;

import java.lang.reflect.InvocationTargetException;

import org.apache.accumulo.core.client.AccumuloClient;

import com.google.common.base.Preconditions;

import datawave.query.tables.stats.ScanSessionStats;

/**
 * Builder for a {@link BatchScannerSessionLongAdder}
 */
public class BatchScannerSessionLongAdderBuilder extends SessionBuilder<BatchScannerSessionLongAdderBuilder> {

    private static final int DEFAULT_THREAD_COUNT = 5;
    private int numQueryThreads = DEFAULT_THREAD_COUNT;

    /**
     * Static access enforces AccumuloClient requirement
     *
     * @param client
     *            the AccumuloClient
     * @return a new instance of a scanner session builder
     */
    public static BatchScannerSessionLongAdderBuilder create(AccumuloClient client) {
        return new BatchScannerSessionLongAdderBuilder(client);
    }

    /**
     * Private constructor to enforce static access
     *
     * @param client
     *            the AccumuloClient
     */
    private BatchScannerSessionLongAdderBuilder(AccumuloClient client) {
        super(client);
    }

    /**
     * Set the number of query threads
     *
     * @param numQueryThreads
     *            the number of query threads
     * @return this builder
     */
    public BatchScannerSessionLongAdderBuilder setNumQueryThreads(int numQueryThreads) {
        this.numQueryThreads = numQueryThreads;
        return this;
    }

    /**
     * Get the number of query threads
     *
     * @return the number of query threads
     */
    public int getNumQueryThreads() {
        return numQueryThreads;
    }

    /**
     * Build the {@link BatchScannerSession}
     *
     * @return the BatchScannerSession
     */
    @Override
    public BatchScannerSessionLongAdder build() {
        try {
            Preconditions.checkArgument(resourceQueueSize > 0, "ResourceQueueSize must be greater than 0");
            ResourceQueue resourceQueue = new ResourceQueue(resourceQueueSize, client);

            Preconditions.checkNotNull(tableName, "TableName must be set");
            Preconditions.checkNotNull(authorizations, "Authorizations must be set");
            Preconditions.checkNotNull(query, "Query must be set");
            Preconditions.checkArgument(resultQueueSize > 0, "ResultQueueSize must be greater than 0");
            ScannerSession scannerSession = new ScannerSession(tableName, authorizations, resourceQueue, resultQueueSize, query);

            //  @formatter:off
            BatchScannerSessionLongAdder session = BatchScannerSessionLongAdder.class.getConstructor(ScannerSession.class)
                    .newInstance(scannerSession)
                    .setThreads(numQueryThreads);
            //  @formatter:on

            if (statsEnabled) {
                session.applyStats(new ScanSessionStats());
            }

            if (consistencyLevel != null) {
                session.getOptions().setConsistencyLevel(consistencyLevel);
            }

            if (!executionHints.isEmpty()) {
                session.getOptions().setExecutionHints(executionHints);
            }

            return session;
        } catch (InvocationTargetException | InstantiationException | IllegalAccessException | NoSuchMethodException e) {
            throw new RuntimeException("Could not create BatchScannerSession", e);
        }
    }

    /**
     * Allow child-specific methods to be picked up by the builder
     *
     * @return this builder
     */
    @Override
    protected BatchScannerSessionLongAdderBuilder self() {
        return this;
    }
}
