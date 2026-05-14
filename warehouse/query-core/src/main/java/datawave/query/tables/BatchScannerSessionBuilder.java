package datawave.query.tables;

import java.lang.reflect.InvocationTargetException;

import org.apache.accumulo.core.client.AccumuloClient;

import com.google.common.base.Preconditions;

import datawave.query.tables.stats.ScanSessionStats;

/**
 * Builder for a {@link BatchScannerSession}
 */
public class BatchScannerSessionBuilder extends SessionBuilder<BatchScannerSessionBuilder> {

    private static final int DEFAULT_THREAD_COUNT = 5;
    private int numQueryThreads = DEFAULT_THREAD_COUNT;

    /**
     * Static access enforces AccumuloClient requirement
     *
     * @param client
     *            the AccumuloClient
     * @return a new instance of a scanner session builder
     */
    public static BatchScannerSessionBuilder create(AccumuloClient client) {
        return new BatchScannerSessionBuilder(client);
    }

    /**
     * Private constructor to enforce static access
     *
     * @param client
     *            the AccumuloClient
     */
    private BatchScannerSessionBuilder(AccumuloClient client) {
        super(client);
    }

    /**
     * Set the number of query threads
     *
     * @param numQueryThreads
     *            the number of query threads
     * @return this builder
     */
    public BatchScannerSessionBuilder setNumQueryThreads(int numQueryThreads) {
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
    public BatchScannerSession build() {
        try {
            Preconditions.checkArgument(resourceQueueSize > 0, "ResourceQueueSize must be greater than 0");
            ResourceQueue resourceQueue = new ResourceQueue(resourceQueueSize, client);

            Preconditions.checkNotNull(tableName, "TableName must be set");
            Preconditions.checkNotNull(authorizations, "Authorizations must be set");
            Preconditions.checkNotNull(query, "Query must be set");
            Preconditions.checkArgument(resultQueueSize > 0, "ResultQueueSize must be greater than 0");
            ScannerSession scannerSession = new ScannerSession(tableName, authorizations, resourceQueue, resultQueueSize, query);

            //  @formatter:off
            BatchScannerSession session = BatchScannerSession.class.getConstructor(ScannerSession.class)
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
    protected BatchScannerSessionBuilder self() {
        return this;
    }
}
