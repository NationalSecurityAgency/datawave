package datawave.query.tables;

import java.lang.reflect.InvocationTargetException;

import org.apache.accumulo.core.client.AccumuloClient;

import com.google.common.base.Preconditions;

import datawave.query.tables.stats.ScanSessionStats;

/**
 * Builder for the {@link AnyFieldScanner}
 * <p>
 * This is virtually identical to the {@link ScannerSessionBuilder} but per-instance builders are better for clarity.
 */
public class AnyFieldScannerBuilder extends SessionBuilder<AnyFieldScannerBuilder> {

    /**
     * Static access enforces AccumuloClient requirement
     *
     * @param client
     *            the AccumuloClient
     * @return a new instance of a scanner session builder
     */
    public static AnyFieldScannerBuilder create(AccumuloClient client) {
        return new AnyFieldScannerBuilder(client);
    }

    /**
     * Private constructor to enforce static access
     *
     * @param client
     *            the AccumuloClient
     */
    private AnyFieldScannerBuilder(AccumuloClient client) {
        super(client);
    }

    /**
     * Build the {@link AnyFieldScanner}
     *
     * @return the AnyFieldScanner
     */
    @Override
    public AnyFieldScanner build() {
        try {
            Preconditions.checkArgument(resourceQueueSize > 0, "ResourceQueueSize must be greater than 0");
            ResourceQueue resourceQueue = new ResourceQueue(resourceQueueSize, client);

            Preconditions.checkNotNull(tableName, "TableName must be set");
            Preconditions.checkNotNull(authorizations, "Authorizations must be set");
            Preconditions.checkNotNull(query, "Query must be set");
            Preconditions.checkArgument(resultQueueSize > 0, "ResultQueueSize must be greater than 0");
            ScannerSession scannerSession = new ScannerSession(tableName, authorizations, resourceQueue, resultQueueSize, query);

            AnyFieldScanner session = AnyFieldScanner.class.getConstructor(ScannerSession.class).newInstance(scannerSession);

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
    protected AnyFieldScannerBuilder self() {
        return this;
    }
}
