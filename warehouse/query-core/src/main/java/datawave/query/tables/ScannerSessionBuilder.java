package datawave.query.tables;

import org.apache.accumulo.core.client.AccumuloClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import datawave.query.tables.stats.ScanSessionStats;

/**
 * Builder for a {@link ScannerSession}
 */
public class ScannerSessionBuilder extends SessionBuilder<ScannerSessionBuilder> {

    private static final Logger log = LoggerFactory.getLogger(ScannerSessionBuilder.class);

    /**
     * Static access enforces AccumuloClient requirement
     *
     * @param client
     *            the AccumuloClient
     * @return a new instance of a scanner session builder
     */
    public static ScannerSessionBuilder create(AccumuloClient client) {
        return new ScannerSessionBuilder(client);
    }

    /**
     * Private constructor to enforce static access
     *
     * @param client
     *            the AccumuloClient
     */
    private ScannerSessionBuilder(AccumuloClient client) {
        super(client);
    }

    /**
     * Allow child-specific methods to be picked up by the builder
     *
     * @return this builder
     */
    @Override
    protected ScannerSessionBuilder self() {
        return this;
    }

    /**
     * Build the {@link ScannerSession}
     *
     * @return the scanner
     */
    @Override
    public ScannerSession build() {
        Preconditions.checkNotNull(tableName, "TableName must be set");
        Preconditions.checkNotNull(authorizations, "Authorizations must be set");
        Preconditions.checkArgument(resourceQueueSize > 0, "ResourceQueueSize must be greater than 0");
        ResourceQueue resourceQueue = new ResourceQueue(resourceQueueSize, client);

        if (log.isTraceEnabled()) {
            log.trace("Creating ScannerSession for {}", tableName);
        }

        Preconditions.checkArgument(resultQueueSize > 0, "ResultQueueSize must be greater than 0");
        Preconditions.checkNotNull(query, "Query must be set");
        ScannerSession session = new ScannerSession(tableName, authorizations, resourceQueue, resultQueueSize, query);

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
    }
}
