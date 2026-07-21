package datawave.query.tables;

import java.lang.reflect.InvocationTargetException;

import org.apache.accumulo.core.client.AccumuloClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.query.tables.stats.ScanSessionStats;

/**
 * Builder for a {@link RangeStreamScanner}
 */
public class RangeStreamScannerBuilder extends SessionBuilder<RangeStreamScannerBuilder> {

    private static final Logger log = LoggerFactory.getLogger(RangeStreamScannerBuilder.class);

    private GenericQueryConfiguration config;

    /**
     * Static access enforces AccumuloClient requirement
     *
     * @param client
     *            the AccumuloClient
     * @return a new instance of a scanner session builder
     */
    public static RangeStreamScannerBuilder create(AccumuloClient client) {
        return new RangeStreamScannerBuilder(client);
    }

    /**
     * Private constructor to enforce static access
     *
     * @param client
     *            the AccumuloClient
     */
    private RangeStreamScannerBuilder(AccumuloClient client) {
        super(client);
    }

    /**
     * Set the {@link GenericQueryConfiguration}. This method can be removed once the RangeStreamScanner uses a builder instead of the ScannerFactory.
     *
     * @param config
     *            the query config
     * @return this builder
     */
    public RangeStreamScannerBuilder setConfig(GenericQueryConfiguration config) {
        this.config = config;
        return this;
    }

    /**
     * Build the {@link RangeStreamScanner}
     *
     * @return the scanner
     */
    @Override
    public RangeStreamScanner build() {
        try {
            Preconditions.checkArgument(resourceQueueSize > 0, "ResourceQueueSize must be greater than 0");
            ResourceQueue resourceQueue = new ResourceQueue(resourceQueueSize, client);

            Preconditions.checkNotNull(tableName, "TableName must be set");
            Preconditions.checkNotNull(authorizations, "Authorizations must be set");
            Preconditions.checkNotNull(query, "Query must be set");
            Preconditions.checkArgument(resultQueueSize > 0, "ResultQueueSize must be greater than 0");
            ScannerSession scannerSession = new ScannerSession(tableName, authorizations, resourceQueue, resultQueueSize, query);
            if (statsEnabled) {
                scannerSession.applyStats(new ScanSessionStats());
            }

            RangeStreamScanner session = RangeStreamScanner.class.getConstructor(ScannerSession.class).newInstance(scannerSession);

            if (consistencyLevel != null) {
                session.getOptions().setConsistencyLevel(consistencyLevel);
            }

            if (!executionHints.isEmpty()) {
                session.getOptions().setExecutionHints(executionHints);
            }

            // the RangeStreamScanner can use a builder instead of the scanner factory
            Preconditions.checkNotNull(config, "GenericQueryConfiguration must be set");
            session.setAccumuloClient(client);

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
    protected RangeStreamScannerBuilder self() {
        return this;
    }
}
