package datawave.query.tables;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.security.Authorizations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import datawave.microservice.query.Query;
import datawave.query.tables.stats.ScanSessionStats;

/**
 * Builder for a Datawave {@link ScannerSession}
 */
public class ScannerSessionBuilder {

    private static final Logger log = LoggerFactory.getLogger(ScannerSessionBuilder.class);

    private static final int DEFAULT_SCAN_QUEUE_SIZE = 100;
    private static final int DEFAULT_RESULT_QUEUE_SIZE = 1_000;

    private int numScanResources = DEFAULT_SCAN_QUEUE_SIZE;
    private int resultQueueSize = DEFAULT_RESULT_QUEUE_SIZE;

    private Class<? extends ScannerSession> wrapper;
    private boolean statsEnabled = false;

    private String tableName;
    private Set<Authorizations> auths;
    private Query query;
    private ScannerBase.ConsistencyLevel level;
    private Map<String,String> hints;

    private final AccumuloClient client;

    public ScannerSessionBuilder(AccumuloClient client) {
        Preconditions.checkNotNull(client, "AccumuloClient must be set");
        this.client = client;
    }

    /**
     * Optional Parameter, sets the number of threads used by the scanner session
     *
     * @param numThreads
     *            the number of threads
     * @return the builder
     */
    public ScannerSessionBuilder withNumThreads(int numThreads) {
        this.numScanResources = numThreads;
        return this;
    }

    /**
     * Optional Parameter, sets the result queue size
     *
     * @param resultQueueSize
     *            the result queue size
     * @return the builder
     */
    public ScannerSessionBuilder withResultQueueSize(int resultQueueSize) {
        this.resultQueueSize = resultQueueSize;
        return this;
    }

    /**
     * Required Parameter, the type of ScannerSession to build
     *
     * @param wrapper
     *            the scanner session class
     * @return the builder
     */
    public ScannerSessionBuilder withWrapper(Class<? extends ScannerSession> wrapper) {
        this.wrapper = wrapper;
        return this;
    }

    /**
     * Optional Parameter, should the scanner session record stats
     *
     * @param statsEnabled
     *            flag that determines if stats are recorded
     * @return the builder
     */
    public ScannerSessionBuilder withStats(boolean statsEnabled) {
        this.statsEnabled = statsEnabled;
        return this;
    }

    /**
     * Required parameter
     *
     * @param tableName
     *            the table name
     * @return the builder
     */
    public ScannerSessionBuilder withTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    /**
     * Required parameter
     *
     * @param auths
     *            the authorizations
     * @return the builder
     */
    public ScannerSessionBuilder withAuths(Set<Authorizations> auths) {
        this.auths = auths;
        return this;
    }

    /**
     * Optional parameter
     *
     * @param query
     *            a {@link Query} instance
     * @return the builder
     */
    public ScannerSessionBuilder withQuery(Query query) {
        this.query = query;
        return this;
    }

    /**
     * Optional parameter
     *
     * @param level
     *            the {@link ScannerBase.ConsistencyLevel}
     * @return the builder
     */
    public ScannerSessionBuilder withConsistencyLevel(ScannerBase.ConsistencyLevel level) {
        this.level = level;
        return this;
    }

    /**
     * Optional parameter
     *
     * @param hints
     *            a map of execution hints
     * @return the builder
     */
    public ScannerSessionBuilder withExecutionHints(Map<String,String> hints) {
        this.hints = hints;
        return this;
    }

    /**
     * Build the {@link Scanner}, setting any optional configs if necessary
     *
     * @return a Scanner
     */
    public <T extends ScannerSession> T build() {
        ResourceQueue resourceQueue;
        Preconditions.checkNotNull(tableName, "TableName must be set");
        Preconditions.checkNotNull(auths, "Authorizations must be set");
        Preconditions.checkNotNull(wrapper, "ScannerSession type must be set");

        try {

            resourceQueue = new ResourceQueue(numScanResources, client);

            log.debug("Creating ScannerSession with {} threads", resourceQueue.getCapacity());

            T session;
            if (wrapper == ScannerSession.class) {
                session = (T) new ScannerSession(tableName, auths, resourceQueue, resultQueueSize, query);
            } else {
                session = (T) wrapper.getConstructor(ScannerSession.class)
                                .newInstance(new ScannerSession(tableName, auths, resourceQueue, resultQueueSize, query));
            }

            if (session instanceof RangeStreamScanner) {
                // deal with the funkitude
                ((RangeStreamScanner) session).setAccumuloClient(client);
            }

            if (statsEnabled) {
                session.applyStats(new ScanSessionStats());
            }

            if (level != null) {
                SessionOptions options = session.getOptions();
                options.setConsistencyLevel(level);
                session.setOptions(options);
            }

            if (hints != null) {
                SessionOptions options = session.getOptions();
                options.setExecutionHints(hints);
                session.setOptions(options);
            }

            return session;
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    public int getNumScanResources() {
        return numScanResources;
    }

    public int getResultQueueSize() {
        return resultQueueSize;
    }

    public Class<? extends ScannerSession> getWrapper() {
        return wrapper;
    }

    public boolean isStatsEnabled() {
        return statsEnabled;
    }

    public String getTableName() {
        return tableName;
    }

    public Set<Authorizations> getAuths() {
        return auths;
    }

    public Query getQuery() {
        return query;
    }

    public ScannerBase.ConsistencyLevel getLevel() {
        return level;
    }

    public Map<String,String> getHints() {
        return hints;
    }
}
