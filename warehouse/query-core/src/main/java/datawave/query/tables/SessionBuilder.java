package datawave.query.tables;

import static org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.security.Authorizations;

import com.google.common.base.Preconditions;

import datawave.microservice.query.Query;

/**
 * Base builder for {@link ScannerSession} and associated classes
 */
public abstract class SessionBuilder<B> {

    protected final AccumuloClient client;
    protected String tableName;
    protected Set<Authorizations> authorizations;

    protected boolean statsEnabled = false;

    private static final int DEFAULT_RESOURCE_QUEUE_SIZE = 100;
    private static final int DEFAULT_RESULT_QUEUE_SIZE = 1_000;

    protected int resourceQueueSize = DEFAULT_RESOURCE_QUEUE_SIZE;
    protected int resultQueueSize = DEFAULT_RESULT_QUEUE_SIZE;

    protected Query query;

    protected ConsistencyLevel consistencyLevel;

    private static final String SCAN_TYPE_KEY = "scan_type";
    private static final String PRIORITY_KEY = "priority";

    protected final Map<String,String> executionHints = new HashMap<>();

    /**
     * Constructor that enforces a non-null AccumuloClient argument
     *
     * @param client
     *            the AccumuloClient
     */
    protected SessionBuilder(AccumuloClient client) {
        Preconditions.checkNotNull(client, "AccumuloClient must be set");
        this.client = client;
    }

    /**
     * Create the ScannerSession implementation
     *
     * @return the scanner session
     */
    abstract ScannerSession build();

    /**
     * Forcing children to implement this method allows child-specific methods to be picked up
     *
     * @return this builder
     */
    protected abstract B self();

    /**
     * Set the table name for the scanner session
     *
     * @param tableName
     *            the table name
     * @return this builder
     */
    public B setTableName(String tableName) {
        this.tableName = tableName;
        return self();
    }

    /**
     * Set the authorization set for the scanner session
     *
     * @param authorizations
     *            the set of authorizations
     * @return this builder
     */
    public B setAuthorizations(Set<Authorizations> authorizations) {
        this.authorizations = authorizations;
        return self();
    }

    /**
     * Set whether {@link datawave.query.tables.stats.ScanSessionStats} will be gathered
     *
     * @param statsEnabled
     *            flag for gathering stats
     * @return this builder
     */
    public B setStatsEnabled(boolean statsEnabled) {
        this.statsEnabled = statsEnabled;
        return self();
    }

    /**
     * Set the internal queue size for the {@link ResourceQueue}
     *
     * @param resourceQueueSize
     *            the size of the resource queue
     * @return this builder
     */
    public B setResourceQueueSize(int resourceQueueSize) {
        this.resourceQueueSize = resourceQueueSize;
        return self();
    }

    /**
     * Set the size of the result queue.
     * <p>
     * <b>NOTE:</b> some implementations are not actually threaded, so it is possible to enter a deadlock state if the number of elements exceeds the size of
     * the result queue. See {@link RangeStreamScanner}
     *
     * @param resultQueueSize
     *            the size of the result queue
     * @return this builder
     */
    public B setResultQueueSize(int resultQueueSize) {
        this.resultQueueSize = resultQueueSize;
        return self();
    }

    /**
     * Set the Query
     *
     * @param query
     *            the query
     * @return this builder
     */
    public B setQuery(Query query) {
        this.query = query;
        return self();
    }

    /**
     * Set the {@link ConsistencyLevel} via an instance.
     * <p>
     * See {@link ConsistencyLevel#values()} for permitted values.
     * <ul>
     * <li>IMMEDIATE routes the scan to a tablet server</li>
     * <li>EVENTUAL routes the scan to a scan server</li>
     * </ul>
     *
     * @param consistencyLevel
     *            the consistency level of the scanner
     * @return this builder
     */
    public B setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
        return self();
    }

    /**
     * Set the {@link ConsistencyLevel} via a string.
     * <p>
     * See {@link ConsistencyLevel#values()} for permitted values.
     * <ul>
     * <li>IMMEDIATE routes the scan to a tablet server</li>
     * <li>EVENTUAL routes the scan to a scan server</li>
     * </ul>
     *
     * @param consistencyLevel
     *            the consistency level of the scanner
     * @return this builder
     */
    public B setConsistencyLevel(String consistencyLevel) {
        this.consistencyLevel = ConsistencyLevel.valueOf(consistencyLevel);
        return self();
    }

    /**
     * Set the scan type on the execution hints. This is used to route the scan to specific executor pools.
     * <p>
     * See {@link ScannerBase#setExecutionHints(Map)} for details.
     *
     * @param scanType
     *            the scan type
     * @return this builder
     */
    public B setScanType(String scanType) {
        executionHints.put(SCAN_TYPE_KEY, scanType);
        return self();
    }

    /**
     * Set a scan priority to the execution hints. This allows a scan to be executed given a relative priority.
     * <p>
     * See {@link ScannerBase#setExecutionHints(Map)} for details.
     *
     * @param scanPriority
     *            an integer priority
     * @return this builder
     */
    public B setScanPriority(int scanPriority) {
        executionHints.put(PRIORITY_KEY, Integer.toString(scanPriority));
        return self();
    }

    /**
     * Get the table name
     *
     * @return the table name
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Get the set of authorizations
     *
     * @return the authorizations
     */
    public Set<Authorizations> getAuthorizations() {
        return authorizations;
    }

    /**
     * Get the stats enabled flag
     *
     * @return the stats enabled flag
     */
    public boolean isStatsEnabled() {
        return statsEnabled;
    }

    /**
     * Get the size of the resource queue
     *
     * @return the size of the resource queue
     */
    public int getResourceQueueSize() {
        return resourceQueueSize;
    }

    /**
     * Get the size of the result queue
     *
     * @return the size of the result queue
     */
    public int getResultQueueSize() {
        return resultQueueSize;
    }

    /**
     * Get the Query
     *
     * @return the query
     */
    public Query getQuery() {
        return query;
    }

    /**
     * Get the consistency level
     *
     * @return the consistency level
     */
    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    /**
     * Get the map of execution hints
     *
     * @return the execution hints
     */
    public Map<String,String> getExecutionHints() {
        return executionHints;
    }
}
