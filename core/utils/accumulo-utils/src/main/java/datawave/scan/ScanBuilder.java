package datawave.scan;

import static org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.security.Authorizations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Builder that contains the core logic for building implementations of a {@link ScannerBase}, specifically {@link Scanner} or {@link BatchScanner}
 * <p>
 * The builder <b>must</b> specify the AccumuloClient, the table name, and the authorizations
 * <p>
 * The builder may specify a consistency level that determines if the scan executes in a tablet server or a scan server.
 * <p>
 * The builder may specify a scan type to determine if the scan executes in a specific executor pool.
 * <p>
 * The builder may specify a scan priority to determine if the scan is prioritized ahead of or behind other scans.
 *
 * @param <B>
 *            the specific builder type
 */
public abstract class ScanBuilder<B> {

    private static final Logger log = LoggerFactory.getLogger(ScanBuilder.class);

    // required variables
    protected final AccumuloClient client;
    protected String tableName;
    protected Authorizations authorizations;

    // optional variables
    protected ScannerBase.ConsistencyLevel consistencyLevel;
    protected static final String SCAN_TYPE_KEY = "scan_type";
    protected static final String PRIORITY_KEY = "priority";
    protected final Map<String,String> executionHints = new HashMap<>();

    protected ScanBuilder(AccumuloClient client) {
        Preconditions.checkNotNull(client, "AccumuloClient must be set");
        this.client = client;
    }

    /**
     * Create the scanner implementation
     *
     * @return the scanner
     */
    abstract ScannerBase build();

    /**
     * Forcing children to implement this method allows child-specific methods to be picked up, in addition to self actualization and satisfying a yearning for
     * mines.
     *
     * @return this builder
     */
    protected abstract B self();

    /**
     * Set the table name for the scanner
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
     * Set the {@link Authorizations} for the scanner
     *
     * @param authorizations
     *            the authorizations
     * @return this builder
     */
    public B setAuthorizations(Authorizations authorizations) {
        this.authorizations = authorizations;
        return self();
    }

    /**
     * Set the {@link ScannerBase.ConsistencyLevel} via an instance.
     * <p>
     * See {@link ScannerBase.ConsistencyLevel#values()} for permitted values.
     * <ul>
     * <li>IMMEDIATE routes the scan to a tablet server</li>
     * <li>EVENTUAL routes the scan to a scan server</li>
     * </ul>
     *
     * @param consistencyLevel
     *            the consistency level of the scanner
     * @return this builder
     */
    public B setConsistencyLevel(ScannerBase.ConsistencyLevel consistencyLevel) {
        if (consistencyLevel == null) {
            log.warn("Null consistencyLevel");
            throw new IllegalArgumentException("consistencyLevel cannot be null");
        }
        this.consistencyLevel = consistencyLevel;
        return self();
    }

    /**
     * Set the {@link ScannerBase.ConsistencyLevel} via a string.
     * <p>
     * See {@link ScannerBase.ConsistencyLevel#values()} for permitted values.
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
        if (consistencyLevel == null) {
            log.warn("Null consistencyLevel");
            throw new IllegalArgumentException("consistencyLevel cannot be null");
        }
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
        if (scanType == null) {
            log.warn("Null scanType");
            throw new IllegalArgumentException("scanType cannot be null");
        }
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
        if (scanPriority < 0) {
            log.warn("Negative scanPriority");
            throw new IllegalArgumentException("scanPriority cannot be negative");
        }
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
     * Get the authorizations
     *
     * @return the authorizations
     */
    public Authorizations getAuthorizations() {
        return authorizations;
    }

    /**
     * Get the consistency level
     *
     * @return the consistency level
     */
    public ScannerBase.ConsistencyLevel getConsistencyLevel() {
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
