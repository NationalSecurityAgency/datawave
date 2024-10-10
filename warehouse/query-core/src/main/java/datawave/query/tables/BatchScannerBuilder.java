package datawave.query.tables;

import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import datawave.microservice.query.Query;
import datawave.query.iterator.QueryInformationIterator;
import datawave.query.util.QueryInformation;
import datawave.security.util.ScannerHelper;

/**
 * Builder for an Accumulo {@link BatchScanner}
 */
public class BatchScannerBuilder {

    private static final Logger log = LoggerFactory.getLogger(BatchScannerBuilder.class);

    private static final int DEFAULT_NUM_THREADS = 8;
    private int numThreads = DEFAULT_NUM_THREADS;

    private String tableName;
    private Set<Authorizations> auths;
    private Query query;
    private ScannerBase.ConsistencyLevel level;
    private Map<String,String> hints;

    private final AccumuloClient client;

    public BatchScannerBuilder(AccumuloClient client) {
        Preconditions.checkNotNull(client);
        this.client = client;
    }

    /**
     * Required parameter
     *
     * @param tableName
     *            the table name
     * @return the builder
     */
    public BatchScannerBuilder withTableName(String tableName) {
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
    public BatchScannerBuilder withAuths(Set<Authorizations> auths) {
        this.auths = auths;
        return this;
    }

    /**
     * Required parameter
     *
     * @param numThreads
     *            the number of threads
     * @return the builder
     */
    public BatchScannerBuilder withNumThreads(int numThreads) {
        this.numThreads = numThreads;
        return this;
    }

    /**
     * Optional parameter
     *
     * @param query
     *            a {@link Query} instance
     * @return the builder
     */
    public BatchScannerBuilder withQuery(Query query) {
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
    public BatchScannerBuilder withConsistencyLevel(ScannerBase.ConsistencyLevel level) {
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
    public BatchScannerBuilder withExecutionHints(Map<String,String> hints) {
        this.hints = hints;
        return this;
    }

    /**
     * Build the {@link BatchScanner}, setting any optional configs if necessary
     *
     * @return a Scanner
     */
    public BatchScanner build() {
        Preconditions.checkNotNull(tableName, "TableName must be set");
        Preconditions.checkNotNull(auths, "Authorizations must be set");

        try {
            BatchScanner scanner = ScannerHelper.createBatchScanner(client, tableName, auths, numThreads);

            if (query != null) {
                QueryInformation info = new QueryInformation(query, query.getQuery());
                IteratorSetting setting = new IteratorSetting(Integer.MAX_VALUE, QueryInformationIterator.class, info.toMap());
                scanner.addScanIterator(setting);
            }

            if (level != null) {
                scanner.setConsistencyLevel(level);
            }

            if (hints != null) {
                scanner.setExecutionHints(hints);
            }

            return scanner;
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public int getNumThreads() {
        return numThreads;
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
