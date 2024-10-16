package datawave.query.tables;

import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;

import com.google.common.base.Preconditions;

import datawave.microservice.query.Query;
import datawave.query.iterator.QueryInformationIterator;
import datawave.query.util.QueryInformation;
import datawave.security.util.ScannerHelper;

/**
 * Builder for an Accumulo {@link Scanner}
 */
public class ScannerBuilder {

    private String tableName;
    private Set<Authorizations> auths;
    private Query query;
    private ConsistencyLevel level;
    private Map<String,String> hints;

    private final AccumuloClient client;

    /**
     * Instantiates the builder using the provided accumulo client
     *
     * @param client
     *            the accumulo client
     */
    public ScannerBuilder(AccumuloClient client) {
        Preconditions.checkNotNull(client, "AccumuloClient must be set");
        this.client = client;
    }

    /**
     * Required parameter
     *
     * @param tableName
     *            the table name
     * @return the builder
     */
    public ScannerBuilder withTableName(String tableName) {
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
    public ScannerBuilder withAuths(Set<Authorizations> auths) {
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
    public ScannerBuilder withQuery(Query query) {
        this.query = query;
        return this;
    }

    /**
     * Optional parameter
     *
     * @param level
     *            the {@link ConsistencyLevel}
     * @return the builder
     */
    public ScannerBuilder withConsistencyLevel(ConsistencyLevel level) {
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
    public ScannerBuilder withExecutionHints(Map<String,String> hints) {
        this.hints = hints;
        return this;
    }

    /**
     * Build the {@link Scanner}, setting any optional configs if necessary
     *
     * @return a Scanner
     */
    public Scanner build() {
        Preconditions.checkNotNull(tableName, "TableName must be set");
        Preconditions.checkNotNull(auths, "Authorizations must be set");

        try {
            Scanner scanner = ScannerHelper.createScanner(client, tableName, auths);

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

    public String getTableName() {
        return tableName;
    }

    public Set<Authorizations> getAuths() {
        return auths;
    }

    public Query getQuery() {
        return query;
    }

    public ConsistencyLevel getLevel() {
        return level;
    }

    public Map<String,String> getHints() {
        return hints;
    }
}
