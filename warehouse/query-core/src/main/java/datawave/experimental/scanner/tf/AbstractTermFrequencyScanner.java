package datawave.experimental.scanner.tf;

import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;

/**
 * Abstract implementation of a {@link TermFrequencyScanner} with a default constructor
 */
public abstract class AbstractTermFrequencyScanner implements TermFrequencyScanner {

    protected AccumuloClient client;
    protected Authorizations auths;
    protected String tableName;
    protected String scanId;
    protected Set<String> termFrequencyFields;
    protected boolean logStats;

    protected AbstractTermFrequencyScanner(AccumuloClient client, Authorizations auths, String tableName, String scanId) {
        this.client = client;
        this.auths = auths;
        this.tableName = tableName;
        this.scanId = scanId;
    }

    @Override
    public void setTermFrequencyFields(Set<String> termFrequencyFields) {
        this.termFrequencyFields = termFrequencyFields;
    }

    @Override
    public void setLogStats(boolean logStats) {
        this.logStats = logStats;
    }
}
