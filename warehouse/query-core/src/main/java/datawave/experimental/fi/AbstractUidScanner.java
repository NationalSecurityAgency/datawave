package datawave.experimental.fi;

import datawave.query.predicate.TimeFilter;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;

import datawave.experimental.util.FieldIndexRangeBuilder;

public abstract class AbstractUidScanner implements UidScanner {

    protected AccumuloClient client;
    protected Authorizations auths;
    protected String tableName;
    protected String scanId;
    protected boolean logStats;
    protected TimeFilter timeFilter = TimeFilter.alwaysTrue();

    protected FieldIndexRangeBuilder rangeBuilder = new FieldIndexRangeBuilder();

    protected AbstractUidScanner(AccumuloClient client, Authorizations auths, String tableName, String scanId) {
        this.client = client;
        this.auths = auths;
        this.tableName = tableName;
        this.scanId = scanId;
    }

    @Override
    public void withTimeFilter(TimeFilter timeFilter) {
        this.timeFilter = timeFilter;
    }
}
