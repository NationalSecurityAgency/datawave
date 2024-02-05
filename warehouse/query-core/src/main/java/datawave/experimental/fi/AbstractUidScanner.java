package datawave.experimental.fi;

import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl3.parser.ParserVisitor;

import datawave.experimental.util.FieldIndexRangeBuilder;
import datawave.query.predicate.TimeFilter;

public abstract class AbstractUidScanner extends ParserVisitor implements UidScanner {

    protected AccumuloClient client;
    protected Authorizations auths;
    protected String tableName;
    protected String scanId;
    protected boolean logStats;
    protected TimeFilter timeFilter = TimeFilter.alwaysTrue();
    protected Set<String> datatypeFilter = null;

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

    @Override
    public void withDatatypeFilter(Set<String> datatypeFilter) {
        this.datatypeFilter = datatypeFilter;
    }
}
