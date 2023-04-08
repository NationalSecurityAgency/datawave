package datawave.query.tables.chained.iterators;

import datawave.core.query.logic.QueryLogic;
import datawave.webservice.query.Query;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Iterator;
import java.util.Set;

/**
 * Abstract class that encompasses the members necessary to run a ChainedQuery. Any implementation should need two {@link QueryLogic}'s, the original query the
 * user entered, the user's auths, and an {@link AccumuloClient}
 * 
 * 
 * 
 * @param <T1>
 *            Type of the initial {@link QueryLogic}
 * @param <T2>
 *            Type of the latter {@link QueryLogic}
 */
public abstract class ChainedQueryIterator<T1,T2> implements Iterator<T2> {
    protected Iterator<T1> initialQueryResults = null;
    protected QueryLogic<T2> latterQueryLogic = null;
    protected Iterator<T2> latterQueryResults = null;
    protected Query initialQuery = null;
    protected AccumuloClient client = null;
    protected Set<Authorizations> auths = null;
    
    public AccumuloClient getClient() {
        return client;
    }
    
    public void setClient(AccumuloClient client) {
        this.client = client;
    }
    
    public Set<Authorizations> getAuths() {
        return auths;
    }
    
    public void setAuths(Set<Authorizations> auths) {
        this.auths = auths;
    }
    
    public void setInitialQueryResults(Iterator<T1> initialQueryResults) {
        this.initialQueryResults = initialQueryResults;
    }
    
    public void setLatterQueryLogic(QueryLogic<T2> latterQueryLogic) {
        this.latterQueryLogic = latterQueryLogic;
    }
    
    public void setInitialQuery(Query query) {
        this.initialQuery = query;
    }
    
    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove() is not supported by this Iterator.");
    }
}
