package datawave.query.tables.chained;

import java.util.Set;
import java.util.TreeSet;

import datawave.query.QueryParameters;
import datawave.query.tables.chained.strategy.ChainStrategy;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.logic.BaseQueryLogic;
import datawave.webservice.query.logic.QueryLogic;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

/**
 * <p>
 * A QueryLogic wrapper which encapsulates two QueryLogic classes for the purpose of performing a query using the output from a previous query.
 * </p>
 * 
 * <p>
 * Example:
 * 
 * <p>
 * <code>Query -&gt; QueryLogic1 -&gt; Results1 -&gt; Query2 -&gt; QueryLogic2 -&gt; Results2</code>
 * </p>
 * <p>
 * where the user would submit <code>Query</code> and receive <code>Results2</code> transparently.
 * 
 * 
 */
public abstract class ChainedQueryTable<T1,T2> extends BaseQueryLogic<T2> {
    protected ChainStrategy<T1,T2> chainStrategy = null;
    protected QueryLogic<T1> logic1 = null;
    protected QueryLogic<T2> logic2 = null;
    private Logger log = Logger.getLogger(ChainedQueryTable.class);
    
    public ChainedQueryTable() {
        super();
    }
    
    public ChainedQueryTable(ChainedQueryTable<T1,T2> other) {
        super(other);
        this.setChainStrategy(other.getChainStrategy());
        this.setLogic1(other.getLogic1());
        this.setLogic2(other.getLogic2());
    }
    
    public QueryLogic<T1> getLogic1() {
        return logic1;
    }
    
    public void setLogic1(QueryLogic<T1> logic1) {
        this.logic1 = logic1;
    }
    
    public QueryLogic<T2> getLogic2() {
        return logic2;
    }
    
    public void setLogic2(QueryLogic<T2> logic2) {
        this.logic2 = logic2;
    }
    
    public ChainStrategy<T1,T2> getChainStrategy() {
        return this.chainStrategy;
    }
    
    public void setChainStrategy(ChainStrategy<T1,T2> strategy) {
        this.chainStrategy = strategy;
    }
    
    @Override
    public AccumuloConnectionFactory.Priority getConnectionPriority() {
        return AccumuloConnectionFactory.Priority.NORMAL;
    }
    
    @Override
    public GenericQueryConfiguration initialize(Connector connection, Query settings, Set<Authorizations> runtimeQueryAuthorizations) throws Exception {
        
        ChainedQueryConfiguration config = new ChainedQueryConfiguration();
        
        if (log.isDebugEnabled()) {
            log.debug("Max Results: " + this.getMaxResults());
        }
        return config;
    }
    
    @Override
    public Set<String> getOptionalQueryParameters() {
        Set<String> params = new TreeSet<>();
        params.addAll(logic1.getOptionalQueryParameters());
        params.addAll(logic2.getOptionalQueryParameters());
        return params;
    }
    
    @Override
    public Set<String> getRequiredQueryParameters() {
        Set<String> params = new TreeSet<>();
        params.addAll(logic1.getRequiredQueryParameters());
        params.addAll(logic2.getRequiredQueryParameters());
        return params;
    }
    
}
