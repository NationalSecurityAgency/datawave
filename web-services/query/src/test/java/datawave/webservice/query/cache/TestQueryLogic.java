package datawave.webservice.query.cache;

import datawave.services.common.connection.AccumuloConnectionFactory;
import datawave.services.query.configuration.GenericQueryConfiguration;
import datawave.services.query.logic.BaseQueryLogic;
import datawave.services.query.logic.QueryLogicTransformer;
import datawave.webservice.query.Query;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Set;

public class TestQueryLogic extends BaseQueryLogic<Object> {
    
    @Override
    public GenericQueryConfiguration initialize(Connector connection, Query settings, Set<Authorizations> runtimeQueryAuthorizations) throws Exception {
        return null;
    }
    
    @Override
    public void setupQuery(GenericQueryConfiguration configuration) throws Exception {}
    
    @Override
    public String getPlan(Connector connection, Query settings, Set<Authorizations> runtimeQueryAuthorizations, boolean expandFields, boolean expandValues)
                    throws Exception {
        return "";
    }
    
    @Override
    public AccumuloConnectionFactory.Priority getConnectionPriority() {
        return AccumuloConnectionFactory.Priority.NORMAL;
    }
    
    @Override
    public QueryLogicTransformer getTransformer(Query settings) {
        return null;
    }
    
    @Override
    public Object clone() throws CloneNotSupportedException {
        return null;
    }
    
    @Override
    public Set<String> getOptionalQueryParameters() {
        return null;
    }
    
    @Override
    public Set<String> getRequiredQueryParameters() {
        return null;
    }
    
    @Override
    public Set<String> getExampleQueries() {
        return null;
    }
    
}
