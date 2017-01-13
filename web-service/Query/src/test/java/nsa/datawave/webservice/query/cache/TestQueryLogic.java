package nsa.datawave.webservice.query.cache;

import java.util.Set;

import nsa.datawave.webservice.common.connection.AccumuloConnectionFactory.Priority;
import nsa.datawave.webservice.query.Query;
import nsa.datawave.webservice.query.configuration.GenericQueryConfiguration;
import nsa.datawave.webservice.query.logic.BaseQueryLogic;
import nsa.datawave.webservice.query.logic.QueryLogicTransformer;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;

public class TestQueryLogic extends BaseQueryLogic<Object> {
    
    @Override
    public GenericQueryConfiguration initialize(Connector connection, Query settings, Set<Authorizations> runtimeQueryAuthorizations) throws Exception {
        return null;
    }
    
    @Override
    public void setupQuery(GenericQueryConfiguration configuration) throws Exception {}
    
    @Override
    public Priority getConnectionPriority() {
        return Priority.NORMAL;
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
