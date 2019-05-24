package datawave.webservice.query.logic;

import java.util.Set;

import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.GenericQueryConfiguration;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Ignore;

@Ignore
public class TestQueryLogic<T> extends BaseQueryLogic<T> {
    
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
        return AccumuloConnectionFactory.Priority.LOW;
    }
    
    @Override
    public QueryLogicTransformer getTransformer(Query settings) {
        return null;
    }
    
    @Override
    public Object clone() throws CloneNotSupportedException {
        TestQueryLogic<Object> other = new TestQueryLogic<>();
        other.setTableName(this.getTableName());
        other.setMaxResults(this.getMaxResults());
        other.setMaxWork(this.getMaxWork());
        return other;
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
        // TODO Auto-generated method stub
        return null;
    }
    
}
