package datawave.webservice.query.logic;

import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Ignore;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.microservice.query.Query;

@Ignore
public class TestQueryLogic<T> extends BaseQueryLogic<T> {

    @Override
    public GenericQueryConfiguration initialize(AccumuloClient client, Query settings, Set<Authorizations> runtimeQueryAuthorizations) throws Exception {
        return null;
    }

    @Override
    public void setupQuery(GenericQueryConfiguration configuration) throws Exception {}

    @Override
    public String getPlan(AccumuloClient client, Query settings, Set<Authorizations> runtimeQueryAuthorizations, boolean expandFields, boolean expandValues)
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
