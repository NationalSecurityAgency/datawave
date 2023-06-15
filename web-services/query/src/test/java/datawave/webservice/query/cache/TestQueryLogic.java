package datawave.webservice.query.cache;

import java.util.Set;

import datawave.webservice.common.connection.AccumuloConnectionFactory.Priority;
import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.logic.BaseQueryLogic;
import datawave.webservice.query.logic.QueryLogicTransformer;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;

public class TestQueryLogic extends BaseQueryLogic<Object> {

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
