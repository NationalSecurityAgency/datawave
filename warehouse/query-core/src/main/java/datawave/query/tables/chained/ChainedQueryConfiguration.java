package datawave.query.tables.chained;

import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.GenericQueryConfiguration;

public class ChainedQueryConfiguration extends GenericQueryConfiguration {

    private static final long serialVersionUID = 444695916607959066L;
    private Query query = null;

    public void setQueryImpl(Query query) {
        this.query = query;
    }

    public Query getQueryImpl() {
        return this.query;
    }
}
