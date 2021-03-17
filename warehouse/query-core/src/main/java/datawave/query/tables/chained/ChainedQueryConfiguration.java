package datawave.query.tables.chained;

import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.GenericQueryConfiguration;

public class ChainedQueryConfiguration extends GenericQueryConfiguration {
    
    private static final long serialVersionUID = 444695916607959066L;
    
    // for backward compatability
    public void setQueryImpl(Query query) {
        setQuery(query);
    }
    
    // for backward capatability
    public Query getQueryImpl() {
        return getQuery();
    }
}
