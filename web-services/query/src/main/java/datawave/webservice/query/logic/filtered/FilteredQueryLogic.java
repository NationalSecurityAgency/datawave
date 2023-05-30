package datawave.webservice.query.logic.filtered;

import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.logic.DelegatingQueryLogic;
import datawave.webservice.query.logic.QueryLogic;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

/**
 * A filtered query logic will only actually execute the delegate query logic if the filter passes. Otherwise this will do nothing and return no results.
 */
public class FilteredQueryLogic extends DelegatingQueryLogic implements QueryLogic<Object> {
    
    private QueryLogicFilter filter;
    
    private boolean filtered = false;
    
    public FilteredQueryLogic() {}
    
    public FilteredQueryLogic(FilteredQueryLogic other) throws CloneNotSupportedException {
        super(other);
        this.filter = other.filter;
        this.filtered = other.filtered;
    }
    
    public QueryLogicFilter getFilter() {
        return filter;
    }
    
    public void setFilter(QueryLogicFilter filter) {
        this.filter = filter;
    }
    
    public interface QueryLogicFilter {
        boolean canRunQuery(Query settings, Set<Authorizations> auths);
    }
    
    @Override
    public String getPlan(AccumuloClient connection, Query settings, Set<Authorizations> runtimeQueryAuthorizations, boolean expandFields, boolean expandValues)
                    throws Exception {
        if (!filtered && filter.canRunQuery(settings, runtimeQueryAuthorizations)) {
            return super.getPlan(connection, settings, runtimeQueryAuthorizations, expandFields, expandValues);
        } else {
            filtered = true;
            return "";
        }
    }
    
    @Override
    public GenericQueryConfiguration initialize(AccumuloClient connection, Query settings, Set<Authorizations> runtimeQueryAuthorizations) throws Exception {
        if (!filtered && filter.canRunQuery(settings, runtimeQueryAuthorizations)) {
            return super.initialize(connection, settings, runtimeQueryAuthorizations);
        } else {
            filtered = true;
            GenericQueryConfiguration config = new GenericQueryConfiguration() {};
            config.setClient(connection);
            config.setQueryString("");
            config.setAuthorizations(runtimeQueryAuthorizations);
            config.setBeginDate(settings.getBeginDate());
            config.setEndDate(settings.getEndDate());
            return config;
        }
    }
    
    @Override
    public void setupQuery(GenericQueryConfiguration configuration) throws Exception {
        if (!filtered) {
            super.setupQuery(configuration);
        }
    }
    
    @Override
    public Iterator<Object> iterator() {
        if (!filtered) {
            return super.iterator();
        } else {
            return Collections.emptyIterator();
        }
    }
    
    @Override
    public Object clone() throws CloneNotSupportedException {
        return new FilteredQueryLogic(this);
    }
}
