package datawave.webservice.query.logic.filtered;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.log4j.Logger;

import datawave.microservice.authorization.util.AuthorizationsUtil;
import datawave.security.authorization.UserOperations;
import datawave.webservice.query.Query;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.iterator.DatawaveTransformIterator;
import datawave.webservice.query.logic.DelegatingQueryLogic;
import datawave.webservice.query.logic.QueryLogic;

/**
 * A filtered query logic will only actually execute the delegate query logic if the filter passes. Otherwise this will do nothing and return no results.
 */
public class FilteredQueryLogic extends DelegatingQueryLogic implements QueryLogic<Object> {

    public static final Logger log = Logger.getLogger(FilteredQueryLogic.class);

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

    public boolean canRunQuery(Query settings, Set<Authorizations> runtimeQueryAuthorizations) {
        if (!filtered) {
            if (!filter.canRunQuery(settings, runtimeQueryAuthorizations)) {
                filtered = true;
            }
        }
        return !isFiltered();
    }

    public boolean isFiltered() {
        if (log.isDebugEnabled()) {
            if (filtered) {
                log.debug("Filter " + filter + " blocking query " + super.getLogicName());
            } else {
                log.debug("Passing through filter " + filter + " for query " + super.getLogicName());
            }
        }
        return filtered || (getDelegate() instanceof FilteredQueryLogic && ((FilteredQueryLogic) getDelegate()).isFiltered());
    }

    @Override
    public String getPlan(AccumuloClient connection, Query settings, Set<Authorizations> runtimeQueryAuthorizations, boolean expandFields, boolean expandValues)
                    throws Exception {
        if (canRunQuery(settings, runtimeQueryAuthorizations)) {
            return super.getPlan(connection, settings, runtimeQueryAuthorizations, expandFields, expandValues);
        } else {
            return "";
        }
    }

    @Override
    public GenericQueryConfiguration initialize(AccumuloClient connection, Query settings, Set<Authorizations> runtimeQueryAuthorizations) throws Exception {
        if (canRunQuery(settings, runtimeQueryAuthorizations)) {
            return super.initialize(connection, settings, runtimeQueryAuthorizations);
        } else {
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
        if (!isFiltered()) {
            super.setupQuery(configuration);
        }
    }

    @Override
    public Iterator<Object> iterator() {
        if (!isFiltered()) {
            return super.iterator();
        } else {
            return Collections.emptyIterator();
        }
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return new FilteredQueryLogic(this);
    }

    @Override
    public TransformIterator getTransformIterator(Query settings) {
        if (!isFiltered()) {
            return super.getTransformIterator(settings);
        } else {
            return new DatawaveTransformIterator(iterator());
        }
    }

    @Override
    public UserOperations getUserOperations(Query settings) {
        Set<Authorizations> auths = AuthorizationsUtil.buildAuthorizations(null);
        if (settings != null && settings.getQueryAuthorizations() != null) {
            auths = AuthorizationsUtil.buildAuthorizations(Collections.singleton(AuthorizationsUtil.splitAuths(settings.getQueryAuthorizations())));
        }
        if (canRunQuery(settings, auths)) {
            return super.getUserOperations(settings);
        } else {
            return null;
        }
    }

}
