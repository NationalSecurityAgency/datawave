package datawave.webservice.query.util;

import org.springframework.util.MultiValueMap;

import datawave.microservice.query.Query;

/**
 * Lookup criteria for paging through content results
 */
public class NextContentCriteria extends AbstractUUIDLookupCriteria {
    private final String queryId;

    public NextContentCriteria(final String queryId, MultiValueMap<String,String> queryParameters) {
        super(queryParameters);
        this.queryId = queryId;
    }

    public NextContentCriteria(final String queryId, final Query settings) {
        super(settings);
        this.queryId = queryId;
    }

    @Override
    public String getRawQueryString() {
        return this.queryId;
    }
}
