package datawave.webservice.query.util;

import datawave.webservice.query.Query;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;

/**
 * Abstract implementation of criteria used for UUID lookup queries
 */
public abstract class AbstractUUIDLookupCriteria {
    private boolean allEventLookup;
    private boolean contentLookup;
    private HttpHeaders headersForStreamedResponse;
    private MultivaluedMap<String,String> queryParameters;

    /**
     * Constructor
     *
     * @param settings
     *            criteria for an existing query
     */
    public AbstractUUIDLookupCriteria(final Query settings) {
        if (null != settings) {
            this.queryParameters = new MultivaluedMapImpl<>();
            this.queryParameters.putAll(settings.toMap());
        }
    }

    public AbstractUUIDLookupCriteria(final MultivaluedMap<String,String> queryParameters) {
        this.queryParameters = queryParameters;
    }

    /**
     * Returns a non-null set of HTTP headers if a streamed response is required. Otherwise, a paged result is required.
     *
     * @return HTTP headers required for a streamed response, or null if paging is required
     */
    public HttpHeaders getStreamingOutputHeaders() {
        return this.headersForStreamedResponse;
    }

    public boolean isAllEventLookup() {
        return this.allEventLookup;
    }

    public boolean isContentLookup() {
        return contentLookup;
    }

    public void setAllEventLookup(boolean allEventLookup) {
        this.allEventLookup = allEventLookup;
    }

    public void setContentLookup(boolean contentLookup) {
        this.contentLookup = contentLookup;
    }

    /**
     * If a streamed response is required, sets the HTTP headers used to invoke the QueryExecutor.execute(..) endpoint.
     *
     * @param headers
     *            HTTP headers required for a streamed response
     */
    public void setStreamingOutputHeaders(final HttpHeaders headers) {
        this.headersForStreamedResponse = headers;
    }

    public MultivaluedMap<String,String> getQueryParameters() {
        return queryParameters;
    }

    public abstract String getRawQueryString();
}
