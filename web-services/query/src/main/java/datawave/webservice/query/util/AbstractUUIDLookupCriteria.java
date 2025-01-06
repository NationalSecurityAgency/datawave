package datawave.webservice.query.util;

import javax.ws.rs.core.HttpHeaders;

import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import datawave.microservice.query.Query;

/**
 * Abstract implementation of criteria used for UUID lookup queries
 */
public abstract class AbstractUUIDLookupCriteria {
    private boolean allEventLookup;
    private boolean contentLookup;
    private HttpHeaders headersForStreamedResponse;
    private MultiValueMap<String,String> queryParameters;

    private String uuidTypeContext;

    /**
     * Constructor
     *
     * @param settings
     *            criteria for an existing query
     */
    public AbstractUUIDLookupCriteria(final Query settings) {
        if (null != settings) {
            this.queryParameters = new LinkedMultiValueMap<>();
            this.queryParameters.putAll(settings.toMap());
        }
    }

    public AbstractUUIDLookupCriteria(final MultiValueMap<String,String> queryParameters) {
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

    public MultiValueMap<String,String> getQueryParameters() {
        return queryParameters;
    }

    public abstract String getRawQueryString();

    /**
     * returns a context for the lookup request if any was specfied in the request. The lookup context is used to obtain alternate query logics for the lookup
     * requests to use. This can be used to modify the types of responses the query operations provide (e.g., plaintext responses.)
     *
     * @return
     */
    public String getUUIDTypeContext() {
        return uuidTypeContext;
    }

    public void setUUIDTypeContext(String uuidTypeContext) {
        this.uuidTypeContext = uuidTypeContext;
    }

}
