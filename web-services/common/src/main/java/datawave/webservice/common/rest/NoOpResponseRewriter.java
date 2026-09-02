package datawave.webservice.common.rest;

import javax.ws.rs.core.Response;

/**
 * A no-operation implementation of {@link ResponseRewriter} that returns the response unchanged. This serves as the default configuration when response
 * rewriting is not needed, making the absence of rewriting an explicit choice rather than a silent fallback.
 */
public class NoOpResponseRewriter implements ResponseRewriter {

    @Override
    public Response rewriteResponse(Response response, ResponseRewriterContext context) {
        return response;
    }
}
