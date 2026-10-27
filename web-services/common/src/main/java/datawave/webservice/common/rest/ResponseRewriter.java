package datawave.webservice.common.rest;

import javax.ws.rs.core.Response;

/**
 * A generic interface for transforming JAX-RS {@link Response} objects before they are returned to the client. Implementations can be provided externally to
 * add behavior such as response redaction, or enrichment. We deliberately choose to modify the JAX-RS objects in this case so that we can modify response
 * headers (e.g., Content-Type) as necessary. This is intentionally different from the approach taken in <code>datawave.core.query.logic.ResponseEnricher</code>
 * despite structural similarities.
 *
 * <p>
 * Implementations are expected to be thread-safe, as a single instance will typically be shared across all requests.
 * </p>
 */
public interface ResponseRewriter {
    /**
     * Transform the given response. Implementations may modify, replace, or wrap the response. If the rewriter does not need to modify the response, it should
     * return it unchanged.
     *
     * @param response
     *            the original response, must not be null
     * @param context
     *            information about the request context (principal, DN, proxy servers, etc.)
     * @return the transformed response, must not be null
     */
    Response rewriteResponse(Response response, ResponseRewriterContext context);
}
