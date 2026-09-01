package datawave.webservice.annotation;

import javax.ws.rs.core.Response;

import datawave.core.query.logic.ResponseRewriter;
import datawave.core.query.logic.ResponseRewriterContext;

/**
 * A test implementation of {@link ResponseRewriter} that adds a marker header to verify that the rewriter was invoked. This is used in unit tests to verify
 * that the {@link AnnotationManagerBean} correctly delegates to the rewriter.
 */
public class MockResponseRewriter implements ResponseRewriter {

    public static final String MOCK_REWRITER_MARKER_HEADER = "X-Mock-ResponseRewriter-Called";
    public static final String MOCK_REWRITER_MARKER_VALUE = "true";

    @Override
    public Response rewriteResponse(Response response, ResponseRewriterContext context) {
        return Response.fromResponse(response).header(MOCK_REWRITER_MARKER_HEADER, MOCK_REWRITER_MARKER_VALUE).build();
    }
}
