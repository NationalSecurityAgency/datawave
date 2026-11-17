package datawave.webservice.annotation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.webservice.common.rest.NoOpResponseRewriter;
import datawave.webservice.common.rest.ResponseRewriter;
import datawave.webservice.common.rest.ResponseRewriterContext;

/**
 * Unit tests verifying that the {@link ResponseRewriter} integration in {@link AnnotationManagerBean} correctly applies the rewriter to success responses and
 * bypasses it for error responses.
 */
public class AnnotationManagerBeanResponseRewriterTest {

    private AnnotationManagerBean bean;

    @BeforeEach
    public void setup() {
        bean = new AnnotationManagerBean();
    }

    @Test
    public void testRewriterAppliedToSuccessResponse() throws Exception {
        // Inject the mock rewriter
        setField(bean, "responseRewriter", new MockResponseRewriter());

        // Create a success response (200 OK)
        Response response = Response.ok("test data", MediaType.APPLICATION_JSON).build();
        Response result = bean.rewriteResponse(response, null);

        assertNotNull(result);
        assertEquals(Response.Status.OK.getStatusCode(), result.getStatus());
        assertEquals(MockResponseRewriter.MOCK_REWRITER_MARKER_VALUE, result.getHeaderString(MockResponseRewriter.MOCK_REWRITER_MARKER_HEADER));
    }

    @Test
    public void testNullRewriterReturnsOriginalResponse() throws Exception {
        // Don't inject any rewriter - leave responseRewriter null
        // The rewriter should gracefully handle this and return the original response

        Response response = Response.ok("test data", MediaType.APPLICATION_JSON).build();
        Response result = bean.rewriteResponse(response, null);

        // Should return a 200 OK without the marker header
        assertNotNull(result);
        assertEquals(Response.Status.OK.getStatusCode(), result.getStatus());
        assertNull(result.getHeaderString(MockResponseRewriter.MOCK_REWRITER_MARKER_HEADER));
    }

    @Test
    public void testNoOpRewriterReturnsOriginalResponse() throws Exception {
        // Inject the NoOp rewriter - this is the explicit default when no rewriting is needed
        setField(bean, "responseRewriter", new NoOpResponseRewriter());

        Response response = Response.ok("test data", MediaType.APPLICATION_JSON).build();
        Response result = bean.rewriteResponse(response, null);

        // Should return a 200 OK without the marker header
        assertNotNull(result);
        assertEquals(Response.Status.OK.getStatusCode(), result.getStatus());
        assertNull(result.getHeaderString(MockResponseRewriter.MOCK_REWRITER_MARKER_HEADER));
    }

    @Test
    public void testRewriterExceptionProduces500() throws Exception {
        // Inject a rewriter that throws
        setField(bean, "responseRewriter", new ResponseRewriter() {
            @Override
            public Response rewriteResponse(Response response, ResponseRewriterContext context) {
                throw new RuntimeException("Rewriter failure for testing");
            }
        });

        Response originalResponse = Response.ok("test data", MediaType.APPLICATION_JSON).build();
        Response result = bean.rewriteResponse(originalResponse, null);

        // Should return HTTP 500 with error message
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), result.getStatus());
        assertNotNull(result.getEntity());
        assertTrue(result.getEntity().toString().contains("Rewriter failure for testing"));
    }

    @Test
    public void testRewriterReturnsNullProduces500() throws Exception {
        // Inject a rewriter that returns null
        setField(bean, "responseRewriter", new ResponseRewriter() {
            @Override
            public Response rewriteResponse(Response response, ResponseRewriterContext context) {
                return null;
            }
        });

        Response originalResponse = Response.ok("test data", MediaType.APPLICATION_JSON).build();
        Response result = bean.rewriteResponse(originalResponse, null);

        // Should return HTTP 500 with error message indicating rewriter returned null
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), result.getStatus());
        assertNotNull(result.getEntity());
        assertTrue(result.getEntity().toString().contains("ResponseRewriter returned null"));
    }

    @Test
    public void testErrorResponsesAreNotRewritten() throws Exception {
        // Inject the mock rewriter
        setField(bean, "responseRewriter", new MockResponseRewriter());

        // Create a 500 error response and verify the rewriter is NOT applied
        Response errorResponse = Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity("{\"message\":\"some error\"}").build();
        Response result = bean.rewriteResponse(errorResponse, null);

        // Should return the original 500 error response unchanged
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), result.getStatus());
        assertNull(result.getHeaderString(MockResponseRewriter.MOCK_REWRITER_MARKER_HEADER));
    }

    @Test
    public void testNotFoundResponsesAreNotRewritten() throws Exception {
        // Inject the mock rewriter
        setField(bean, "responseRewriter", new MockResponseRewriter());

        // Create a 404 response and verify the rewriter is NOT applied
        Response notFoundResponse = Response.status(Response.Status.NOT_FOUND).entity("{\"message\":\"not found\"}").build();
        Response result = bean.rewriteResponse(notFoundResponse, null);

        // Should return the original 404 response unchanged
        assertEquals(Response.Status.NOT_FOUND.getStatusCode(), result.getStatus());
        assertNull(result.getHeaderString(MockResponseRewriter.MOCK_REWRITER_MARKER_HEADER));
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
