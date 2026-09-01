package datawave.webservice.annotation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;

import javax.ws.rs.core.Response;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.core.query.logic.ResponseRewriter;
import datawave.core.query.logic.ResponseRewriterContext;

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

        // Create a success response (200 OK) through the package-private jsonOk method
        Response result = bean.jsonOk("test data", null);

        assertNotNull(result);
        assertEquals(Response.Status.OK.getStatusCode(), result.getStatus());
        assertEquals(MockResponseRewriter.MOCK_REWRITER_MARKER_VALUE, result.getHeaderString(MockResponseRewriter.MOCK_REWRITER_MARKER_HEADER));
    }

    @Test
    public void testNoRewriterReturnsOriginalResponse() throws Exception {
        // Don't inject a rewriter - leave responseRewriter null
        assertNull(getField(bean, "responseRewriter"));

        Response result = bean.jsonOk("test data", null);

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

        Response result = bean.jsonOk("test data", null);

        // Should return HTTP 500 with error message
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), result.getStatus());
        assertNotNull(result.getEntity());
        assertTrue(result.getEntity().toString().contains("Rewriter failure for testing"));
    }

    @Test
    public void testMockResponseRewriterAddsMarker() {
        MockResponseRewriter rewriter = new MockResponseRewriter();
        Response original = Response.ok("data").build();
        MockResponseRewriterContext context = new MockResponseRewriterContext();

        Response result = rewriter.rewriteResponse(original, context);

        assertEquals(MockResponseRewriter.MOCK_REWRITER_MARKER_VALUE, result.getHeaderString(MockResponseRewriter.MOCK_REWRITER_MARKER_HEADER));
    }

    // --- Test helpers ---

    // Minimal test context implementation that satisfies ResponseRewriterContext
    private static class MockResponseRewriterContext implements ResponseRewriterContext {
        @Override
        public datawave.security.authorization.DatawavePrincipal getDatawavePrincipal() {
            return null;
        }

        @Override
        public String getUserDn() {
            return "CN=testUser";
        }

        @Override
        public java.util.Collection<String> getProxyServers() {
            return java.util.Collections.emptyList();
        }
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Object getField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }
}
