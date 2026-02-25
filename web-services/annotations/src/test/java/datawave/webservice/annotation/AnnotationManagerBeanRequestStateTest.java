package datawave.webservice.annotation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ejb.EJBContext;
import javax.ws.rs.core.Response;

import org.apache.accumulo.core.client.AccumuloClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import datawave.annotation.data.transform.DefaultTimestampTransformer;
import datawave.annotation.data.transform.DefaultVisibilityTransformer;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.query.config.annotation.AnnotationConfig;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.webservice.query.runner.AccumuloConnectionRequestBean;

/**
 * Unit tests verifying that per-request stateful variables in {@link AnnotationManagerBean} are properly reset after each request, preventing state from
 * leaking across requests when the bean instance is reused from the stateless EJB pool.
 */
public class AnnotationManagerBeanRequestStateTest {

    @Mock
    private EJBContext ctx;

    @Mock
    private AccumuloConnectionFactory connectionFactory;

    @Mock
    private AccumuloConnectionRequestBean accumuloConnectionRequestBean;

    @Mock
    private AccumuloClient accumuloClient;

    private AutoCloseable mocks;
    private AnnotationManagerBean bean;

    @BeforeEach
    public void setup() throws Exception {
        mocks = MockitoAnnotations.openMocks(this);

        bean = new AnnotationManagerBean();

        // Build a real AnnotationManagerConfig with a real AnnotationConfig
        AnnotationConfig annotationConfig = new AnnotationConfig();
        annotationConfig.setAnnotationTableName("annotation");
        annotationConfig.setAnnotationSourceTableName("annotationSource");
        annotationConfig.setVisibilityTransformer(new DefaultVisibilityTransformer());
        annotationConfig.setTimestampTransformer(new DefaultTimestampTransformer());

        AnnotationManagerConfig config = new AnnotationManagerConfig();
        config.setAnnotationConfig(annotationConfig);
        config.setConnPoolName("default");
        config.setPriority(AccumuloConnectionFactory.Priority.NORMAL);
        config.setEnableInternalIdLookup(true);

        // Inject mocks and config into private fields
        setField(bean, "ctx", ctx);
        setField(bean, "connectionFactory", connectionFactory);
        setField(bean, "accumuloConnectionRequestBean", accumuloConnectionRequestBean);
        setField(bean, "config", config);

        // Set up the principal
        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of("testUser"), DatawaveUser.UserType.USER, List.of("ALL", "PUBLIC"), null, null, -1L);
        DatawavePrincipal principal = new DatawavePrincipal(List.of(user));
        when(ctx.getCallerPrincipal()).thenReturn(principal);

        // Set up connection factory to return a mock client
        when(connectionFactory.getTrackingMap(any())).thenReturn(new HashMap<>());
        when(connectionFactory.getClient(any(), any(), any(), any(), any())).thenReturn(accumuloClient);
    }

    @AfterEach
    public void tearDown() throws Exception {
        mocks.close();
    }

    @Test
    public void testStateIsResetAfterRequest() throws Exception {
        // Request will fail because mock AccumuloClient doesn't support scanning,
        // but the finally block should still reset state.
        bean.getAnnotationSource("nonexistentHash");

        // Verify per-request fields are null after the request completes
        assertNull(getField(bean, "client"), "client should be null after request");
        assertNull(getField(bean, "authorizations"), "authorizations should be null after request");
        assertNull(getField(bean, "annotationDataAccess"), "annotationDataAccess should be null after request");

        Map<?,?> cache = (Map<?,?>) getField(bean, "retrievedSourcesCache");
        assertTrue(cache.isEmpty(), "retrievedSourcesCache should be empty after request");
    }

    @Test
    public void testAccumuloClientIsReInitializedOnSubsequentRequests() throws Exception {
        // First request
        bean.getAnnotationSource("hash1");

        // Second request on the same bean instance (simulates EJB pool reuse)
        bean.getAnnotationSource("hash2");

        // The client should have been obtained twice — once per request,
        // proving state was properly reset between requests.
        verify(connectionFactory, times(2)).getClient(any(), any(), any(), any(), any());

        // And the client should have been returned to the pool twice
        verify(connectionFactory, times(2)).returnClient(any());
    }

    @Test
    public void testStateIsResetAfterGetAnnotationTypes() throws Exception {
        Response response = bean.getAnnotationTypes("DOCUMENT", "20250704_249/testDataType/abcde.fghij.klmno");

        assertNull(getField(bean, "client"), "client should be null after request");
        assertNull(getField(bean, "authorizations"), "authorizations should be null after request");
        assertNull(getField(bean, "annotationDataAccess"), "annotationDataAccess should be null after request");
    }

    @Test
    public void testStateIsResetAfterConnectionFailure() throws Exception {
        // Simulate an error during client acquisition on the second call
        when(connectionFactory.getClient(any(), any(), any(), any(), any())).thenReturn(accumuloClient).thenThrow(new RuntimeException("connection failure"));

        // First request
        bean.getAnnotationSource("hash1");

        // Second request will fail during initialization
        Response response = bean.getAnnotationSource("hash2");
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());

        // State should still be cleaned up even after failure
        assertNull(getField(bean, "client"), "client should be null after failed request");
        assertNull(getField(bean, "authorizations"), "authorizations should be null after failed request");
    }

    @Test
    public void testRetrievedSourcesCacheIsClearedBetweenRequests() throws Exception {
        bean.getAnnotationSource("hash1");

        Map<?,?> cache = (Map<?,?>) getField(bean, "retrievedSourcesCache");
        assertTrue(cache.isEmpty(), "retrievedSourcesCache should be empty between requests");

        bean.getAnnotationSource("hash2");
        cache = (Map<?,?>) getField(bean, "retrievedSourcesCache");
        assertTrue(cache.isEmpty(), "retrievedSourcesCache should be empty between requests");
    }

    @Test
    public void testLookupUUIDServiceIsResetBetweenRequests() throws Exception {
        bean.getAnnotationTypes("DOCUMENT", "20250704_249/testDataType/abcde.fghij.klmno");

        assertNull(getField(bean, "lookupUUIDService"), "lookupUUIDService should be null after request");
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = findField(target.getClass(), fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Object getField(Object target, String fieldName) throws Exception {
        Field field = findField(target.getClass(), fieldName);
        field.setAccessible(true);
        return field.get(target);
    }

    private static Field findField(Class<?> clazz, String fieldName) throws NoSuchFieldException {
        Class<?> current = clazz;
        while (current != null) {
            try {
                return current.getDeclaredField(fieldName);
            } catch (NoSuchFieldException e) {
                current = current.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName);
    }
}
