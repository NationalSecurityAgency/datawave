package datawave.webservice.annotation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;

import javax.ejb.EJBContext;
import javax.ws.rs.core.Response;

import org.apache.accumulo.core.client.AccumuloClient;
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
 * Unit tests verifying that per-request state in {@link AnnotationManagerBean} is properly implemented by observing interactions with the Accumulo connection
 * factory and connection request bean.
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

    private AnnotationManagerBean bean;

    @BeforeEach
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);

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

    @Test
    public void testConnectionFactoryIsCalledOncePerRequest() throws Exception {
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
    public void testConnectionBeanIsCalledOncePerRequest() throws Exception {
        // First request
        bean.getAnnotationSource("hash1");

        // Second request on the same bean instance (simulates EJB pool reuse)
        bean.getAnnotationSource("hash2");

        // The client should have been obtained twice — once per request,
        // proving state was properly reset between requests.
        verify(accumuloConnectionRequestBean, times(2)).requestBegin(any(), any(), any());

        // And the client should have been returned to the pool twice
        verify(accumuloConnectionRequestBean, times(2)).requestEnd(any());
    }

    @Test
    public void testConnectionFactoryFailureImpactsSecondRequest() throws Exception {
        // Simulate an error during client acquisition on the second call
        when(connectionFactory.getClient(any(), any(), any(), any(), any())).thenReturn(accumuloClient).thenThrow(new RuntimeException("connection failure"));

        // First request
        bean.getAnnotationSource("hash1");

        // Second request will fail during initialization
        Response response = bean.getAnnotationSource("hash2");
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = findField(target.getClass(), fieldName);
        field.setAccessible(true);
        field.set(target, value);
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
