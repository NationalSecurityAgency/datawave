package datawave.resteasy.interceptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.lang.annotation.Annotation;

import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.NewCookie;
import javax.ws.rs.core.Response;

import org.jboss.resteasy.core.ResourceMethodInvoker;
import org.jboss.resteasy.core.interception.jaxrs.ContainerResponseContextImpl;
import org.jboss.resteasy.core.interception.jaxrs.ResponseContainerRequestContext;
import org.jboss.resteasy.mock.MockHttpRequest;
import org.jboss.resteasy.mock.MockHttpResponse;
import org.jboss.resteasy.specimpl.BuiltResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import datawave.annotation.GenerateQuerySessionId;

/**
 *
 */
@ExtendWith(MockitoExtension.class)
public class CreateQuerySessionIDFilterTest {

    private CreateQuerySessionIDFilter filter;
    private ResponseContainerRequestContext request;
    private ContainerResponseContextImpl response;
    private GenerateQuerySessionId annotation;
    @Mock
    private ResourceMethodInvoker method;

    @BeforeEach
    public void setUp() throws Exception {
        annotation = new GenerateQuerySessionId() {
            @Override
            public String cookieBasePath() {
                return "/test/path/";
            }

            @Override
            public Class<? extends Annotation> annotationType() {
                return GenerateQuerySessionId.class;
            }
        };

        request = new ResponseContainerRequestContext(MockHttpRequest.post("/mock"));
        request.setProperty(ResourceMethodInvoker.class.getName(), method);

        response = new ContainerResponseContextImpl(request.getHttpRequest(), new MockHttpResponse(), new BuiltResponse(), null, new ContainerResponseFilter[0],
                        (t) -> {}, null);

        filter = new CreateQuerySessionIDFilter();
    }

    @Test
    public void filter() throws Exception {
        doReturn(new Annotation[] {annotation}).when(method).getMethodAnnotations();

        CreateQuerySessionIDFilter.QUERY_ID.set("1234");
        filter.filter(request, response);

        NewCookie responseCookie = (NewCookie) response.getHeaders().getFirst("Set-Cookie");
        assertNotNull(responseCookie, "No cookie present when we should have one.");
        assertEquals("query-session-id", responseCookie.getName());
        assertEquals("/test/path/1234", responseCookie.getPath());

        verify(method, times(1)).getMethodAnnotations();

        if (method != null && mockingDetails(method).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(method));
        }
    }

    @Test
    public void filterNoQueryId() throws Exception {
        doReturn(new Annotation[] {annotation}).when(method).getMethodAnnotations();
        // More method calls due to logging the error about QUERY_ID threadlocal not set.
        doReturn(null).when(method).getResourceClass();
        // noinspection ConfusingArgumentToVarargsMethod
        doReturn(getClass().getMethod("filterNoQueryId")).when(method).getMethod();

        CreateQuerySessionIDFilter.QUERY_ID.set(null);
        filter.filter(request, response);

        NewCookie responseCookie = (NewCookie) response.getHeaders().getFirst("Set-Cookie");
        assertNotNull(responseCookie, "No cookie present when we should have one.");
        assertEquals("query-session-id", responseCookie.getName());
        assertEquals("/test/path/", responseCookie.getPath());

        InOrder order = inOrder(method);
        order.verify(method).getMethodAnnotations();
        order.verify(method).getResourceClass();
        order.verify(method).getMethod();

        if (method != null && mockingDetails(method).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(method));
        }
    }

    @Test
    public void filterServerError() throws Exception {
        response.setStatusInfo(Response.Status.INTERNAL_SERVER_ERROR);
        doReturn(new Annotation[] {annotation}).when(method).getMethodAnnotations();

        CreateQuerySessionIDFilter.QUERY_ID.set("1234");
        filter.filter(request, response);

        NewCookie responseCookie = (NewCookie) response.getHeaders().getFirst("Set-Cookie");
        assertNull(responseCookie, "Cookie present when we shouldn't have one.");

        verify(method, times(1)).getMethodAnnotations();

        if (method != null && mockingDetails(method).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(method));
        }
    }

    @Test
    public void filterClientError() throws Exception {
        response.setStatusInfo(Response.Status.BAD_REQUEST);
        doReturn(new Annotation[] {annotation}).when(method).getMethodAnnotations();

        CreateQuerySessionIDFilter.QUERY_ID.set("1234");
        filter.filter(request, response);

        NewCookie responseCookie = (NewCookie) response.getHeaders().getFirst("Set-Cookie");
        assertNull(responseCookie, "Cookie present when we shouldn't have one.");

        verify(method, times(1)).getMethodAnnotations();

        if (method != null && mockingDetails(method).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(method));
        }
    }
}
