package datawave.resteasy.interceptor;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;

import java.lang.annotation.Annotation;

import javax.ws.rs.core.NewCookie;
import javax.ws.rs.core.Response;

import datawave.annotation.GenerateQuerySessionId;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.easymock.MockType;
import org.jboss.resteasy.core.ResourceMethodInvoker;
import org.jboss.resteasy.core.interception.ContainerResponseContextImpl;
import org.jboss.resteasy.core.interception.ResponseContainerRequestContext;
import org.jboss.resteasy.mock.MockHttpRequest;
import org.jboss.resteasy.mock.MockHttpResponse;
import org.jboss.resteasy.specimpl.BuiltResponse;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 *
 */
@RunWith(EasyMockRunner.class)
public class CreateQuerySessionIDFilterTest extends EasyMockSupport {

    private CreateQuerySessionIDFilter filter;
    private ResponseContainerRequestContext request;
    private ContainerResponseContextImpl response;
    private GenerateQuerySessionId annotation;
    @Mock(type = MockType.STRICT)
    private ResourceMethodInvoker method;

    @Before
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

        response = new ContainerResponseContextImpl(request.getHttpRequest(), new MockHttpResponse(), new BuiltResponse());

        filter = new CreateQuerySessionIDFilter();
    }

    @Test
    public void filter() throws Exception {
        EasyMock.expect(method.getMethodAnnotations()).andReturn(new Annotation[] {annotation});
        replayAll();

        CreateQuerySessionIDFilter.QUERY_ID.set("1234");
        filter.filter(request, response);

        NewCookie responseCookie = (NewCookie) response.getHeaders().getFirst("Set-Cookie");
        assertNotNull("No cookie present when we should have one.", responseCookie);
        assertEquals("query-session-id", responseCookie.getName());
        assertEquals("/test/path/1234", responseCookie.getPath());

        verifyAll();
    }

    @Test
    public void filterNoQueryId() throws Exception {
        EasyMock.expect(method.getMethodAnnotations()).andReturn(new Annotation[] {annotation});
        // More method calls due to logging the error about QUERY_ID threadlocal not set.
        EasyMock.expect(method.getResourceClass()).andReturn(null);
        // noinspection ConfusingArgumentToVarargsMethod
        EasyMock.expect(method.getMethod()).andReturn(getClass().getMethod("filterNoQueryId", null));
        replayAll();

        CreateQuerySessionIDFilter.QUERY_ID.set(null);
        filter.filter(request, response);

        NewCookie responseCookie = (NewCookie) response.getHeaders().getFirst("Set-Cookie");
        assertNotNull("No cookie present when we should have one.", responseCookie);
        assertEquals("query-session-id", responseCookie.getName());
        assertEquals("/test/path/", responseCookie.getPath());

        verifyAll();
    }

    @Test
    public void filterServerError() throws Exception {
        response.setStatusInfo(Response.Status.INTERNAL_SERVER_ERROR);
        EasyMock.expect(method.getMethodAnnotations()).andReturn(new Annotation[] {annotation});
        replayAll();

        CreateQuerySessionIDFilter.QUERY_ID.set("1234");
        filter.filter(request, response);

        NewCookie responseCookie = (NewCookie) response.getHeaders().getFirst("Set-Cookie");
        assertNull("Cookie present when we shouldn't have one.", responseCookie);

        verifyAll();
    }

    @Test
    public void filterClientError() throws Exception {
        response.setStatusInfo(Response.Status.BAD_REQUEST);
        EasyMock.expect(method.getMethodAnnotations()).andReturn(new Annotation[] {annotation});
        replayAll();

        CreateQuerySessionIDFilter.QUERY_ID.set("1234");
        filter.filter(request, response);

        NewCookie responseCookie = (NewCookie) response.getHeaders().getFirst("Set-Cookie");
        assertNull("Cookie present when we shouldn't have one.", responseCookie);

        verifyAll();
    }
}
