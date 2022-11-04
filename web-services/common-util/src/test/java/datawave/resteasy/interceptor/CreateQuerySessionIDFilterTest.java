package datawave.resteasy.interceptor;

import datawave.annotation.GenerateQuerySessionId;
import org.easymock.EasyMock;
import org.easymock.EasyMockExtension;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.easymock.MockType;
import org.jboss.resteasy.core.ResourceMethodInvoker;
import org.jboss.resteasy.core.interception.ContainerResponseContextImpl;
import org.jboss.resteasy.core.interception.ResponseContainerRequestContext;
import org.jboss.resteasy.mock.MockHttpRequest;
import org.jboss.resteasy.mock.MockHttpResponse;
import org.jboss.resteasy.specimpl.BuiltResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.ws.rs.core.NewCookie;
import javax.ws.rs.core.Response;
import java.lang.annotation.Annotation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 *
 */
@ExtendWith(EasyMockExtension.class)
public class CreateQuerySessionIDFilterTest extends EasyMockSupport {
    
    private CreateQuerySessionIDFilter filter;
    private ResponseContainerRequestContext request;
    private ContainerResponseContextImpl response;
    private GenerateQuerySessionId annotation;
    @Mock(type = MockType.STRICT)
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
        assertNotNull(responseCookie, "No cookie present when we should have one.");
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
        assertNotNull(responseCookie, "No cookie present when we should have one.");
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
        assertNull(responseCookie, "Cookie present when we shouldn't have one.");
        
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
        assertNull(responseCookie, "Cookie present when we shouldn't have one.");
        
        verifyAll();
    }
}
