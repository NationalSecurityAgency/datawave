package datawave.webservice.common.exception;

import com.google.common.collect.Lists;
import com.google.common.net.HttpHeaders;
import datawave.Constants;
import datawave.webservice.query.exception.QueryException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.ejb.EJBAccessException;
import javax.ejb.EJBException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.lang.reflect.UndeclaredThrowableException;

public class RESTExceptionMapperTest {
    
    private static RESTExceptionMapper rem;
    
    @BeforeAll
    public static void before() {
        rem = new RESTExceptionMapper();
    }
    
    @Test
    public void testToResponse_simpleException() {
        
        Exception e = new Exception();
        Response response = rem.toResponse(e);
        MultivaluedMap<String,Object> responseMap = response.getHeaders();
        
        Assertions.assertEquals(500, response.getStatus());
        Assertions.assertEquals(6, responseMap.size());
        Assertions.assertEquals(Lists.newArrayList(true), responseMap.get(HttpHeaders.ACCESS_CONTROL_ALLOW_CREDENTIALS));
        Assertions.assertEquals(Lists.newArrayList("*"), responseMap.get(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN));
        Assertions.assertEquals(Lists.newArrayList(864000), responseMap.get(HttpHeaders.ACCESS_CONTROL_MAX_AGE));
        Assertions.assertEquals(Lists.newArrayList("500-1"), responseMap.get(Constants.ERROR_CODE));
        Assertions.assertEquals(Lists.newArrayList("null/null"), responseMap.get(Constants.RESPONSE_ORIGIN));
        Assertions.assertEquals(Lists.newArrayList("X-SSL-ClientCert-Subject, X-ProxiedEntitiesChain, X-ProxiedIssuersChain, Accept, Accept-Encoding"),
                        responseMap.get(HttpHeaders.ACCESS_CONTROL_ALLOW_HEADERS));
    }
    
    @Test
    public void testToResponse_EJBException() {
        Exception e = new EJBException(new WebApplicationException());
        StackTraceElement[] traceArr = new StackTraceElement[1];
        traceArr[0] = new StackTraceElement("dummyClass", "dummyMethod", null, 0);
        
        e.setStackTrace(traceArr);
        
        Response response = rem.toResponse(e);
        MultivaluedMap<String,Object> responseMap = response.getHeaders();
        
        Assertions.assertEquals(500, response.getStatus());
        Assertions.assertEquals(5, responseMap.size());
        Assertions.assertEquals(Lists.newArrayList(true), responseMap.get(HttpHeaders.ACCESS_CONTROL_ALLOW_CREDENTIALS));
        Assertions.assertEquals(Lists.newArrayList("*"), responseMap.get(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN));
        Assertions.assertEquals(Lists.newArrayList(864000), responseMap.get(HttpHeaders.ACCESS_CONTROL_MAX_AGE));
        Assertions.assertEquals(Lists.newArrayList("null/null"), responseMap.get(Constants.RESPONSE_ORIGIN));
        Assertions.assertEquals(Lists.newArrayList("X-SSL-ClientCert-Subject, X-ProxiedEntitiesChain, X-ProxiedIssuersChain, Accept, Accept-Encoding"),
                        responseMap.get(HttpHeaders.ACCESS_CONTROL_ALLOW_HEADERS));
    }
    
    @Test
    public void testToResponse_WrappedQueryException() {
        Exception e = new WebApplicationException(new QueryException("top-level", new QueryException("bottom-level", "567-8"), "123-4"));
        StackTraceElement[] traceArr = new StackTraceElement[1];
        traceArr[0] = new StackTraceElement("dummyClass", "dummyMethod", null, 0);
        
        e.setStackTrace(traceArr);
        
        Response response = rem.toResponse(e);
        MultivaluedMap<String,Object> responseMap = response.getHeaders();
        
        Assertions.assertEquals(500, response.getStatus());
        Assertions.assertEquals(6, responseMap.size());
        Assertions.assertEquals(Lists.newArrayList(true), responseMap.get(HttpHeaders.ACCESS_CONTROL_ALLOW_CREDENTIALS));
        Assertions.assertEquals(Lists.newArrayList("*"), responseMap.get(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN));
        Assertions.assertEquals(Lists.newArrayList(864000), responseMap.get(HttpHeaders.ACCESS_CONTROL_MAX_AGE));
        Assertions.assertEquals(Lists.newArrayList("567-8"), responseMap.get(Constants.ERROR_CODE));
        Assertions.assertEquals(Lists.newArrayList("null/null"), responseMap.get(Constants.RESPONSE_ORIGIN));
        Assertions.assertEquals(Lists.newArrayList("X-SSL-ClientCert-Subject, X-ProxiedEntitiesChain, X-ProxiedIssuersChain, Accept, Accept-Encoding"),
                        responseMap.get(HttpHeaders.ACCESS_CONTROL_ALLOW_HEADERS));
    }
    
    @Test
    public void testToResponse_EJBException2() {
        Exception e = new EJBException(new QueryException());
        StackTraceElement[] traceArr = new StackTraceElement[1];
        traceArr[0] = new StackTraceElement("dummyClass", "dummyMethod", null, 0);
        
        e.setStackTrace(traceArr);
        
        Response response = rem.toResponse(e);
        MultivaluedMap<String,Object> responseMap = response.getHeaders();
        
        Assertions.assertEquals(500, response.getStatus());
        Assertions.assertEquals(6, responseMap.size());
        Assertions.assertEquals(Lists.newArrayList(true), responseMap.get(HttpHeaders.ACCESS_CONTROL_ALLOW_CREDENTIALS));
        Assertions.assertEquals(Lists.newArrayList("*"), responseMap.get(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN));
        Assertions.assertEquals(Lists.newArrayList(864000), responseMap.get(HttpHeaders.ACCESS_CONTROL_MAX_AGE));
        Assertions.assertEquals(Lists.newArrayList("500-1"), responseMap.get(Constants.ERROR_CODE));
        Assertions.assertEquals(Lists.newArrayList("null/null"), responseMap.get(Constants.RESPONSE_ORIGIN));
        Assertions.assertEquals(Lists.newArrayList("X-SSL-ClientCert-Subject, X-ProxiedEntitiesChain, X-ProxiedIssuersChain, Accept, Accept-Encoding"),
                        responseMap.get(HttpHeaders.ACCESS_CONTROL_ALLOW_HEADERS));
    }
    
    @Test
    public void testToResponse_EJBException3() {
        Exception e = new EJBException(new ArrayStoreException());
        StackTraceElement[] traceArr = new StackTraceElement[1];
        traceArr[0] = new StackTraceElement("dummyClass", "dummyMethod", null, 0);
        
        e.setStackTrace(traceArr);
        
        Response response = rem.toResponse(e);
        MultivaluedMap<String,Object> responseMap = response.getHeaders();
        
        Assertions.assertEquals(500, response.getStatus());
        Assertions.assertEquals(6, responseMap.size());
        Assertions.assertEquals(Lists.newArrayList(true), responseMap.get(HttpHeaders.ACCESS_CONTROL_ALLOW_CREDENTIALS));
        Assertions.assertEquals(Lists.newArrayList("*"), responseMap.get(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN));
        Assertions.assertEquals(Lists.newArrayList(864000), responseMap.get(HttpHeaders.ACCESS_CONTROL_MAX_AGE));
        Assertions.assertEquals(Lists.newArrayList("500-1"), responseMap.get(Constants.ERROR_CODE));
        Assertions.assertEquals(Lists.newArrayList("null/null"), responseMap.get(Constants.RESPONSE_ORIGIN));
        Assertions.assertEquals(Lists.newArrayList("X-SSL-ClientCert-Subject, X-ProxiedEntitiesChain, X-ProxiedIssuersChain, Accept, Accept-Encoding"),
                        responseMap.get(HttpHeaders.ACCESS_CONTROL_ALLOW_HEADERS));
    }
    
    @Test
    public void testToResponse_GetClassAndCause() {
        Exception e = new UndeclaredThrowableException(new Exception(), "cause");
        StackTraceElement[] traceArr = new StackTraceElement[1];
        traceArr[0] = new StackTraceElement("dummyClass", "dummyMethod", null, 0);
        
        e.setStackTrace(traceArr);
        
        Response response = rem.toResponse(e);
        MultivaluedMap<String,Object> responseMap = response.getHeaders();
        
        Assertions.assertEquals(500, response.getStatus());
        Assertions.assertEquals(6, responseMap.size());
        Assertions.assertEquals(Lists.newArrayList(true), responseMap.get(HttpHeaders.ACCESS_CONTROL_ALLOW_CREDENTIALS));
        Assertions.assertEquals(Lists.newArrayList("*"), responseMap.get(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN));
        Assertions.assertEquals(Lists.newArrayList(864000), responseMap.get(HttpHeaders.ACCESS_CONTROL_MAX_AGE));
        Assertions.assertEquals(Lists.newArrayList("500-1"), responseMap.get(Constants.ERROR_CODE));
        Assertions.assertEquals(Lists.newArrayList("null/null"), responseMap.get(Constants.RESPONSE_ORIGIN));
        Assertions.assertEquals(Lists.newArrayList("X-SSL-ClientCert-Subject, X-ProxiedEntitiesChain, X-ProxiedIssuersChain, Accept, Accept-Encoding"),
                        responseMap.get(HttpHeaders.ACCESS_CONTROL_ALLOW_HEADERS));
    }
    
    @Test
    public void testToResponse_GetClassAndCause2() {
        Exception e = new UndeclaredThrowableException(null, "java.lang.IllegalArgumentException: ");
        StackTraceElement[] traceArr = new StackTraceElement[1];
        traceArr[0] = new StackTraceElement("dummyClass", "dummyMethod", null, 0);
        
        e.setStackTrace(traceArr);
        
        Response response = rem.toResponse(e);
        MultivaluedMap<String,Object> responseMap = response.getHeaders();
        
        Assertions.assertEquals(400, response.getStatus());
        Assertions.assertEquals(6, responseMap.size());
        Assertions.assertEquals(Lists.newArrayList(true), responseMap.get(HttpHeaders.ACCESS_CONTROL_ALLOW_CREDENTIALS));
        Assertions.assertEquals(Lists.newArrayList("*"), responseMap.get(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN));
        Assertions.assertEquals(Lists.newArrayList(864000), responseMap.get(HttpHeaders.ACCESS_CONTROL_MAX_AGE));
        Assertions.assertEquals(Lists.newArrayList("400-1"), responseMap.get(Constants.ERROR_CODE));
        Assertions.assertEquals(Lists.newArrayList("null/null"), responseMap.get(Constants.RESPONSE_ORIGIN));
        Assertions.assertEquals(Lists.newArrayList("X-SSL-ClientCert-Subject, X-ProxiedEntitiesChain, X-ProxiedIssuersChain, Accept, Accept-Encoding"),
                        responseMap.get(HttpHeaders.ACCESS_CONTROL_ALLOW_HEADERS));
    }
    
    @Test
    public void testToResponse_EJBAccessException() {
        Exception e = new EJBAccessException("Caller unauthorized");
        StackTraceElement[] traceArr = new StackTraceElement[1];
        traceArr[0] = new StackTraceElement("dummyClass", "dummyMethod", null, 0);
        
        e.setStackTrace(traceArr);
        
        Response response = rem.toResponse(e);
        MultivaluedMap<String,Object> responseMap = response.getHeaders();
        
        Assertions.assertEquals(403, response.getStatus());
        Assertions.assertEquals(6, responseMap.size());
        Assertions.assertEquals(Lists.newArrayList(true), responseMap.get(HttpHeaders.ACCESS_CONTROL_ALLOW_CREDENTIALS));
        Assertions.assertEquals(Lists.newArrayList("*"), responseMap.get(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN));
        Assertions.assertEquals(Lists.newArrayList(864000), responseMap.get(HttpHeaders.ACCESS_CONTROL_MAX_AGE));
        Assertions.assertEquals(Lists.newArrayList("403-1"), responseMap.get(Constants.ERROR_CODE));
        Assertions.assertEquals(Lists.newArrayList("null/null"), responseMap.get(Constants.RESPONSE_ORIGIN));
        Assertions.assertEquals(Lists.newArrayList("X-SSL-ClientCert-Subject, X-ProxiedEntitiesChain, X-ProxiedIssuersChain, Accept, Accept-Encoding"),
                        responseMap.get(HttpHeaders.ACCESS_CONTROL_ALLOW_HEADERS));
    }
    
    @Test
    public void testToResponse_EJBAccessException2() {
        Exception e = new EJBAccessException("Caller is legit");
        StackTraceElement[] traceArr = new StackTraceElement[1];
        traceArr[0] = new StackTraceElement("dummyClass", "dummyMethod", null, 0);
        
        e.setStackTrace(traceArr);
        
        Response response = rem.toResponse(e);
        MultivaluedMap<String,Object> responseMap = response.getHeaders();
        
        Assertions.assertEquals(500, response.getStatus());
        Assertions.assertEquals(6, responseMap.size());
        Assertions.assertEquals(Lists.newArrayList(true), responseMap.get(HttpHeaders.ACCESS_CONTROL_ALLOW_CREDENTIALS));
        Assertions.assertEquals(Lists.newArrayList("*"), responseMap.get(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN));
        Assertions.assertEquals(Lists.newArrayList(864000), responseMap.get(HttpHeaders.ACCESS_CONTROL_MAX_AGE));
        Assertions.assertEquals(Lists.newArrayList("500-1"), responseMap.get(Constants.ERROR_CODE));
        Assertions.assertEquals(Lists.newArrayList("null/null"), responseMap.get(Constants.RESPONSE_ORIGIN));
        Assertions.assertEquals(Lists.newArrayList("X-SSL-ClientCert-Subject, X-ProxiedEntitiesChain, X-ProxiedIssuersChain, Accept, Accept-Encoding"),
                        responseMap.get(HttpHeaders.ACCESS_CONTROL_ALLOW_HEADERS));
    }
}
