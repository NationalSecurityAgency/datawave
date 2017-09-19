package datawave.webservice.common.exception;

import java.lang.reflect.UndeclaredThrowableException;
import java.util.Arrays;

import javax.ejb.EJBAccessException;
import javax.ejb.EJBException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import datawave.webservice.query.exception.QueryException;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class RESTExceptionMapperTest {
    
    private Logger log = Logger.getLogger(this.getClass());
    
    private static RESTExceptionMapper rem;
    
    @BeforeClass
    public static void before() {
        rem = new RESTExceptionMapper();
    }
    
    @Test
    public void testToResponse_simpleException() {
        
        Exception e = new Exception();
        Response response = rem.toResponse(e);
        MultivaluedMap<String,Object> responseMap = response.getMetadata();
        String[] keyArray = new String[responseMap.keySet().size()];
        keyArray = responseMap.keySet().toArray(keyArray);
        Arrays.sort(keyArray);
        
        Assert.assertEquals(500, response.getStatus());
        Assert.assertEquals(5, responseMap.size());
        Assert.assertEquals("[true]", responseMap.get(keyArray[0]).toString());
        Assert.assertEquals(1, responseMap.get(keyArray[0]).size());
        Assert.assertEquals("[X-SSL-ClientCert-Subject, X-ProxiedEntitiesChain, X-ProxiedIssuersChain, Accept, Accept-Encoding]", responseMap.get(keyArray[1])
                        .toString());
        Assert.assertEquals("[*]", responseMap.get(keyArray[2]).toString());
        Assert.assertEquals("[864000]", responseMap.get(keyArray[3]).toString());
        Assert.assertEquals("[null/null]", responseMap.get(keyArray[4]).toString());
    }
    
    @Test
    public void testToResponse_EJBException() {
        Exception e = new EJBException();
        StackTraceElement[] traceArr = new StackTraceElement[1];
        traceArr[0] = new StackTraceElement("dummyClass", "dummyMethod", null, 0);
        
        e.setStackTrace(traceArr);
        e.initCause(new WebApplicationException());
        
        Response response = rem.toResponse(e);
        MultivaluedMap<String,Object> responseMap = response.getMetadata();
        String[] keyArray = new String[responseMap.keySet().size()];
        keyArray = responseMap.keySet().toArray(keyArray);
        Arrays.sort(keyArray);
        
        Assert.assertEquals(500, response.getStatus());
        Assert.assertEquals(5, responseMap.size());
        Assert.assertEquals("[true]", responseMap.get(keyArray[0]).toString());
        Assert.assertEquals(1, responseMap.get(keyArray[1]).size());
        Assert.assertEquals("[X-SSL-ClientCert-Subject, X-ProxiedEntitiesChain, X-ProxiedIssuersChain, Accept, Accept-Encoding]", responseMap.get(keyArray[1])
                        .toString());
        Assert.assertEquals("[*]", responseMap.get(keyArray[2]).toString());
        Assert.assertEquals("[864000]", responseMap.get(keyArray[3]).toString());
        Assert.assertEquals("[null/null]", responseMap.get(keyArray[4]).toString());
    }
    
    @Test
    public void testToResponse_EJBException2() {
        Exception e = new EJBException();
        StackTraceElement[] traceArr = new StackTraceElement[1];
        traceArr[0] = new StackTraceElement("dummyClass", "dummyMethod", null, 0);
        
        e.setStackTrace(traceArr);
        e.initCause(new QueryException());
        
        Response response = rem.toResponse(e);
        MultivaluedMap<String,Object> responseMap = response.getMetadata();
        String[] keyArray = new String[responseMap.keySet().size()];
        keyArray = responseMap.keySet().toArray(keyArray);
        Arrays.sort(keyArray);
        
        Assert.assertEquals(500, response.getStatus());
        Assert.assertEquals(5, responseMap.size());
        Assert.assertEquals("[true]", responseMap.get(keyArray[0]).toString());
        Assert.assertEquals(1, responseMap.get(keyArray[1]).size());
        Assert.assertEquals("[X-SSL-ClientCert-Subject, X-ProxiedEntitiesChain, X-ProxiedIssuersChain, Accept, Accept-Encoding]", responseMap.get(keyArray[1])
                        .toString());
        Assert.assertEquals("[*]", responseMap.get(keyArray[2]).toString());
        Assert.assertEquals("[864000]", responseMap.get(keyArray[3]).toString());
        Assert.assertEquals("[null/null]", responseMap.get(keyArray[4]).toString());
    }
    
    @Test
    public void testToResponse_EJBException3() {
        Exception e = new EJBException();
        StackTraceElement[] traceArr = new StackTraceElement[1];
        traceArr[0] = new StackTraceElement("dummyClass", "dummyMethod", null, 0);
        
        e.setStackTrace(traceArr);
        e.initCause(new ArrayStoreException());
        
        Response response = rem.toResponse(e);
        MultivaluedMap<String,Object> responseMap = response.getMetadata();
        String[] keyArray = new String[responseMap.keySet().size()];
        keyArray = responseMap.keySet().toArray(keyArray);
        Arrays.sort(keyArray);
        
        Assert.assertEquals(500, response.getStatus());
        Assert.assertEquals(5, responseMap.size());
        Assert.assertEquals("[true]", responseMap.get(keyArray[0]).toString());
        Assert.assertEquals(1, responseMap.get(keyArray[1]).size());
        Assert.assertEquals("[X-SSL-ClientCert-Subject, X-ProxiedEntitiesChain, X-ProxiedIssuersChain, Accept, Accept-Encoding]", responseMap.get(keyArray[1])
                        .toString());
        Assert.assertEquals("[*]", responseMap.get(keyArray[2]).toString());
        Assert.assertEquals("[864000]", responseMap.get(keyArray[3]).toString());
        Assert.assertEquals("[null/null]", responseMap.get(keyArray[4]).toString());
    }
    
    @Test
    public void testToResponse_GetClassAndCause() {
        Exception e = new UndeclaredThrowableException(new Exception(), "cause");
        StackTraceElement[] traceArr = new StackTraceElement[1];
        traceArr[0] = new StackTraceElement("dummyClass", "dummyMethod", null, 0);
        
        e.setStackTrace(traceArr);
        
        Response response = rem.toResponse(e);
        MultivaluedMap<String,Object> responseMap = response.getMetadata();
        String[] keyArray = new String[responseMap.keySet().size()];
        keyArray = responseMap.keySet().toArray(keyArray);
        Arrays.sort(keyArray);
        
        Assert.assertEquals(500, response.getStatus());
        Assert.assertEquals(5, responseMap.size());
        Assert.assertEquals("[true]", responseMap.get(keyArray[0]).toString());
        Assert.assertEquals(1, responseMap.get(keyArray[1]).size());
        Assert.assertEquals("[X-SSL-ClientCert-Subject, X-ProxiedEntitiesChain, X-ProxiedIssuersChain, Accept, Accept-Encoding]", responseMap.get(keyArray[1])
                        .toString());
        Assert.assertEquals("[*]", responseMap.get(keyArray[2]).toString());
        Assert.assertEquals("[864000]", responseMap.get(keyArray[3]).toString());
        Assert.assertEquals("[null/null]", responseMap.get(keyArray[4]).toString());
    }
    
    @Test
    public void testToResponse_GetClassAndCause2() {
        Exception e = new UndeclaredThrowableException(null, "java.lang.IllegalArgumentException: ");
        StackTraceElement[] traceArr = new StackTraceElement[1];
        traceArr[0] = new StackTraceElement("dummyClass", "dummyMethod", null, 0);
        
        e.setStackTrace(traceArr);
        
        Response response = rem.toResponse(e);
        MultivaluedMap<String,Object> responseMap = response.getMetadata();
        String[] keyArray = new String[responseMap.keySet().size()];
        keyArray = responseMap.keySet().toArray(keyArray);
        Arrays.sort(keyArray);
        
        Assert.assertEquals(400, response.getStatus());
        Assert.assertEquals(5, responseMap.size());
        Assert.assertEquals("[true]", responseMap.get(keyArray[0]).toString());
        Assert.assertEquals(1, responseMap.get(keyArray[1]).size());
        Assert.assertEquals("[X-SSL-ClientCert-Subject, X-ProxiedEntitiesChain, X-ProxiedIssuersChain, Accept, Accept-Encoding]", responseMap.get(keyArray[1])
                        .toString());
        Assert.assertEquals("[*]", responseMap.get(keyArray[2]).toString());
        Assert.assertEquals("[864000]", responseMap.get(keyArray[3]).toString());
        Assert.assertEquals("[null/null]", responseMap.get(keyArray[4]).toString());
    }
    
    @Test
    public void testToResponse_EJBAccessException() {
        Exception e = new EJBAccessException("Caller unauthorized");
        StackTraceElement[] traceArr = new StackTraceElement[1];
        traceArr[0] = new StackTraceElement("dummyClass", "dummyMethod", null, 0);
        
        e.setStackTrace(traceArr);
        
        Response response = rem.toResponse(e);
        MultivaluedMap<String,Object> responseMap = response.getMetadata();
        String[] keyArray = new String[responseMap.keySet().size()];
        keyArray = responseMap.keySet().toArray(keyArray);
        Arrays.sort(keyArray);
        
        Assert.assertEquals(403, response.getStatus());
        Assert.assertEquals(5, responseMap.size());
        Assert.assertEquals("[true]", responseMap.get(keyArray[0]).toString());
        Assert.assertEquals(1, responseMap.get(keyArray[1]).size());
        Assert.assertEquals("[X-SSL-ClientCert-Subject, X-ProxiedEntitiesChain, X-ProxiedIssuersChain, Accept, Accept-Encoding]", responseMap.get(keyArray[1])
                        .toString());
        Assert.assertEquals("[*]", responseMap.get(keyArray[2]).toString());
        Assert.assertEquals("[864000]", responseMap.get(keyArray[3]).toString());
        Assert.assertEquals("[null/null]", responseMap.get(keyArray[4]).toString());
    }
    
    @Test
    public void testToResponse_EJBAccessException2() {
        Exception e = new EJBAccessException("Caller is legit");
        StackTraceElement[] traceArr = new StackTraceElement[1];
        traceArr[0] = new StackTraceElement("dummyClass", "dummyMethod", null, 0);
        
        e.setStackTrace(traceArr);
        
        Response response = rem.toResponse(e);
        MultivaluedMap<String,Object> responseMap = response.getMetadata();
        String[] keyArray = new String[responseMap.keySet().size()];
        keyArray = responseMap.keySet().toArray(keyArray);
        Arrays.sort(keyArray);
        
        Assert.assertEquals(500, response.getStatus());
        Assert.assertEquals(5, responseMap.size());
        Assert.assertEquals("[true]", responseMap.get(keyArray[0]).toString());
        Assert.assertEquals(1, responseMap.get(keyArray[1]).size());
        Assert.assertEquals("[X-SSL-ClientCert-Subject, X-ProxiedEntitiesChain, X-ProxiedIssuersChain, Accept, Accept-Encoding]", responseMap.get(keyArray[1])
                        .toString());
        Assert.assertEquals("[*]", responseMap.get(keyArray[2]).toString());
        Assert.assertEquals("[864000]", responseMap.get(keyArray[3]).toString());
        Assert.assertEquals("[null/null]", responseMap.get(keyArray[4]).toString());
    }
}
