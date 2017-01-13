package nsa.datawave.webservice.query.exception;

import java.util.List;

import javax.ws.rs.core.Response.Status;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

public class QueryExceptionTest {
    
    protected static final Logger log = Logger.getLogger(QueryExceptionTest.class);
    
    private final String message = "message";
    private final Throwable throwable = new Throwable("throws");
    private final String strErrCode = "404-1";
    private final DatawaveErrorCode code = DatawaveErrorCode.ACCUMULO_AUTHS_ERROR;
    private final Status status = Status.ACCEPTED;
    
    private final String assertMsg = "Unable to retrieve Accumulo user authorizations. message";
    
    private QueryException qe;
    
    /**
     * Tests constructor form of
     * 
     * public QueryException()
     */
    @Test
    public void testEmptyConstructor() {
        qe = new QueryException();
        
        Assert.assertEquals("500-1", qe.getErrorCode());
        Assert.assertEquals(500, qe.getStatusCode());
        Assert.assertEquals(null, qe.getLocalizedMessage());
        Assert.assertEquals(null, qe.getMessage());
        Assert.assertEquals(null, qe.getCause());
        
        qe.setErrorCode("99999 of uptime.");
        Assert.assertEquals("99999 of uptime.", qe.getErrorCode());
    }
    
    /**
     * Tests constructor form of
     * 
     * public QueryException(String message)
     */
    @Test
    public void testMessageConstructor() {
        qe = new QueryException("message");
        
        Assert.assertEquals("500-1", qe.getErrorCode());
        Assert.assertEquals(500, qe.getStatusCode());
        Assert.assertEquals("message", qe.getLocalizedMessage());
        Assert.assertEquals("message", qe.getMessage());
        Assert.assertEquals(null, qe.getCause());
    }
    
    /**
     * Tests constructor form of
     * 
     * public QueryException(String message, Throwable cause)
     */
    @Test
    public void testMessageThrowableConstructor() {
        qe = new QueryException("message", throwable);
        
        Assert.assertEquals("500-1", qe.getErrorCode());
        Assert.assertEquals(500, qe.getStatusCode());
        Assert.assertEquals("message", qe.getLocalizedMessage());
        Assert.assertEquals("message", qe.getMessage());
        Assert.assertEquals("throws", qe.getCause().getMessage());
    }
    
    /**
     * Tests constructor form of
     * 
     * public QueryException(Throwable cause, String errorCode)
     */
    @Test
    public void testThrowableErrorCodeConstructor() {
        qe = new QueryException(throwable, strErrCode);
        
        Assert.assertEquals("404-1", qe.getErrorCode());
        Assert.assertEquals(404, qe.getStatusCode());
        Assert.assertEquals(throwable.toString(), qe.getLocalizedMessage());
        Assert.assertEquals(throwable.toString(), qe.getMessage());
    }
    
    /**
     * Tests constructor form of
     * 
     * public QueryException(DatawaveErrorCode code, Throwable cause)
     */
    @Test
    public void testDatawaveErrorCodeThrowableConstructor() {
        qe = new QueryException(code, throwable);
        
        Assert.assertEquals("500-50", qe.getErrorCode());
        Assert.assertEquals(500, qe.getStatusCode());
        Assert.assertEquals("Unable to retrieve Accumulo user authorizations.", qe.getLocalizedMessage());
        Assert.assertEquals("Unable to retrieve Accumulo user authorizations.", qe.getMessage());
    }
    
    /**
     * Tests constructor form of
     * 
     * public QueryException(DatawaveErrorCode code, String debugMessage)
     */
    @Test
    public void testDatawaveErrorCodeDebugMsgConstructor() {
        qe = new QueryException(code, message);
        
        Assert.assertEquals("500-50", qe.getErrorCode());
        Assert.assertEquals(500, qe.getStatusCode());
        Assert.assertEquals(assertMsg, qe.getLocalizedMessage());
        Assert.assertEquals(assertMsg, qe.getMessage());
    }
    
    /**
     * Tests constructor form of
     * 
     * public QueryException(DatawaveErrorCode code, Throwable cause, String debugMessage)
     */
    @Test
    public void testDatawaveErrorCodeThrowableDebugMsgConstructor() {
        qe = new QueryException(code, throwable, message);
        
        Assert.assertEquals("500-50", qe.getErrorCode());
        Assert.assertEquals(500, qe.getStatusCode());
        Assert.assertEquals(assertMsg, qe.getLocalizedMessage());
        Assert.assertEquals(assertMsg, qe.getMessage());
        Assert.assertEquals("throws", qe.getCause().getMessage());
        
        // addSuppressed not supported until 1.7. This package is marked to be 1.6 compatible
        // Throwable throwable_2 = new Throwable("throws2");
        // qe.addSuppressed(throwable_2);
        
        StackTraceElement[] st = new StackTraceElement[2];
        st[0] = new StackTraceElement("a", "b", "c", 0);
        st[1] = new StackTraceElement("d", "e", "f", 1);
        
        throwable.setStackTrace(st);
        qe = new QueryException(code, throwable, message);
        
        QueryException bottom = qe.getBottomQueryException();
        Assert.assertEquals("500-50", bottom.getErrorCode());
        Assert.assertEquals(assertMsg, bottom.getMessage());
        
        List<QueryException> qeList = qe.getQueryExceptionsInStack();
        Assert.assertEquals(1, qeList.size());
        
    }
    
    /**
     * Tests constructor form of
     * 
     * public QueryException(DatawaveErrorCode code)
     */
    @Test
    public void testDatawaveErrorCodeConstructor() {
        qe = new QueryException(code);
        
        Assert.assertEquals("500-50", qe.getErrorCode());
        Assert.assertEquals(500, qe.getStatusCode());
        Assert.assertEquals("Unable to retrieve Accumulo user authorizations.", qe.getLocalizedMessage());
        Assert.assertEquals("Unable to retrieve Accumulo user authorizations.", qe.getMessage());
    }
    
    /**
     * Tests constructor form of
     * 
     * public QueryException(DatawaveErrorCode code)
     */
    @Test
    public void testMessageResponseStatus() {
        qe = new QueryException(message, status);
        
        Assert.assertEquals("202", qe.getErrorCode());
        Assert.assertEquals(202, qe.getStatusCode());
        Assert.assertEquals("message", qe.getLocalizedMessage());
        Assert.assertEquals("message", qe.getMessage());
    }
    
    /**
     * Tests constructor form of
     * 
     * public QueryException(String message, Throwable cause, String errorCode)
     */
    @Test
    public void testMessageThrowableErrorCode() {
        qe = new QueryException(message, throwable, strErrCode);
        
        Assert.assertEquals(strErrCode, qe.getErrorCode());
        Assert.assertEquals(404, qe.getStatusCode());
        Assert.assertEquals("message", qe.getLocalizedMessage());
        Assert.assertEquals("message", qe.getMessage());
        Assert.assertEquals("throws", qe.getCause().getMessage());
    }
    
    /**
     * Tests constructor form of
     * 
     * public QueryException(String message, String errorCode)
     */
    @Test
    public void testMessageErrorCode() {
        qe = new QueryException(message, strErrCode);
        
        Assert.assertEquals(strErrCode, qe.getErrorCode());
        Assert.assertEquals(404, qe.getStatusCode());
        Assert.assertEquals("message", qe.getLocalizedMessage());
        Assert.assertEquals("message", qe.getMessage());
    }
    
    /**
     * Tests constructor form of
     * 
     * public QueryException(Throwable cause)
     */
    @Test
    public void testThrowable() {
        qe = new QueryException(throwable);
        
        Assert.assertEquals("500-1", qe.getErrorCode());
        Assert.assertEquals(500, qe.getStatusCode());
        Assert.assertEquals(throwable.toString(), qe.getLocalizedMessage());
        Assert.assertEquals(throwable.toString(), qe.getMessage());
        Assert.assertEquals("throws", qe.getCause().getMessage());
    }
}
