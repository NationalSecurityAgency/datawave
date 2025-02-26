package datawave.webservice.query.exception;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryExceptionTest {
    
    protected static final Logger log = LoggerFactory.getLogger(QueryExceptionTest.class);
    
    private final String message = "message";
    private final Throwable throwable = new Throwable("throws");
    private final String strErrCode = "404-1";
    private final DatawaveErrorCode code = DatawaveErrorCode.ACCUMULO_AUTHS_ERROR;
    
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
        
        assertEquals("500-1", qe.getErrorCode());
        assertEquals(500, qe.getStatusCode());
        assertNull(qe.getLocalizedMessage());
        assertNull(qe.getMessage());
        assertNull(qe.getCause());
        
        qe.setErrorCode("99999 of uptime.");
        assertEquals("99999 of uptime.", qe.getErrorCode());
    }
    
    /**
     * Tests constructor form of
     * 
     * public QueryException(String message)
     */
    @Test
    public void testMessageConstructor() {
        qe = new QueryException("message");
        
        assertEquals("500-1", qe.getErrorCode());
        assertEquals(500, qe.getStatusCode());
        assertEquals("message", qe.getLocalizedMessage());
        assertEquals("message", qe.getMessage());
        assertNull(qe.getCause());
    }
    
    /**
     * Tests constructor form of
     * 
     * public QueryException(String message, Throwable cause)
     */
    @Test
    public void testMessageThrowableConstructor() {
        qe = new QueryException("message", throwable);
        
        assertEquals("500-1", qe.getErrorCode());
        assertEquals(500, qe.getStatusCode());
        assertEquals("message", qe.getLocalizedMessage());
        assertEquals("message", qe.getMessage());
        assertEquals("throws", qe.getCause().getMessage());
    }
    
    /**
     * Tests constructor form of
     * 
     * public QueryException(Throwable cause, String errorCode)
     */
    @Test
    public void testThrowableErrorCodeConstructor() {
        qe = new QueryException(throwable, strErrCode);
        
        assertEquals("404-1", qe.getErrorCode());
        assertEquals(404, qe.getStatusCode());
        assertEquals(throwable.toString(), qe.getLocalizedMessage());
        assertEquals(throwable.toString(), qe.getMessage());
    }
    
    /**
     * Tests constructor form of
     * 
     * public QueryException(DatawaveErrorCode code, Throwable cause)
     */
    @Test
    public void testDatawaveErrorCodeThrowableConstructor() {
        qe = new QueryException(code, throwable);
        
        assertEquals("500-50", qe.getErrorCode());
        assertEquals(500, qe.getStatusCode());
        assertEquals("Unable to retrieve Accumulo user authorizations.", qe.getLocalizedMessage());
        assertEquals("Unable to retrieve Accumulo user authorizations.", qe.getMessage());
    }
    
    /**
     * Tests constructor form of
     * 
     * public QueryException(DatawaveErrorCode code, String debugMessage)
     */
    @Test
    public void testDatawaveErrorCodeDebugMsgConstructor() {
        qe = new QueryException(code, message);
        
        assertEquals("500-50", qe.getErrorCode());
        assertEquals(500, qe.getStatusCode());
        assertEquals(assertMsg, qe.getLocalizedMessage());
        assertEquals(assertMsg, qe.getMessage());
    }
    
    /**
     * Tests constructor form of
     * 
     * public QueryException(DatawaveErrorCode code, Throwable cause, String debugMessage)
     */
    @Test
    public void testDatawaveErrorCodeThrowableDebugMsgConstructor() {
        qe = new QueryException(code, throwable, message);
        
        assertEquals("500-50", qe.getErrorCode());
        assertEquals(500, qe.getStatusCode());
        assertEquals(assertMsg, qe.getLocalizedMessage());
        assertEquals(assertMsg, qe.getMessage());
        assertEquals("throws", qe.getCause().getMessage());
        
        // addSuppressed not supported until 1.7. This package is marked to be 1.6 compatible
        // Throwable throwable_2 = new Throwable("throws2");
        // qe.addSuppressed(throwable_2);
        
        StackTraceElement[] st = new StackTraceElement[2];
        st[0] = new StackTraceElement("a", "b", "c", 0);
        st[1] = new StackTraceElement("d", "e", "f", 1);
        
        throwable.setStackTrace(st);
        qe = new QueryException(code, throwable, message);
        
        QueryException bottom = qe.getBottomQueryException();
        assertEquals("500-50", bottom.getErrorCode());
        assertEquals(assertMsg, bottom.getMessage());
        
        List<QueryException> qeList = qe.getQueryExceptionsInStack();
        assertEquals(1, qeList.size());
        
    }
    
    /**
     * Tests constructor form of
     * 
     * public QueryException(DatawaveErrorCode code)
     */
    @Test
    public void testDatawaveErrorCodeConstructor() {
        qe = new QueryException(code);
        
        assertEquals("500-50", qe.getErrorCode());
        assertEquals(500, qe.getStatusCode());
        assertEquals("Unable to retrieve Accumulo user authorizations.", qe.getLocalizedMessage());
        assertEquals("Unable to retrieve Accumulo user authorizations.", qe.getMessage());
    }
    
    /**
     * Tests constructor form of
     * 
     * public QueryException(DatawaveErrorCode code)
     */
    @Test
    public void testMessageResponseStatus() {
        qe = new QueryException(message, 202);
        
        assertEquals("202", qe.getErrorCode());
        assertEquals(202, qe.getStatusCode());
        assertEquals("message", qe.getLocalizedMessage());
        assertEquals("message", qe.getMessage());
    }
    
    /**
     * Tests constructor form of
     * 
     * public QueryException(String message, Throwable cause, String errorCode)
     */
    @Test
    public void testMessageThrowableErrorCode() {
        qe = new QueryException(message, throwable, strErrCode);
        
        assertEquals(strErrCode, qe.getErrorCode());
        assertEquals(404, qe.getStatusCode());
        assertEquals("message", qe.getLocalizedMessage());
        assertEquals("message", qe.getMessage());
        assertEquals("throws", qe.getCause().getMessage());
    }
    
    /**
     * Tests constructor form of
     * 
     * public QueryException(String message, String errorCode)
     */
    @Test
    public void testMessageErrorCode() {
        qe = new QueryException(message, strErrCode);
        
        assertEquals(strErrCode, qe.getErrorCode());
        assertEquals(404, qe.getStatusCode());
        assertEquals("message", qe.getLocalizedMessage());
        assertEquals("message", qe.getMessage());
    }
    
    /**
     * Tests constructor form of
     * 
     * public QueryException(Throwable cause)
     */
    @Test
    public void testThrowable() {
        qe = new QueryException(throwable);
        
        assertEquals("500-1", qe.getErrorCode());
        assertEquals(500, qe.getStatusCode());
        assertEquals(throwable.toString(), qe.getLocalizedMessage());
        assertEquals(throwable.toString(), qe.getMessage());
        assertEquals("throws", qe.getCause().getMessage());
    }
}
