package datawave.webservice.query.exception;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class TimeoutQueryExceptionTest {
    
    private TimeoutQueryException tqe;
    
    private final String message = "Bad query exception";
    private final Throwable throwable = new Throwable("throws");
    private final String strErrCode = "500-26";
    private final DatawaveErrorCode code = DatawaveErrorCode.QUERY_TIMEOUT;
    
    private final String assertMsg = "Query timed out. Bad query exception";
    private final String assertMsg2 = "Query timed out.";
    
    @Test
    public void testEmptyConstructor() {
        tqe = new TimeoutQueryException();
        assertEquals(500, tqe.getStatusCode());
        assertEquals("500-1", tqe.getErrorCode());
        assertNull(tqe.getMessage());
        assertNull(tqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageConstructor() {
        tqe = new TimeoutQueryException(message);
        assertEquals(500, tqe.getStatusCode());
        assertEquals("500-1", tqe.getErrorCode());
        assertEquals(message, tqe.getMessage());
        assertEquals(message, tqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageThrowableConstructor() {
        tqe = new TimeoutQueryException(message, throwable);
        assertEquals(500, tqe.getStatusCode());
        assertEquals("500-1", tqe.getErrorCode());
        assertEquals(message, tqe.getMessage());
        assertEquals(message, tqe.getLocalizedMessage());
    }
    
    @Test
    public void testThrowableErrorCodeConstructor() {
        tqe = new TimeoutQueryException(throwable, strErrCode);
        assertEquals(500, tqe.getStatusCode());
        assertEquals("500-26", tqe.getErrorCode());
        assertEquals(throwable.toString(), tqe.getMessage());
        assertEquals(throwable.toString(), tqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeThrowableConstructor() {
        tqe = new TimeoutQueryException(code, throwable);
        assertEquals(500, tqe.getStatusCode());
        assertEquals("500-27", tqe.getErrorCode());
        assertEquals(assertMsg2, tqe.getMessage());
        assertEquals(assertMsg2, tqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeDebugMsgConstructor() {
        tqe = new TimeoutQueryException(code, message);
        assertEquals(500, tqe.getStatusCode());
        assertEquals("500-27", tqe.getErrorCode());
        assertEquals(assertMsg, tqe.getMessage());
        assertEquals(assertMsg, tqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeThrowableDebugMsgConstructor() {
        tqe = new TimeoutQueryException(code, throwable, message);
        assertEquals(500, tqe.getStatusCode());
        assertEquals("500-27", tqe.getErrorCode());
        assertEquals(assertMsg, tqe.getMessage());
        assertEquals(assertMsg, tqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeConstructor() {
        tqe = new TimeoutQueryException(code);
        assertEquals(500, tqe.getStatusCode());
        assertEquals("500-27", tqe.getErrorCode());
        assertEquals(assertMsg2, tqe.getMessage());
        assertEquals(assertMsg2, tqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageResponseStatus() {
        tqe = new TimeoutQueryException(message, 500);
        assertEquals(500, tqe.getStatusCode());
        assertEquals("500", tqe.getErrorCode());
        assertEquals(message, tqe.getMessage());
        assertEquals(message, tqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageThrowableErrorCode() {
        tqe = new TimeoutQueryException(message, throwable, strErrCode);
        assertEquals(500, tqe.getStatusCode());
        assertEquals("500-26", tqe.getErrorCode());
        assertEquals(message, tqe.getMessage());
        assertEquals(message, tqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageErrorCode() {
        tqe = new TimeoutQueryException(message, strErrCode);
        assertEquals(500, tqe.getStatusCode());
        assertEquals("500-26", tqe.getErrorCode());
        assertEquals(message, tqe.getMessage());
        assertEquals(message, tqe.getLocalizedMessage());
    }
    
    @Test
    public void testThrowable() {
        tqe = new TimeoutQueryException(throwable);
        assertEquals(500, tqe.getStatusCode());
        assertEquals("500-1", tqe.getErrorCode());
        assertEquals(throwable.toString(), tqe.getMessage());
        assertEquals(throwable.toString(), tqe.getLocalizedMessage());
    }
}
