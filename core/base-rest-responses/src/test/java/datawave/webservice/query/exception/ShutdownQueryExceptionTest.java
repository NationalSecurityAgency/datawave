package datawave.webservice.query.exception;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class ShutdownQueryExceptionTest {
    
    private ShutdownQueryException sqe;
    
    private final String message = "Bad query exception";
    private final Throwable throwable = new Throwable("throws");
    private final String strErrCode = "500-26";
    private final DatawaveErrorCode code = DatawaveErrorCode.SERVER_SHUTDOWN;
    
    private final String assertMsg = "Server being shut down. Bad query exception";
    private final String assertMsg2 = "Server being shut down.";
    
    @Test
    public void testEmptyConstructor() {
        sqe = new ShutdownQueryException();
        assertEquals(500, sqe.getStatusCode());
        assertEquals("500-1", sqe.getErrorCode());
        assertNull(sqe.getMessage());
        assertNull(sqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageConstructor() {
        sqe = new ShutdownQueryException(message);
        assertEquals(500, sqe.getStatusCode());
        assertEquals("500-1", sqe.getErrorCode());
        assertEquals(message, sqe.getMessage());
        assertEquals(message, sqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageThrowableConstructor() {
        sqe = new ShutdownQueryException(message, throwable);
        assertEquals(500, sqe.getStatusCode());
        assertEquals("500-1", sqe.getErrorCode());
        assertEquals(message, sqe.getMessage());
        assertEquals(message, sqe.getLocalizedMessage());
    }
    
    @Test
    public void testThrowableErrorCodeConstructor() {
        sqe = new ShutdownQueryException(throwable, strErrCode);
        assertEquals(500, sqe.getStatusCode());
        assertEquals("500-26", sqe.getErrorCode());
        assertEquals(throwable.toString(), sqe.getMessage());
        assertEquals(throwable.toString(), sqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeThrowableConstructor() {
        sqe = new ShutdownQueryException(code, throwable);
        assertEquals(500, sqe.getStatusCode());
        assertEquals("500-26", sqe.getErrorCode());
        assertEquals(assertMsg2, sqe.getMessage());
        assertEquals(assertMsg2, sqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeDebugMsgConstructor() {
        sqe = new ShutdownQueryException(code, message);
        assertEquals(500, sqe.getStatusCode());
        assertEquals("500-26", sqe.getErrorCode());
        assertEquals(assertMsg, sqe.getMessage());
        assertEquals(assertMsg, sqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeThrowableDebugMsgConstructor() {
        sqe = new ShutdownQueryException(code, throwable, message);
        assertEquals(500, sqe.getStatusCode());
        assertEquals("500-26", sqe.getErrorCode());
        assertEquals(assertMsg, sqe.getMessage());
        assertEquals(assertMsg, sqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeConstructor() {
        sqe = new ShutdownQueryException(code);
        assertEquals(500, sqe.getStatusCode());
        assertEquals("500-26", sqe.getErrorCode());
        assertEquals(assertMsg2, sqe.getMessage());
        assertEquals(assertMsg2, sqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageResponseStatus() {
        sqe = new ShutdownQueryException(message, 500);
        assertEquals(500, sqe.getStatusCode());
        assertEquals("500", sqe.getErrorCode());
        assertEquals(message, sqe.getMessage());
        assertEquals(message, sqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageThrowableErrorCode() {
        sqe = new ShutdownQueryException(message, throwable, strErrCode);
        assertEquals(500, sqe.getStatusCode());
        assertEquals("500-26", sqe.getErrorCode());
        assertEquals(message, sqe.getMessage());
        assertEquals(message, sqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageErrorCode() {
        sqe = new ShutdownQueryException(message, strErrCode);
        assertEquals(500, sqe.getStatusCode());
        assertEquals("500-26", sqe.getErrorCode());
        assertEquals(message, sqe.getMessage());
        assertEquals(message, sqe.getLocalizedMessage());
    }
    
    @Test
    public void testThrowable() {
        sqe = new ShutdownQueryException(throwable);
        assertEquals(500, sqe.getStatusCode());
        assertEquals("500-1", sqe.getErrorCode());
        assertEquals(throwable.toString(), sqe.getMessage());
        assertEquals(throwable.toString(), sqe.getLocalizedMessage());
    }
}
