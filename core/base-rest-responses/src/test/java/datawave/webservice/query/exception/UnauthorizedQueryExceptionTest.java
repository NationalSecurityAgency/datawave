package datawave.webservice.query.exception;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class UnauthorizedQueryExceptionTest {
    
    private UnauthorizedQueryException tqe;
    
    private final String message = "Bad query exception";
    private final Throwable throwable = new Throwable("throws");
    private final String strErrCode = "401-1";
    private final DatawaveErrorCode code = DatawaveErrorCode.QUERY_OWNER_MISMATCH;
    
    private final String assertMsg = "Current user does not match user that defined query. Bad query exception";
    private final String assertMsg2 = "Current user does not match user that defined query.";
    
    @Test
    public void testEmptyConstructor() {
        tqe = new UnauthorizedQueryException();
        assertEquals(500, tqe.getStatusCode());
        assertEquals("500-1", tqe.getErrorCode());
        assertNull(tqe.getMessage());
        assertNull(tqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageConstructor() {
        tqe = new UnauthorizedQueryException(message);
        assertEquals(500, tqe.getStatusCode());
        assertEquals("500-1", tqe.getErrorCode());
        assertEquals(message, tqe.getMessage());
        assertEquals(message, tqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageThrowableConstructor() {
        tqe = new UnauthorizedQueryException(message, throwable);
        assertEquals(500, tqe.getStatusCode());
        assertEquals("500-1", tqe.getErrorCode());
        assertEquals(message, tqe.getMessage());
        assertEquals(message, tqe.getLocalizedMessage());
    }
    
    @Test
    public void testThrowableErrorCodeConstructor() {
        tqe = new UnauthorizedQueryException(throwable, strErrCode);
        assertEquals(401, tqe.getStatusCode());
        assertEquals("401-1", tqe.getErrorCode());
        assertEquals(throwable.toString(), tqe.getMessage());
        assertEquals(throwable.toString(), tqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeThrowableConstructor() {
        tqe = new UnauthorizedQueryException(code, throwable);
        assertEquals(401, tqe.getStatusCode());
        assertEquals("401-1", tqe.getErrorCode());
        assertEquals(assertMsg2, tqe.getMessage());
        assertEquals(assertMsg2, tqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeDebugMsgConstructor() {
        tqe = new UnauthorizedQueryException(code, message);
        assertEquals(401, tqe.getStatusCode());
        assertEquals("401-1", tqe.getErrorCode());
        assertEquals(assertMsg, tqe.getMessage());
        assertEquals(assertMsg, tqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeThrowableDebugMsgConstructor() {
        tqe = new UnauthorizedQueryException(code, throwable, message);
        assertEquals(401, tqe.getStatusCode());
        assertEquals("401-1", tqe.getErrorCode());
        assertEquals(assertMsg, tqe.getMessage());
        assertEquals(assertMsg, tqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeConstructor() {
        tqe = new UnauthorizedQueryException(code);
        assertEquals(401, tqe.getStatusCode());
        assertEquals("401-1", tqe.getErrorCode());
        assertEquals(assertMsg2, tqe.getMessage());
        assertEquals(assertMsg2, tqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageResponseStatus() {
        tqe = new UnauthorizedQueryException(message, 401);
        assertEquals(401, tqe.getStatusCode());
        assertEquals("401", tqe.getErrorCode());
        assertEquals(message, tqe.getMessage());
        assertEquals(message, tqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageThrowableErrorCode() {
        tqe = new UnauthorizedQueryException(message, throwable, strErrCode);
        assertEquals(401, tqe.getStatusCode());
        assertEquals("401-1", tqe.getErrorCode());
        assertEquals(message, tqe.getMessage());
        assertEquals(message, tqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageErrorCode() {
        tqe = new UnauthorizedQueryException(message, strErrCode);
        assertEquals(401, tqe.getStatusCode());
        assertEquals("401-1", tqe.getErrorCode());
        assertEquals(message, tqe.getMessage());
        assertEquals(message, tqe.getLocalizedMessage());
    }
    
    @Test
    public void testThrowable() {
        tqe = new UnauthorizedQueryException(throwable);
        assertEquals(500, tqe.getStatusCode());
        assertEquals("500-1", tqe.getErrorCode());
        assertEquals(throwable.toString(), tqe.getMessage());
        assertEquals(throwable.toString(), tqe.getLocalizedMessage());
    }
    
}
