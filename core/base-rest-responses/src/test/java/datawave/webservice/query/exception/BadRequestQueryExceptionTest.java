package datawave.webservice.query.exception;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class BadRequestQueryExceptionTest {
    
    private BadRequestQueryException brqe;
    
    private final String message = "Bad query exception";
    private final Throwable throwable = new Throwable("throws");
    private final String strErrCode = "404-1";
    private final DatawaveErrorCode code = DatawaveErrorCode.QUERY_NAME_REQUIRED;
    
    private final String assertMsg = "Param queryName is required. Bad query exception";
    private final String assertMsg2 = "Param queryName is required.";
    
    @Test
    public void testEmptyConstructor() {
        brqe = new BadRequestQueryException();
        assertEquals(500, brqe.getStatusCode());
        assertEquals("500-1", brqe.getErrorCode());
        assertNull(brqe.getMessage());
        assertNull(brqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageConstructor() {
        brqe = new BadRequestQueryException(message);
        assertEquals(500, brqe.getStatusCode());
        assertEquals("500-1", brqe.getErrorCode());
        assertEquals(message, brqe.getMessage());
        assertEquals(message, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageThrowableConstructor() {
        brqe = new BadRequestQueryException(message, throwable);
        assertEquals(500, brqe.getStatusCode());
        assertEquals("500-1", brqe.getErrorCode());
        assertEquals(message, brqe.getMessage());
        assertEquals(message, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testThrowableErrorCodeConstructor() {
        brqe = new BadRequestQueryException(throwable, strErrCode);
        assertEquals(404, brqe.getStatusCode());
        assertEquals("404-1", brqe.getErrorCode());
        assertEquals(throwable.toString(), brqe.getMessage());
        assertEquals(throwable.toString(), brqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeThrowableConstructor() {
        brqe = new BadRequestQueryException(code, throwable);
        assertEquals(400, brqe.getStatusCode());
        assertEquals("400-1", brqe.getErrorCode());
        assertEquals(assertMsg2, brqe.getMessage());
        assertEquals(assertMsg2, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeDebugMsgConstructor() {
        brqe = new BadRequestQueryException(code, message);
        assertEquals(400, brqe.getStatusCode());
        assertEquals("400-1", brqe.getErrorCode());
        assertEquals(assertMsg, brqe.getMessage());
        assertEquals(assertMsg, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeThrowableDebugMsgConstructor() {
        brqe = new BadRequestQueryException(code, throwable, message);
        assertEquals(400, brqe.getStatusCode());
        assertEquals("400-1", brqe.getErrorCode());
        assertEquals(assertMsg, brqe.getMessage());
        assertEquals(assertMsg, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeConstructor() {
        brqe = new BadRequestQueryException(code);
        assertEquals(400, brqe.getStatusCode());
        assertEquals("400-1", brqe.getErrorCode());
        assertEquals(assertMsg2, brqe.getMessage());
        assertEquals(assertMsg2, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageResponseStatus() {
        brqe = new BadRequestQueryException(message, 400);
        assertEquals(400, brqe.getStatusCode());
        assertEquals("400", brqe.getErrorCode());
        assertEquals(message, brqe.getMessage());
        assertEquals(message, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageThrowableErrorCode() {
        brqe = new BadRequestQueryException(message, throwable, strErrCode);
        assertEquals(404, brqe.getStatusCode());
        assertEquals("404-1", brqe.getErrorCode());
        assertEquals(message, brqe.getMessage());
        assertEquals(message, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageErrorCode() {
        brqe = new BadRequestQueryException(message, strErrCode);
        assertEquals(404, brqe.getStatusCode());
        assertEquals("404-1", brqe.getErrorCode());
        assertEquals(message, brqe.getMessage());
        assertEquals(message, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testThrowable() {
        brqe = new BadRequestQueryException(throwable);
        assertEquals(500, brqe.getStatusCode());
        assertEquals("500-1", brqe.getErrorCode());
        assertEquals(throwable.toString(), brqe.getMessage());
        assertEquals(throwable.toString(), brqe.getLocalizedMessage());
    }
}
