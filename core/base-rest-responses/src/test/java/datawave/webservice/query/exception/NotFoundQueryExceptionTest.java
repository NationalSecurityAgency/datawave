package datawave.webservice.query.exception;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class NotFoundQueryExceptionTest {
    
    private NotFoundQueryException nfqe;
    
    private final String message = "Bad query exception";
    private final Throwable throwable = new Throwable("throws");
    private final String strErrCode = "204-5";
    private final DatawaveErrorCode code = DatawaveErrorCode.QUERY_OR_VIEW_NOT_FOUND;
    
    private final String assertMsg = "Query/view not found. Bad query exception";
    private final String assertMsg2 = "Query/view not found.";
    
    @Test
    public void testEmptyConstructor() {
        nfqe = new NotFoundQueryException();
        assertEquals(500, nfqe.getStatusCode());
        assertEquals("500-1", nfqe.getErrorCode());
        assertNull(nfqe.getMessage());
        assertNull(nfqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageConstructor() {
        nfqe = new NotFoundQueryException(message);
        assertEquals(500, nfqe.getStatusCode());
        assertEquals("500-1", nfqe.getErrorCode());
        assertEquals(message, nfqe.getMessage());
        assertEquals(message, nfqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageThrowableConstructor() {
        nfqe = new NotFoundQueryException(message, throwable);
        assertEquals(500, nfqe.getStatusCode());
        assertEquals("500-1", nfqe.getErrorCode());
        assertEquals(message, nfqe.getMessage());
        assertEquals(message, nfqe.getLocalizedMessage());
    }
    
    @Test
    public void testThrowableErrorCodeConstructor() {
        nfqe = new NotFoundQueryException(throwable, strErrCode);
        assertEquals(204, nfqe.getStatusCode());
        assertEquals("204-5", nfqe.getErrorCode());
        assertEquals(throwable.toString(), nfqe.getMessage());
        assertEquals(throwable.toString(), nfqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeThrowableConstructor() {
        nfqe = new NotFoundQueryException(code, throwable);
        assertEquals(404, nfqe.getStatusCode());
        assertEquals("404-6", nfqe.getErrorCode());
        assertEquals(assertMsg2, nfqe.getMessage());
        assertEquals(assertMsg2, nfqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeDebugMsgConstructor() {
        nfqe = new NotFoundQueryException(code, message);
        assertEquals(404, nfqe.getStatusCode());
        assertEquals("404-6", nfqe.getErrorCode());
        assertEquals(assertMsg, nfqe.getMessage());
        assertEquals(assertMsg, nfqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeThrowableDebugMsgConstructor() {
        nfqe = new NotFoundQueryException(code, throwable, message);
        assertEquals(404, nfqe.getStatusCode());
        assertEquals("404-6", nfqe.getErrorCode());
        assertEquals(assertMsg, nfqe.getMessage());
        assertEquals(assertMsg, nfqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeConstructor() {
        nfqe = new NotFoundQueryException(code);
        assertEquals(404, nfqe.getStatusCode());
        assertEquals("404-6", nfqe.getErrorCode());
        assertEquals(assertMsg2, nfqe.getMessage());
        assertEquals(assertMsg2, nfqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageResponseStatus() {
        nfqe = new NotFoundQueryException(message, 404);
        assertEquals(404, nfqe.getStatusCode());
        assertEquals("404", nfqe.getErrorCode());
        assertEquals(message, nfqe.getMessage());
        assertEquals(message, nfqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageThrowableErrorCode() {
        nfqe = new NotFoundQueryException(message, throwable, strErrCode);
        assertEquals(204, nfqe.getStatusCode());
        assertEquals("204-5", nfqe.getErrorCode());
        assertEquals(message, nfqe.getMessage());
        assertEquals(message, nfqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageErrorCode() {
        nfqe = new NotFoundQueryException(message, strErrCode);
        assertEquals(204, nfqe.getStatusCode());
        assertEquals("204-5", nfqe.getErrorCode());
        assertEquals(message, nfqe.getMessage());
        assertEquals(message, nfqe.getLocalizedMessage());
    }
    
    @Test
    public void testThrowable() {
        nfqe = new NotFoundQueryException(throwable);
        assertEquals(500, nfqe.getStatusCode());
        assertEquals("500-1", nfqe.getErrorCode());
        assertEquals(throwable.toString(), nfqe.getMessage());
        assertEquals(throwable.toString(), nfqe.getLocalizedMessage());
    }
}
