package datawave.webservice.query.exception;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class NoResultsQueryExceptionTest {
    
    private NoResultsQueryException nrqe;
    
    private final String message = "Bad query exception";
    private final Throwable throwable = new Throwable("throws");
    private final String strErrCode = "404-1";
    private final DatawaveErrorCode code = DatawaveErrorCode.QUERY_NAME_REQUIRED;
    
    private final String assertMsg = "Param queryName is required. Bad query exception";
    private final String assertMsg2 = "Param queryName is required.";
    
    @Test
    public void testEmptyConstructor() {
        nrqe = new NoResultsQueryException();
        assertEquals(500, nrqe.getStatusCode());
        assertEquals("500-1", nrqe.getErrorCode());
        assertNull(nrqe.getMessage());
        assertNull(nrqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageConstructor() {
        nrqe = new NoResultsQueryException(message);
        assertEquals(500, nrqe.getStatusCode());
        assertEquals("500-1", nrqe.getErrorCode());
        assertEquals(message, nrqe.getMessage());
        assertEquals(message, nrqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageThrowableConstructor() {
        nrqe = new NoResultsQueryException(message, throwable);
        assertEquals(500, nrqe.getStatusCode());
        assertEquals("500-1", nrqe.getErrorCode());
        assertEquals(message, nrqe.getMessage());
        assertEquals(message, nrqe.getLocalizedMessage());
    }
    
    @Test
    public void testThrowableErrorCodeConstructor() {
        nrqe = new NoResultsQueryException(throwable, strErrCode);
        assertEquals(404, nrqe.getStatusCode());
        assertEquals("404-1", nrqe.getErrorCode());
        assertEquals(throwable.toString(), nrqe.getMessage());
        assertEquals(throwable.toString(), nrqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeThrowableConstructor() {
        nrqe = new NoResultsQueryException(code, throwable);
        assertEquals(400, nrqe.getStatusCode());
        assertEquals("400-1", nrqe.getErrorCode());
        assertEquals(assertMsg2, nrqe.getMessage());
        assertEquals(assertMsg2, nrqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeDebugMsgConstructor() {
        nrqe = new NoResultsQueryException(code, message);
        assertEquals(400, nrqe.getStatusCode());
        assertEquals("400-1", nrqe.getErrorCode());
        assertEquals(assertMsg, nrqe.getMessage());
        assertEquals(assertMsg, nrqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeThrowableDebugMsgConstructor() {
        nrqe = new NoResultsQueryException(code, throwable, message);
        assertEquals(400, nrqe.getStatusCode());
        assertEquals("400-1", nrqe.getErrorCode());
        assertEquals(assertMsg, nrqe.getMessage());
        assertEquals(assertMsg, nrqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeConstructor() {
        nrqe = new NoResultsQueryException(code);
        assertEquals(400, nrqe.getStatusCode());
        assertEquals("400-1", nrqe.getErrorCode());
        assertEquals(assertMsg2, nrqe.getMessage());
        assertEquals(assertMsg2, nrqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageResponseStatus() {
        nrqe = new NoResultsQueryException(message, 400);
        assertEquals(400, nrqe.getStatusCode());
        assertEquals("400", nrqe.getErrorCode());
        assertEquals(message, nrqe.getMessage());
        assertEquals(message, nrqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageThrowableErrorCode() {
        nrqe = new NoResultsQueryException(message, throwable, strErrCode);
        assertEquals(404, nrqe.getStatusCode());
        assertEquals("404-1", nrqe.getErrorCode());
        assertEquals(message, nrqe.getMessage());
        assertEquals(message, nrqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageErrorCode() {
        nrqe = new NoResultsQueryException(message, strErrCode);
        assertEquals(404, nrqe.getStatusCode());
        assertEquals("404-1", nrqe.getErrorCode());
        assertEquals(message, nrqe.getMessage());
        assertEquals(message, nrqe.getLocalizedMessage());
    }
    
    @Test
    public void testThrowable() {
        nrqe = new NoResultsQueryException(throwable);
        assertEquals(500, nrqe.getStatusCode());
        assertEquals("500-1", nrqe.getErrorCode());
        assertEquals(throwable.toString(), nrqe.getMessage());
        assertEquals(throwable.toString(), nrqe.getLocalizedMessage());
    }
}
