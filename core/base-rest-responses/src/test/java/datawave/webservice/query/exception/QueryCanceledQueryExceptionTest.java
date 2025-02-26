package datawave.webservice.query.exception;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class QueryCanceledQueryExceptionTest {
    
    private QueryCanceledQueryException qcqe;
    
    private final String message = "Bad query exception";
    private final Throwable throwable = new Throwable("throws");
    private final String strErrCode = "204-3";
    private final DatawaveErrorCode code = DatawaveErrorCode.QUERY_CANCELED;
    
    private final String assertMsg = "Query was canceled. Bad query exception";
    private final String assertMsg2 = "Query was canceled.";
    
    @Test
    public void testEmptyConstructor() {
        qcqe = new QueryCanceledQueryException();
        assertEquals(500, qcqe.getStatusCode());
        assertEquals("500-1", qcqe.getErrorCode());
        assertNull(qcqe.getMessage());
        assertNull(qcqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageConstructor() {
        qcqe = new QueryCanceledQueryException(message);
        assertEquals(500, qcqe.getStatusCode());
        assertEquals("500-1", qcqe.getErrorCode());
        assertEquals(message, qcqe.getMessage());
        assertEquals(message, qcqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageThrowableConstructor() {
        qcqe = new QueryCanceledQueryException(message, throwable);
        assertEquals(500, qcqe.getStatusCode());
        assertEquals("500-1", qcqe.getErrorCode());
        assertEquals(message, qcqe.getMessage());
        assertEquals(message, qcqe.getLocalizedMessage());
    }
    
    @Test
    public void testThrowableErrorCodeConstructor() {
        qcqe = new QueryCanceledQueryException(throwable, strErrCode);
        assertEquals(204, qcqe.getStatusCode());
        assertEquals("204-3", qcqe.getErrorCode());
        assertEquals(throwable.toString(), qcqe.getMessage());
        assertEquals(throwable.toString(), qcqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeThrowableConstructor() {
        qcqe = new QueryCanceledQueryException(code, throwable);
        assertEquals(204, qcqe.getStatusCode());
        assertEquals("204-3", qcqe.getErrorCode());
        assertEquals(assertMsg2, qcqe.getMessage());
        assertEquals(assertMsg2, qcqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeDebugMsgConstructor() {
        qcqe = new QueryCanceledQueryException(code, message);
        assertEquals(204, qcqe.getStatusCode());
        assertEquals("204-3", qcqe.getErrorCode());
        assertEquals(assertMsg, qcqe.getMessage());
        assertEquals(assertMsg, qcqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeThrowableDebugMsgConstructor() {
        qcqe = new QueryCanceledQueryException(code, throwable, message);
        assertEquals(204, qcqe.getStatusCode());
        assertEquals("204-3", qcqe.getErrorCode());
        assertEquals(assertMsg, qcqe.getMessage());
        assertEquals(assertMsg, qcqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeConstructor() {
        qcqe = new QueryCanceledQueryException(code);
        assertEquals(204, qcqe.getStatusCode());
        assertEquals("204-3", qcqe.getErrorCode());
        assertEquals(assertMsg2, qcqe.getMessage());
        assertEquals(assertMsg2, qcqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageResponseStatus() {
        qcqe = new QueryCanceledQueryException(message, 204);
        assertEquals(204, qcqe.getStatusCode());
        assertEquals("204", qcqe.getErrorCode());
        assertEquals(message, qcqe.getMessage());
        assertEquals(message, qcqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageThrowableErrorCode() {
        qcqe = new QueryCanceledQueryException(message, throwable, strErrCode);
        assertEquals(204, qcqe.getStatusCode());
        assertEquals("204-3", qcqe.getErrorCode());
        assertEquals(message, qcqe.getMessage());
        assertEquals(message, qcqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageErrorCode() {
        qcqe = new QueryCanceledQueryException(message, strErrCode);
        assertEquals(204, qcqe.getStatusCode());
        assertEquals("204-3", qcqe.getErrorCode());
        assertEquals(message, qcqe.getMessage());
        assertEquals(message, qcqe.getLocalizedMessage());
    }
    
    @Test
    public void testThrowable() {
        qcqe = new QueryCanceledQueryException(throwable);
        assertEquals(500, qcqe.getStatusCode());
        assertEquals("500-1", qcqe.getErrorCode());
        assertEquals(throwable.toString(), qcqe.getMessage());
        assertEquals(throwable.toString(), qcqe.getLocalizedMessage());
    }
}
