package datawave.webservice.query.exception;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class PreConditionFailedQueryExceptionTest {
    
    private PreConditionFailedQueryException brqe;
    
    private final String message = "Bad query exception";
    private final Throwable throwable = new Throwable("throws");
    private final String strErrCode = "412-10";
    private final DatawaveErrorCode code = DatawaveErrorCode.QUERY_TERM_THRESHOLD_EXCEEDED;
    
    private final String assertMsg = "Query failed because it exceeded the query term threshold. Bad query exception";
    private final String assertMsg2 = "Query failed because it exceeded the query term threshold.";
    
    @Test
    public void testEmptyConstructor() {
        brqe = new PreConditionFailedQueryException();
        assertEquals(500, brqe.getStatusCode());
        assertEquals("500-1", brqe.getErrorCode());
        assertNull(brqe.getMessage());
        assertNull(brqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageConstructor() {
        brqe = new PreConditionFailedQueryException(message);
        assertEquals(500, brqe.getStatusCode());
        assertEquals("500-1", brqe.getErrorCode());
        assertEquals(message, brqe.getMessage());
        assertEquals(message, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageThrowableConstructor() {
        brqe = new PreConditionFailedQueryException(message, throwable);
        assertEquals(500, brqe.getStatusCode());
        assertEquals("500-1", brqe.getErrorCode());
        assertEquals(message, brqe.getMessage());
        assertEquals(message, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testThrowableErrorCodeConstructor() {
        brqe = new PreConditionFailedQueryException(throwable, strErrCode);
        assertEquals(412, brqe.getStatusCode());
        assertEquals("412-10", brqe.getErrorCode());
        assertEquals(throwable.toString(), brqe.getMessage());
        assertEquals(throwable.toString(), brqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeThrowableConstructor() {
        brqe = new PreConditionFailedQueryException(code, throwable);
        assertEquals(412, brqe.getStatusCode());
        assertEquals("412-10", brqe.getErrorCode());
        assertEquals(assertMsg2, brqe.getMessage());
        assertEquals(assertMsg2, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeDebugMsgConstructor() {
        brqe = new PreConditionFailedQueryException(code, message);
        assertEquals(412, brqe.getStatusCode());
        assertEquals("412-10", brqe.getErrorCode());
        assertEquals(assertMsg, brqe.getMessage());
        assertEquals(assertMsg, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeThrowableDebugMsgConstructor() {
        brqe = new PreConditionFailedQueryException(code, throwable, message);
        assertEquals(412, brqe.getStatusCode());
        assertEquals("412-10", brqe.getErrorCode());
        assertEquals(assertMsg, brqe.getMessage());
        assertEquals(assertMsg, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeConstructor() {
        brqe = new PreConditionFailedQueryException(code);
        assertEquals(412, brqe.getStatusCode());
        assertEquals("412-10", brqe.getErrorCode());
        assertEquals(assertMsg2, brqe.getMessage());
        assertEquals(assertMsg2, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageResponseStatus() {
        brqe = new PreConditionFailedQueryException(message, 412);
        assertEquals(412, brqe.getStatusCode());
        assertEquals("412", brqe.getErrorCode());
        assertEquals(message, brqe.getMessage());
        assertEquals(message, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageThrowableErrorCode() {
        brqe = new PreConditionFailedQueryException(message, throwable, strErrCode);
        assertEquals(412, brqe.getStatusCode());
        assertEquals("412-10", brqe.getErrorCode());
        assertEquals(message, brqe.getMessage());
        assertEquals(message, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageErrorCode() {
        brqe = new PreConditionFailedQueryException(message, strErrCode);
        assertEquals(412, brqe.getStatusCode());
        assertEquals("412-10", brqe.getErrorCode());
        assertEquals(message, brqe.getMessage());
        assertEquals(message, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testThrowable() {
        brqe = new PreConditionFailedQueryException(throwable);
        assertEquals(500, brqe.getStatusCode());
        assertEquals("500-1", brqe.getErrorCode());
        assertEquals(throwable.toString(), brqe.getMessage());
        assertEquals(throwable.toString(), brqe.getLocalizedMessage());
    }
}
