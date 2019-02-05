package datawave.webservice.query.exception;

import org.junit.Assert;
import org.junit.Test;

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
        Assert.assertEquals(500, qcqe.getStatusCode());
        Assert.assertEquals("500-1", qcqe.getErrorCode());
        Assert.assertNull(qcqe.getMessage());
        Assert.assertNull(qcqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageConstructor() {
        qcqe = new QueryCanceledQueryException(message);
        Assert.assertEquals(500, qcqe.getStatusCode());
        Assert.assertEquals("500-1", qcqe.getErrorCode());
        Assert.assertEquals(message, qcqe.getMessage());
        Assert.assertEquals(message, qcqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageThrowableConstructor() {
        qcqe = new QueryCanceledQueryException(message, throwable);
        Assert.assertEquals(500, qcqe.getStatusCode());
        Assert.assertEquals("500-1", qcqe.getErrorCode());
        Assert.assertEquals(message, qcqe.getMessage());
        Assert.assertEquals(message, qcqe.getLocalizedMessage());
    }
    
    @Test
    public void testThrowableErrorCodeConstructor() {
        qcqe = new QueryCanceledQueryException(throwable, strErrCode);
        Assert.assertEquals(204, qcqe.getStatusCode());
        Assert.assertEquals("204-3", qcqe.getErrorCode());
        Assert.assertEquals(throwable.toString(), qcqe.getMessage());
        Assert.assertEquals(throwable.toString(), qcqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeThrowableConstructor() {
        qcqe = new QueryCanceledQueryException(code, throwable);
        Assert.assertEquals(204, qcqe.getStatusCode());
        Assert.assertEquals("204-3", qcqe.getErrorCode());
        Assert.assertEquals(assertMsg2, qcqe.getMessage());
        Assert.assertEquals(assertMsg2, qcqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeDebugMsgConstructor() {
        qcqe = new QueryCanceledQueryException(code, message);
        Assert.assertEquals(204, qcqe.getStatusCode());
        Assert.assertEquals("204-3", qcqe.getErrorCode());
        Assert.assertEquals(assertMsg, qcqe.getMessage());
        Assert.assertEquals(assertMsg, qcqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeThrowableDebugMsgConstructor() {
        qcqe = new QueryCanceledQueryException(code, throwable, message);
        Assert.assertEquals(204, qcqe.getStatusCode());
        Assert.assertEquals("204-3", qcqe.getErrorCode());
        Assert.assertEquals(assertMsg, qcqe.getMessage());
        Assert.assertEquals(assertMsg, qcqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeConstructor() {
        qcqe = new QueryCanceledQueryException(code);
        Assert.assertEquals(204, qcqe.getStatusCode());
        Assert.assertEquals("204-3", qcqe.getErrorCode());
        Assert.assertEquals(assertMsg2, qcqe.getMessage());
        Assert.assertEquals(assertMsg2, qcqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageResponseStatus() {
        qcqe = new QueryCanceledQueryException(message, 204);
        Assert.assertEquals(204, qcqe.getStatusCode());
        Assert.assertEquals("204", qcqe.getErrorCode());
        Assert.assertEquals(message, qcqe.getMessage());
        Assert.assertEquals(message, qcqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageThrowableErrorCode() {
        qcqe = new QueryCanceledQueryException(message, throwable, strErrCode);
        Assert.assertEquals(204, qcqe.getStatusCode());
        Assert.assertEquals("204-3", qcqe.getErrorCode());
        Assert.assertEquals(message, qcqe.getMessage());
        Assert.assertEquals(message, qcqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageErrorCode() {
        qcqe = new QueryCanceledQueryException(message, strErrCode);
        Assert.assertEquals(204, qcqe.getStatusCode());
        Assert.assertEquals("204-3", qcqe.getErrorCode());
        Assert.assertEquals(message, qcqe.getMessage());
        Assert.assertEquals(message, qcqe.getLocalizedMessage());
    }
    
    @Test
    public void testThrowable() {
        qcqe = new QueryCanceledQueryException(throwable);
        Assert.assertEquals(500, qcqe.getStatusCode());
        Assert.assertEquals("500-1", qcqe.getErrorCode());
        Assert.assertEquals(throwable.toString(), qcqe.getMessage());
        Assert.assertEquals(throwable.toString(), qcqe.getLocalizedMessage());
    }
}
