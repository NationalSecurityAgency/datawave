package datawave.webservice.query.exception;

import org.junit.Assert;
import org.junit.Test;

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
        Assert.assertEquals(500, tqe.getStatusCode());
        Assert.assertEquals("500-1", tqe.getErrorCode());
        Assert.assertNull(tqe.getMessage());
        Assert.assertNull(tqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageConstructor() {
        tqe = new TimeoutQueryException(message);
        Assert.assertEquals(500, tqe.getStatusCode());
        Assert.assertEquals("500-1", tqe.getErrorCode());
        Assert.assertEquals(message, tqe.getMessage());
        Assert.assertEquals(message, tqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageThrowableConstructor() {
        tqe = new TimeoutQueryException(message, throwable);
        Assert.assertEquals(500, tqe.getStatusCode());
        Assert.assertEquals("500-1", tqe.getErrorCode());
        Assert.assertEquals(message, tqe.getMessage());
        Assert.assertEquals(message, tqe.getLocalizedMessage());
    }
    
    @Test
    public void testThrowableErrorCodeConstructor() {
        tqe = new TimeoutQueryException(throwable, strErrCode);
        Assert.assertEquals(500, tqe.getStatusCode());
        Assert.assertEquals("500-26", tqe.getErrorCode());
        Assert.assertEquals(throwable.toString(), tqe.getMessage());
        Assert.assertEquals(throwable.toString(), tqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeThrowableConstructor() {
        tqe = new TimeoutQueryException(code, throwable);
        Assert.assertEquals(500, tqe.getStatusCode());
        Assert.assertEquals("500-27", tqe.getErrorCode());
        Assert.assertEquals(assertMsg2, tqe.getMessage());
        Assert.assertEquals(assertMsg2, tqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeDebugMsgConstructor() {
        tqe = new TimeoutQueryException(code, message);
        Assert.assertEquals(500, tqe.getStatusCode());
        Assert.assertEquals("500-27", tqe.getErrorCode());
        Assert.assertEquals(assertMsg, tqe.getMessage());
        Assert.assertEquals(assertMsg, tqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeThrowableDebugMsgConstructor() {
        tqe = new TimeoutQueryException(code, throwable, message);
        Assert.assertEquals(500, tqe.getStatusCode());
        Assert.assertEquals("500-27", tqe.getErrorCode());
        Assert.assertEquals(assertMsg, tqe.getMessage());
        Assert.assertEquals(assertMsg, tqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeConstructor() {
        tqe = new TimeoutQueryException(code);
        Assert.assertEquals(500, tqe.getStatusCode());
        Assert.assertEquals("500-27", tqe.getErrorCode());
        Assert.assertEquals(assertMsg2, tqe.getMessage());
        Assert.assertEquals(assertMsg2, tqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageResponseStatus() {
        tqe = new TimeoutQueryException(message, 500);
        Assert.assertEquals(500, tqe.getStatusCode());
        Assert.assertEquals("500", tqe.getErrorCode());
        Assert.assertEquals(message, tqe.getMessage());
        Assert.assertEquals(message, tqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageThrowableErrorCode() {
        tqe = new TimeoutQueryException(message, throwable, strErrCode);
        Assert.assertEquals(500, tqe.getStatusCode());
        Assert.assertEquals("500-26", tqe.getErrorCode());
        Assert.assertEquals(message, tqe.getMessage());
        Assert.assertEquals(message, tqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageErrorCode() {
        tqe = new TimeoutQueryException(message, strErrCode);
        Assert.assertEquals(500, tqe.getStatusCode());
        Assert.assertEquals("500-26", tqe.getErrorCode());
        Assert.assertEquals(message, tqe.getMessage());
        Assert.assertEquals(message, tqe.getLocalizedMessage());
    }
    
    @Test
    public void testThrowable() {
        tqe = new TimeoutQueryException(throwable);
        Assert.assertEquals(500, tqe.getStatusCode());
        Assert.assertEquals("500-1", tqe.getErrorCode());
        Assert.assertEquals(throwable.toString(), tqe.getMessage());
        Assert.assertEquals(throwable.toString(), tqe.getLocalizedMessage());
    }
}
