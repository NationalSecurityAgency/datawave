package nsa.datawave.webservice.query.exception;

import javax.ws.rs.core.Response.Status;

import org.junit.Assert;
import org.junit.Test;

public class ShutdownQueryExceptionTest {
    
    private ShutdownQueryException sqe;
    
    private final String message = "Bad query exception";
    private final Throwable throwable = new Throwable("throws");
    private final String strErrCode = "500-26";
    private final DatawaveErrorCode code = DatawaveErrorCode.SERVER_SHUTDOWN;
    private final Status status = Status.INTERNAL_SERVER_ERROR;
    
    private final String assertMsg = "Server being shut down. Bad query exception";
    private final String assertMsg2 = "Server being shut down.";
    
    @Test
    public void testEmptyConstructor() {
        sqe = new ShutdownQueryException();
        Assert.assertEquals(500, sqe.getStatusCode());
        Assert.assertEquals("500-1", sqe.getErrorCode());
        Assert.assertEquals(null, sqe.getMessage());
        Assert.assertEquals(null, sqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageConstructor() {
        sqe = new ShutdownQueryException(message);
        Assert.assertEquals(500, sqe.getStatusCode());
        Assert.assertEquals("500-1", sqe.getErrorCode());
        Assert.assertEquals(message, sqe.getMessage());
        Assert.assertEquals(message, sqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageThrowableConstructor() {
        sqe = new ShutdownQueryException(message, throwable);
        Assert.assertEquals(500, sqe.getStatusCode());
        Assert.assertEquals("500-1", sqe.getErrorCode());
        Assert.assertEquals(message, sqe.getMessage());
        Assert.assertEquals(message, sqe.getLocalizedMessage());
    }
    
    @Test
    public void testThrowableErrorCodeConstructor() {
        sqe = new ShutdownQueryException(throwable, strErrCode);
        Assert.assertEquals(500, sqe.getStatusCode());
        Assert.assertEquals("500-26", sqe.getErrorCode());
        Assert.assertEquals(throwable.toString(), sqe.getMessage());
        Assert.assertEquals(throwable.toString(), sqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeThrowableConstructor() {
        sqe = new ShutdownQueryException(code, throwable);
        Assert.assertEquals(500, sqe.getStatusCode());
        Assert.assertEquals("500-26", sqe.getErrorCode());
        Assert.assertEquals(assertMsg2, sqe.getMessage());
        Assert.assertEquals(assertMsg2, sqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeDebugMsgConstructor() {
        sqe = new ShutdownQueryException(code, message);
        Assert.assertEquals(500, sqe.getStatusCode());
        Assert.assertEquals("500-26", sqe.getErrorCode());
        Assert.assertEquals(assertMsg, sqe.getMessage());
        Assert.assertEquals(assertMsg, sqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeThrowableDebugMsgConstructor() {
        sqe = new ShutdownQueryException(code, throwable, message);
        Assert.assertEquals(500, sqe.getStatusCode());
        Assert.assertEquals("500-26", sqe.getErrorCode());
        Assert.assertEquals(assertMsg, sqe.getMessage());
        Assert.assertEquals(assertMsg, sqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeConstructor() {
        sqe = new ShutdownQueryException(code);
        Assert.assertEquals(500, sqe.getStatusCode());
        Assert.assertEquals("500-26", sqe.getErrorCode());
        Assert.assertEquals(assertMsg2, sqe.getMessage());
        Assert.assertEquals(assertMsg2, sqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageResponseStatus() {
        sqe = new ShutdownQueryException(message, status);
        Assert.assertEquals(500, sqe.getStatusCode());
        Assert.assertEquals("500", sqe.getErrorCode());
        Assert.assertEquals(message, sqe.getMessage());
        Assert.assertEquals(message, sqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageThrowableErrorCode() {
        sqe = new ShutdownQueryException(message, throwable, strErrCode);
        Assert.assertEquals(500, sqe.getStatusCode());
        Assert.assertEquals("500-26", sqe.getErrorCode());
        Assert.assertEquals(message, sqe.getMessage());
        Assert.assertEquals(message, sqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageErrorCode() {
        sqe = new ShutdownQueryException(message, strErrCode);
        Assert.assertEquals(500, sqe.getStatusCode());
        Assert.assertEquals("500-26", sqe.getErrorCode());
        Assert.assertEquals(message, sqe.getMessage());
        Assert.assertEquals(message, sqe.getLocalizedMessage());
    }
    
    @Test
    public void testThrowable() {
        sqe = new ShutdownQueryException(throwable);
        Assert.assertEquals(500, sqe.getStatusCode());
        Assert.assertEquals("500-1", sqe.getErrorCode());
        Assert.assertEquals(throwable.toString(), sqe.getMessage());
        Assert.assertEquals(throwable.toString(), sqe.getLocalizedMessage());
    }
}
