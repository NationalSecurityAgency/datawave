package datawave.webservice.query.exception;

import org.junit.Assert;
import org.junit.Test;

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
        Assert.assertEquals(500, tqe.getStatusCode());
        Assert.assertEquals("500-1", tqe.getErrorCode());
        Assert.assertNull(tqe.getMessage());
        Assert.assertNull(tqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageConstructor() {
        tqe = new UnauthorizedQueryException(message);
        Assert.assertEquals(500, tqe.getStatusCode());
        Assert.assertEquals("500-1", tqe.getErrorCode());
        Assert.assertEquals(message, tqe.getMessage());
        Assert.assertEquals(message, tqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageThrowableConstructor() {
        tqe = new UnauthorizedQueryException(message, throwable);
        Assert.assertEquals(500, tqe.getStatusCode());
        Assert.assertEquals("500-1", tqe.getErrorCode());
        Assert.assertEquals(message, tqe.getMessage());
        Assert.assertEquals(message, tqe.getLocalizedMessage());
    }
    
    @Test
    public void testThrowableErrorCodeConstructor() {
        tqe = new UnauthorizedQueryException(throwable, strErrCode);
        Assert.assertEquals(401, tqe.getStatusCode());
        Assert.assertEquals("401-1", tqe.getErrorCode());
        Assert.assertEquals(throwable.toString(), tqe.getMessage());
        Assert.assertEquals(throwable.toString(), tqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeThrowableConstructor() {
        tqe = new UnauthorizedQueryException(code, throwable);
        Assert.assertEquals(401, tqe.getStatusCode());
        Assert.assertEquals("401-1", tqe.getErrorCode());
        Assert.assertEquals(assertMsg2, tqe.getMessage());
        Assert.assertEquals(assertMsg2, tqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeDebugMsgConstructor() {
        tqe = new UnauthorizedQueryException(code, message);
        Assert.assertEquals(401, tqe.getStatusCode());
        Assert.assertEquals("401-1", tqe.getErrorCode());
        Assert.assertEquals(assertMsg, tqe.getMessage());
        Assert.assertEquals(assertMsg, tqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeThrowableDebugMsgConstructor() {
        tqe = new UnauthorizedQueryException(code, throwable, message);
        Assert.assertEquals(401, tqe.getStatusCode());
        Assert.assertEquals("401-1", tqe.getErrorCode());
        Assert.assertEquals(assertMsg, tqe.getMessage());
        Assert.assertEquals(assertMsg, tqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeConstructor() {
        tqe = new UnauthorizedQueryException(code);
        Assert.assertEquals(401, tqe.getStatusCode());
        Assert.assertEquals("401-1", tqe.getErrorCode());
        Assert.assertEquals(assertMsg2, tqe.getMessage());
        Assert.assertEquals(assertMsg2, tqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageResponseStatus() {
        tqe = new UnauthorizedQueryException(message, 401);
        Assert.assertEquals(401, tqe.getStatusCode());
        Assert.assertEquals("401", tqe.getErrorCode());
        Assert.assertEquals(message, tqe.getMessage());
        Assert.assertEquals(message, tqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageThrowableErrorCode() {
        tqe = new UnauthorizedQueryException(message, throwable, strErrCode);
        Assert.assertEquals(401, tqe.getStatusCode());
        Assert.assertEquals("401-1", tqe.getErrorCode());
        Assert.assertEquals(message, tqe.getMessage());
        Assert.assertEquals(message, tqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageErrorCode() {
        tqe = new UnauthorizedQueryException(message, strErrCode);
        Assert.assertEquals(401, tqe.getStatusCode());
        Assert.assertEquals("401-1", tqe.getErrorCode());
        Assert.assertEquals(message, tqe.getMessage());
        Assert.assertEquals(message, tqe.getLocalizedMessage());
    }
    
    @Test
    public void testThrowable() {
        tqe = new UnauthorizedQueryException(throwable);
        Assert.assertEquals(500, tqe.getStatusCode());
        Assert.assertEquals("500-1", tqe.getErrorCode());
        Assert.assertEquals(throwable.toString(), tqe.getMessage());
        Assert.assertEquals(throwable.toString(), tqe.getLocalizedMessage());
    }
    
}
