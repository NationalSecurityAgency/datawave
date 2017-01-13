package nsa.datawave.webservice.query.exception;

import javax.ws.rs.core.Response.Status;

import org.junit.Assert;
import org.junit.Test;

public class BadRequestQueryExceptionTest {
    
    private BadRequestQueryException brqe;
    
    private final String message = "Bad query exception";
    private final Throwable throwable = new Throwable("throws");
    private final String strErrCode = "404-1";
    private final DatawaveErrorCode code = DatawaveErrorCode.QUERY_NAME_REQUIRED;
    private final Status status = Status.BAD_REQUEST;
    
    private final String assertMsg = "Param queryName is required. Bad query exception";
    private final String assertMsg2 = "Param queryName is required.";
    
    @Test
    public void testEmptyConstructor() {
        brqe = new BadRequestQueryException();
        Assert.assertEquals(500, brqe.getStatusCode());
        Assert.assertEquals("500-1", brqe.getErrorCode());
        Assert.assertEquals(null, brqe.getMessage());
        Assert.assertEquals(null, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageConstructor() {
        brqe = new BadRequestQueryException(message);
        Assert.assertEquals(500, brqe.getStatusCode());
        Assert.assertEquals("500-1", brqe.getErrorCode());
        Assert.assertEquals(message, brqe.getMessage());
        Assert.assertEquals(message, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageThrowableConstructor() {
        brqe = new BadRequestQueryException(message, throwable);
        Assert.assertEquals(500, brqe.getStatusCode());
        Assert.assertEquals("500-1", brqe.getErrorCode());
        Assert.assertEquals(message, brqe.getMessage());
        Assert.assertEquals(message, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testThrowableErrorCodeConstructor() {
        brqe = new BadRequestQueryException(throwable, strErrCode);
        Assert.assertEquals(404, brqe.getStatusCode());
        Assert.assertEquals("404-1", brqe.getErrorCode());
        Assert.assertEquals(throwable.toString(), brqe.getMessage());
        Assert.assertEquals(throwable.toString(), brqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeThrowableConstructor() {
        brqe = new BadRequestQueryException(code, throwable);
        Assert.assertEquals(400, brqe.getStatusCode());
        Assert.assertEquals("400-1", brqe.getErrorCode());
        Assert.assertEquals(assertMsg2, brqe.getMessage());
        Assert.assertEquals(assertMsg2, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeDebugMsgConstructor() {
        brqe = new BadRequestQueryException(code, message);
        Assert.assertEquals(400, brqe.getStatusCode());
        Assert.assertEquals("400-1", brqe.getErrorCode());
        Assert.assertEquals(assertMsg, brqe.getMessage());
        Assert.assertEquals(assertMsg, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeThrowableDebugMsgConstructor() {
        brqe = new BadRequestQueryException(code, throwable, message);
        Assert.assertEquals(400, brqe.getStatusCode());
        Assert.assertEquals("400-1", brqe.getErrorCode());
        Assert.assertEquals(assertMsg, brqe.getMessage());
        Assert.assertEquals(assertMsg, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeConstructor() {
        brqe = new BadRequestQueryException(code);
        Assert.assertEquals(400, brqe.getStatusCode());
        Assert.assertEquals("400-1", brqe.getErrorCode());
        Assert.assertEquals(assertMsg2, brqe.getMessage());
        Assert.assertEquals(assertMsg2, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageResponseStatus() {
        brqe = new BadRequestQueryException(message, status);
        Assert.assertEquals(400, brqe.getStatusCode());
        Assert.assertEquals("400", brqe.getErrorCode());
        Assert.assertEquals(message, brqe.getMessage());
        Assert.assertEquals(message, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageThrowableErrorCode() {
        brqe = new BadRequestQueryException(message, throwable, strErrCode);
        Assert.assertEquals(404, brqe.getStatusCode());
        Assert.assertEquals("404-1", brqe.getErrorCode());
        Assert.assertEquals(message, brqe.getMessage());
        Assert.assertEquals(message, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageErrorCode() {
        brqe = new BadRequestQueryException(message, strErrCode);
        Assert.assertEquals(404, brqe.getStatusCode());
        Assert.assertEquals("404-1", brqe.getErrorCode());
        Assert.assertEquals(message, brqe.getMessage());
        Assert.assertEquals(message, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testThrowable() {
        brqe = new BadRequestQueryException(throwable);
        Assert.assertEquals(500, brqe.getStatusCode());
        Assert.assertEquals("500-1", brqe.getErrorCode());
        Assert.assertEquals(throwable.toString(), brqe.getMessage());
        Assert.assertEquals(throwable.toString(), brqe.getLocalizedMessage());
    }
}
