package nsa.datawave.webservice.query.exception;

import javax.ws.rs.core.Response.Status;

import org.junit.Assert;
import org.junit.Test;

public class NotFoundQueryExceptionTest {
    
    private NotFoundQueryException nfqe;
    
    private final String message = "Bad query exception";
    private final Throwable throwable = new Throwable("throws");
    private final String strErrCode = "204-5";
    private final DatawaveErrorCode code = DatawaveErrorCode.QUERY_OR_VIEW_NOT_FOUND;
    private final Status status = Status.NOT_FOUND;
    
    private final String assertMsg = "Query/view not found. Bad query exception";
    private final String assertMsg2 = "Query/view not found.";
    
    @Test
    public void testEmptyConstructor() {
        nfqe = new NotFoundQueryException();
        Assert.assertEquals(500, nfqe.getStatusCode());
        Assert.assertEquals("500-1", nfqe.getErrorCode());
        Assert.assertEquals(null, nfqe.getMessage());
        Assert.assertEquals(null, nfqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageConstructor() {
        nfqe = new NotFoundQueryException(message);
        Assert.assertEquals(500, nfqe.getStatusCode());
        Assert.assertEquals("500-1", nfqe.getErrorCode());
        Assert.assertEquals(message, nfqe.getMessage());
        Assert.assertEquals(message, nfqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageThrowableConstructor() {
        nfqe = new NotFoundQueryException(message, throwable);
        Assert.assertEquals(500, nfqe.getStatusCode());
        Assert.assertEquals("500-1", nfqe.getErrorCode());
        Assert.assertEquals(message, nfqe.getMessage());
        Assert.assertEquals(message, nfqe.getLocalizedMessage());
    }
    
    @Test
    public void testThrowableErrorCodeConstructor() {
        nfqe = new NotFoundQueryException(throwable, strErrCode);
        Assert.assertEquals(204, nfqe.getStatusCode());
        Assert.assertEquals("204-5", nfqe.getErrorCode());
        Assert.assertEquals(throwable.toString(), nfqe.getMessage());
        Assert.assertEquals(throwable.toString(), nfqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeThrowableConstructor() {
        nfqe = new NotFoundQueryException(code, throwable);
        Assert.assertEquals(404, nfqe.getStatusCode());
        Assert.assertEquals("404-6", nfqe.getErrorCode());
        Assert.assertEquals(assertMsg2, nfqe.getMessage());
        Assert.assertEquals(assertMsg2, nfqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeDebugMsgConstructor() {
        nfqe = new NotFoundQueryException(code, message);
        Assert.assertEquals(404, nfqe.getStatusCode());
        Assert.assertEquals("404-6", nfqe.getErrorCode());
        Assert.assertEquals(assertMsg, nfqe.getMessage());
        Assert.assertEquals(assertMsg, nfqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeThrowableDebugMsgConstructor() {
        nfqe = new NotFoundQueryException(code, throwable, message);
        Assert.assertEquals(404, nfqe.getStatusCode());
        Assert.assertEquals("404-6", nfqe.getErrorCode());
        Assert.assertEquals(assertMsg, nfqe.getMessage());
        Assert.assertEquals(assertMsg, nfqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeConstructor() {
        nfqe = new NotFoundQueryException(code);
        Assert.assertEquals(404, nfqe.getStatusCode());
        Assert.assertEquals("404-6", nfqe.getErrorCode());
        Assert.assertEquals(assertMsg2, nfqe.getMessage());
        Assert.assertEquals(assertMsg2, nfqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageResponseStatus() {
        nfqe = new NotFoundQueryException(message, status);
        Assert.assertEquals(404, nfqe.getStatusCode());
        Assert.assertEquals("404", nfqe.getErrorCode());
        Assert.assertEquals(message, nfqe.getMessage());
        Assert.assertEquals(message, nfqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageThrowableErrorCode() {
        nfqe = new NotFoundQueryException(message, throwable, strErrCode);
        Assert.assertEquals(204, nfqe.getStatusCode());
        Assert.assertEquals("204-5", nfqe.getErrorCode());
        Assert.assertEquals(message, nfqe.getMessage());
        Assert.assertEquals(message, nfqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageErrorCode() {
        nfqe = new NotFoundQueryException(message, strErrCode);
        Assert.assertEquals(204, nfqe.getStatusCode());
        Assert.assertEquals("204-5", nfqe.getErrorCode());
        Assert.assertEquals(message, nfqe.getMessage());
        Assert.assertEquals(message, nfqe.getLocalizedMessage());
    }
    
    @Test
    public void testThrowable() {
        nfqe = new NotFoundQueryException(throwable);
        Assert.assertEquals(500, nfqe.getStatusCode());
        Assert.assertEquals("500-1", nfqe.getErrorCode());
        Assert.assertEquals(throwable.toString(), nfqe.getMessage());
        Assert.assertEquals(throwable.toString(), nfqe.getLocalizedMessage());
    }
}
