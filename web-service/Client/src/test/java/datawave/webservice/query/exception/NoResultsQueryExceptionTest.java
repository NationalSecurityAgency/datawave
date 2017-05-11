package datawave.webservice.query.exception;

import javax.ws.rs.core.Response.Status;

import org.junit.Assert;
import org.junit.Test;

public class NoResultsQueryExceptionTest {
    
    private NoResultsQueryException nrqe;
    
    private final String message = "Bad query exception";
    private final Throwable throwable = new Throwable("throws");
    private final String strErrCode = "404-1";
    private final DatawaveErrorCode code = DatawaveErrorCode.QUERY_NAME_REQUIRED;
    private final Status status = Status.BAD_REQUEST;
    
    private final String assertMsg = "Param queryName is required. Bad query exception";
    private final String assertMsg2 = "Param queryName is required.";
    
    @Test
    public void testEmptyConstructor() {
        nrqe = new NoResultsQueryException();
        Assert.assertEquals(500, nrqe.getStatusCode());
        Assert.assertEquals("500-1", nrqe.getErrorCode());
        Assert.assertEquals(null, nrqe.getMessage());
        Assert.assertEquals(null, nrqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageConstructor() {
        nrqe = new NoResultsQueryException(message);
        Assert.assertEquals(500, nrqe.getStatusCode());
        Assert.assertEquals("500-1", nrqe.getErrorCode());
        Assert.assertEquals(message, nrqe.getMessage());
        Assert.assertEquals(message, nrqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageThrowableConstructor() {
        nrqe = new NoResultsQueryException(message, throwable);
        Assert.assertEquals(500, nrqe.getStatusCode());
        Assert.assertEquals("500-1", nrqe.getErrorCode());
        Assert.assertEquals(message, nrqe.getMessage());
        Assert.assertEquals(message, nrqe.getLocalizedMessage());
    }
    
    @Test
    public void testThrowableErrorCodeConstructor() {
        nrqe = new NoResultsQueryException(throwable, strErrCode);
        Assert.assertEquals(404, nrqe.getStatusCode());
        Assert.assertEquals("404-1", nrqe.getErrorCode());
        Assert.assertEquals(throwable.toString(), nrqe.getMessage());
        Assert.assertEquals(throwable.toString(), nrqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeThrowableConstructor() {
        nrqe = new NoResultsQueryException(code, throwable);
        Assert.assertEquals(400, nrqe.getStatusCode());
        Assert.assertEquals("400-1", nrqe.getErrorCode());
        Assert.assertEquals(assertMsg2, nrqe.getMessage());
        Assert.assertEquals(assertMsg2, nrqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeDebugMsgConstructor() {
        nrqe = new NoResultsQueryException(code, message);
        Assert.assertEquals(400, nrqe.getStatusCode());
        Assert.assertEquals("400-1", nrqe.getErrorCode());
        Assert.assertEquals(assertMsg, nrqe.getMessage());
        Assert.assertEquals(assertMsg, nrqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeThrowableDebugMsgConstructor() {
        nrqe = new NoResultsQueryException(code, throwable, message);
        Assert.assertEquals(400, nrqe.getStatusCode());
        Assert.assertEquals("400-1", nrqe.getErrorCode());
        Assert.assertEquals(assertMsg, nrqe.getMessage());
        Assert.assertEquals(assertMsg, nrqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeConstructor() {
        nrqe = new NoResultsQueryException(code);
        Assert.assertEquals(400, nrqe.getStatusCode());
        Assert.assertEquals("400-1", nrqe.getErrorCode());
        Assert.assertEquals(assertMsg2, nrqe.getMessage());
        Assert.assertEquals(assertMsg2, nrqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageResponseStatus() {
        nrqe = new NoResultsQueryException(message, status);
        Assert.assertEquals(400, nrqe.getStatusCode());
        Assert.assertEquals("400", nrqe.getErrorCode());
        Assert.assertEquals(message, nrqe.getMessage());
        Assert.assertEquals(message, nrqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageThrowableErrorCode() {
        nrqe = new NoResultsQueryException(message, throwable, strErrCode);
        Assert.assertEquals(404, nrqe.getStatusCode());
        Assert.assertEquals("404-1", nrqe.getErrorCode());
        Assert.assertEquals(message, nrqe.getMessage());
        Assert.assertEquals(message, nrqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageErrorCode() {
        nrqe = new NoResultsQueryException(message, strErrCode);
        Assert.assertEquals(404, nrqe.getStatusCode());
        Assert.assertEquals("404-1", nrqe.getErrorCode());
        Assert.assertEquals(message, nrqe.getMessage());
        Assert.assertEquals(message, nrqe.getLocalizedMessage());
    }
    
    @Test
    public void testThrowable() {
        nrqe = new NoResultsQueryException(throwable);
        Assert.assertEquals(500, nrqe.getStatusCode());
        Assert.assertEquals("500-1", nrqe.getErrorCode());
        Assert.assertEquals(throwable.toString(), nrqe.getMessage());
        Assert.assertEquals(throwable.toString(), nrqe.getLocalizedMessage());
    }
}
