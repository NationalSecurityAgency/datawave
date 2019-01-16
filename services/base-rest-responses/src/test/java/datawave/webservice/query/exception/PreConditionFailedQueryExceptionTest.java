package datawave.webservice.query.exception;

import org.junit.Assert;
import org.junit.Test;

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
        Assert.assertEquals(500, brqe.getStatusCode());
        Assert.assertEquals("500-1", brqe.getErrorCode());
        Assert.assertNull(brqe.getMessage());
        Assert.assertNull(brqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageConstructor() {
        brqe = new PreConditionFailedQueryException(message);
        Assert.assertEquals(500, brqe.getStatusCode());
        Assert.assertEquals("500-1", brqe.getErrorCode());
        Assert.assertEquals(message, brqe.getMessage());
        Assert.assertEquals(message, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageThrowableConstructor() {
        brqe = new PreConditionFailedQueryException(message, throwable);
        Assert.assertEquals(500, brqe.getStatusCode());
        Assert.assertEquals("500-1", brqe.getErrorCode());
        Assert.assertEquals(message, brqe.getMessage());
        Assert.assertEquals(message, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testThrowableErrorCodeConstructor() {
        brqe = new PreConditionFailedQueryException(throwable, strErrCode);
        Assert.assertEquals(412, brqe.getStatusCode());
        Assert.assertEquals("412-10", brqe.getErrorCode());
        Assert.assertEquals(throwable.toString(), brqe.getMessage());
        Assert.assertEquals(throwable.toString(), brqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeThrowableConstructor() {
        brqe = new PreConditionFailedQueryException(code, throwable);
        Assert.assertEquals(412, brqe.getStatusCode());
        Assert.assertEquals("412-10", brqe.getErrorCode());
        Assert.assertEquals(assertMsg2, brqe.getMessage());
        Assert.assertEquals(assertMsg2, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeDebugMsgConstructor() {
        brqe = new PreConditionFailedQueryException(code, message);
        Assert.assertEquals(412, brqe.getStatusCode());
        Assert.assertEquals("412-10", brqe.getErrorCode());
        Assert.assertEquals(assertMsg, brqe.getMessage());
        Assert.assertEquals(assertMsg, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeThrowableDebugMsgConstructor() {
        brqe = new PreConditionFailedQueryException(code, throwable, message);
        Assert.assertEquals(412, brqe.getStatusCode());
        Assert.assertEquals("412-10", brqe.getErrorCode());
        Assert.assertEquals(assertMsg, brqe.getMessage());
        Assert.assertEquals(assertMsg, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testDatawaveErrorCodeConstructor() {
        brqe = new PreConditionFailedQueryException(code);
        Assert.assertEquals(412, brqe.getStatusCode());
        Assert.assertEquals("412-10", brqe.getErrorCode());
        Assert.assertEquals(assertMsg2, brqe.getMessage());
        Assert.assertEquals(assertMsg2, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageResponseStatus() {
        brqe = new PreConditionFailedQueryException(message, 412);
        Assert.assertEquals(412, brqe.getStatusCode());
        Assert.assertEquals("412", brqe.getErrorCode());
        Assert.assertEquals(message, brqe.getMessage());
        Assert.assertEquals(message, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageThrowableErrorCode() {
        brqe = new PreConditionFailedQueryException(message, throwable, strErrCode);
        Assert.assertEquals(412, brqe.getStatusCode());
        Assert.assertEquals("412-10", brqe.getErrorCode());
        Assert.assertEquals(message, brqe.getMessage());
        Assert.assertEquals(message, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testMessageErrorCode() {
        brqe = new PreConditionFailedQueryException(message, strErrCode);
        Assert.assertEquals(412, brqe.getStatusCode());
        Assert.assertEquals("412-10", brqe.getErrorCode());
        Assert.assertEquals(message, brqe.getMessage());
        Assert.assertEquals(message, brqe.getLocalizedMessage());
    }
    
    @Test
    public void testThrowable() {
        brqe = new PreConditionFailedQueryException(throwable);
        Assert.assertEquals(500, brqe.getStatusCode());
        Assert.assertEquals("500-1", brqe.getErrorCode());
        Assert.assertEquals(throwable.toString(), brqe.getMessage());
        Assert.assertEquals(throwable.toString(), brqe.getLocalizedMessage());
    }
}
