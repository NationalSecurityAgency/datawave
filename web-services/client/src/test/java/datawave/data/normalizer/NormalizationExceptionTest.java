package datawave.data.normalizer;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class NormalizationExceptionTest {
    
    private NormalizationException ne;
    private Throwable throwable;
    private String message;
    
    @Before
    public void beforeTests() {
        message = "NormalizationException (hint: it's your fault)";
        throwable = new Throwable(message);
    }
    
    @Test
    public void testEmptyConstructor() {
        ne = new NormalizationException();
        
        Assert.assertNull(ne.getMessage());
        Assert.assertNull(ne.getLocalizedMessage());
    }
    
    @Test
    public void testMessageThrowableConstructor() {
        ne = new NormalizationException(message, throwable);
        
        Assert.assertEquals(message, ne.getMessage());
        Assert.assertEquals(message, ne.getLocalizedMessage());
    }
    
    @Test
    public void testMessageConstructor() {
        ne = new NormalizationException(message);
        
        Assert.assertEquals(message, ne.getMessage());
        Assert.assertEquals(message, ne.getLocalizedMessage());
    }
    
    @Test
    public void testThrowableConstructor() {
        ne = new NormalizationException(throwable);
        
        Assert.assertEquals(throwable.toString(), ne.getMessage());
        Assert.assertEquals(throwable.toString(), ne.getLocalizedMessage());
    }
}
