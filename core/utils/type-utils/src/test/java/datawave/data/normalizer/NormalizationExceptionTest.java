package datawave.data.normalizer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.data.type.NumberType;

public class NormalizationExceptionTest {
    
    private NormalizationException ne;
    private Throwable throwable;
    private String message;
    
    @BeforeEach
    public void beforeTests() {
        message = "NormalizationException (hint: it's your fault)";
        throwable = new Throwable(message);
    }
    
    @Test
    public void testPreEncodedValue() {
        NumberType type = new NumberType();
        assertEquals("+cE1.23", type.normalize("+cE1.23"));
    }
    
    @Test
    public void testEmptyConstructor() {
        ne = new NormalizationException();
        
        assertNull(ne.getMessage());
        assertNull(ne.getLocalizedMessage());
    }
    
    @Test
    public void testMessageThrowableConstructor() {
        ne = new NormalizationException(message, throwable);
        
        assertEquals(message, ne.getMessage());
        assertEquals(message, ne.getLocalizedMessage());
    }
    
    @Test
    public void testMessageConstructor() {
        ne = new NormalizationException(message);
        
        assertEquals(message, ne.getMessage());
        assertEquals(message, ne.getLocalizedMessage());
    }
    
    @Test
    public void testThrowableConstructor() {
        ne = new NormalizationException(throwable);
        
        assertEquals(throwable.toString(), ne.getMessage());
        assertEquals(throwable.toString(), ne.getLocalizedMessage());
    }
}
