package datawave.edge.util;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.accumulo.core.data.Value;
import org.junit.Test;

public class ExtendedHyperLogLogPlusTest {
    
    @Test
    @SuppressWarnings("static-method")
    public void test1() throws IOException {
        final ExtendedHyperLogLogPlus ehllp1 = new ExtendedHyperLogLogPlus();
        
        ehllp1.offer("string1.1");
        assertEquals("Test cardinality", 1, ehllp1.getCardinality());
        
        ehllp1.offer("string1.1");
        assertEquals("Test cardinality", 1, ehllp1.getCardinality());
        
        ehllp1.offer("string1.2");
        assertEquals("Test cardinality", 2, ehllp1.getCardinality());
        
        final ExtendedHyperLogLogPlus deserialized = new ExtendedHyperLogLogPlus(new Value(ehllp1.getBytes()));
        
        assertEquals("Test serialization", ehllp1.getCardinality(), deserialized.getCardinality());
        
        final ExtendedHyperLogLogPlus ehllp2 = new ExtendedHyperLogLogPlus();
        
        ehllp2.offer("string2.1");
        assertEquals("Test cardinality", 1, ehllp2.getCardinality());
        
        ehllp2.offer("string2.1");
        assertEquals("Test cardinality", 1, ehllp2.getCardinality());
        
        ehllp2.offer("string2.2");
        assertEquals("Test cardinality", 2, ehllp2.getCardinality());
        
        ehllp1.addAll(ehllp2);
        assertEquals("Test cardinality", 4, ehllp1.getCardinality());
        
        ehllp1.clear();
        assertEquals("Test clear", 0, ehllp1.getCardinality());
    }
    
    @Test
    @SuppressWarnings("static-method")
    public void test2() throws IOException {
        final ExtendedHyperLogLogPlus ehllp1 = new ExtendedHyperLogLogPlus();
        
        for (int i = 0; i < 4000; i++) {
            ehllp1.offer("string1." + i);
        }
        
        assertEquals("Test cardinality", 4063, ehllp1.getCardinality());
        
        final ExtendedHyperLogLogPlus deserialized = new ExtendedHyperLogLogPlus(new Value(ehllp1.getBytes()));
        
        assertEquals("Test serialization", ehllp1.getCardinality(), deserialized.getCardinality());
    }
}
