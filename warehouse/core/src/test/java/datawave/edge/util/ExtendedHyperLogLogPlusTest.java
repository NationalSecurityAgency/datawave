package datawave.edge.util;

import org.apache.accumulo.core.data.Value;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ExtendedHyperLogLogPlusTest {
    
    @Test
    @SuppressWarnings("static-method")
    public void test1() throws IOException {
        final ExtendedHyperLogLogPlus ehllp1 = new ExtendedHyperLogLogPlus();
        
        ehllp1.offer("string1.1");
        assertEquals(1, ehllp1.getCardinality(), "Test cardinality");
        
        ehllp1.offer("string1.1");
        assertEquals(1, ehllp1.getCardinality(), "Test cardinality");
        
        ehllp1.offer("string1.2");
        assertEquals(2, ehllp1.getCardinality(), "Test cardinality");
        
        final ExtendedHyperLogLogPlus deserialized = new ExtendedHyperLogLogPlus(new Value(ehllp1.getBytes()));
        
        assertEquals(ehllp1.getCardinality(), deserialized.getCardinality(), "Test serialization");
        
        final ExtendedHyperLogLogPlus ehllp2 = new ExtendedHyperLogLogPlus();
        
        ehllp2.offer("string2.1");
        assertEquals(1, ehllp2.getCardinality(), "Test cardinality");
        
        ehllp2.offer("string2.1");
        assertEquals(1, ehllp2.getCardinality(), "Test cardinality");
        
        ehllp2.offer("string2.2");
        assertEquals(2, ehllp2.getCardinality(), "Test cardinality");
        
        ehllp1.addAll(ehllp2);
        assertEquals(4, ehllp1.getCardinality(), "Test cardinality");
        
        ehllp1.clear();
        assertEquals(0, ehllp1.getCardinality(), "Test clear");
    }
    
    @Test
    @SuppressWarnings("static-method")
    public void test2() throws IOException {
        final ExtendedHyperLogLogPlus ehllp1 = new ExtendedHyperLogLogPlus();
        
        for (int i = 0; i < 4000; i++) {
            ehllp1.offer("string1." + i);
        }
        
        assertEquals(4063, ehllp1.getCardinality(), "Test cardinality");
        
        final ExtendedHyperLogLogPlus deserialized = new ExtendedHyperLogLogPlus(new Value(ehllp1.getBytes()));
        
        assertEquals(ehllp1.getCardinality(), deserialized.getCardinality(), "Test serialization");
    }
}
