package datawave.webservice.query.data;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test that object sizeof mechanism
 */
public class ObjectSizeOfTest {
    
    /**
     * @throws java.lang.Exception
     */
    @BeforeEach
    public void setUp() throws Exception {}
    
    /**
     * @throws java.lang.Exception
     */
    @AfterEach
    public void tearDown() throws Exception {}
    
    @Test
    public void testPrimitives() {
        assertEquals(0, ObjectSizeOf.Sizer.getPrimitiveObjectSize(void.class));
        assertEquals(1, ObjectSizeOf.Sizer.getPrimitiveObjectSize(boolean.class));
        assertEquals(1, ObjectSizeOf.Sizer.getPrimitiveObjectSize(byte.class));
        assertEquals(2, ObjectSizeOf.Sizer.getPrimitiveObjectSize(char.class));
        assertEquals(2, ObjectSizeOf.Sizer.getPrimitiveObjectSize(short.class));
        assertEquals(4, ObjectSizeOf.Sizer.getPrimitiveObjectSize(int.class));
        assertEquals(4, ObjectSizeOf.Sizer.getPrimitiveObjectSize(float.class));
        assertEquals(8, ObjectSizeOf.Sizer.getPrimitiveObjectSize(long.class));
        assertEquals(8, ObjectSizeOf.Sizer.getPrimitiveObjectSize(double.class));
    }
    
    @Test
    public void testRoundUp() {
        assertEquals(0, ObjectSizeOf.Sizer.roundUp(0));
        assertEquals(8, ObjectSizeOf.Sizer.roundUp(1));
        assertEquals(8, ObjectSizeOf.Sizer.roundUp(2));
        assertEquals(8, ObjectSizeOf.Sizer.roundUp(3));
        assertEquals(8, ObjectSizeOf.Sizer.roundUp(4));
        assertEquals(8, ObjectSizeOf.Sizer.roundUp(5));
        assertEquals(8, ObjectSizeOf.Sizer.roundUp(6));
        assertEquals(8, ObjectSizeOf.Sizer.roundUp(7));
        assertEquals(8, ObjectSizeOf.Sizer.roundUp(8));
        assertEquals(16, ObjectSizeOf.Sizer.roundUp(9));
        assertEquals(16, ObjectSizeOf.Sizer.roundUp(10));
        assertEquals(16, ObjectSizeOf.Sizer.roundUp(11));
        assertEquals(88, ObjectSizeOf.Sizer.roundUp(81));
        assertEquals(168, ObjectSizeOf.Sizer.roundUp(165));
    }
    
    @Test
    public void testNumbers() {
        assertEquals(16, ObjectSizeOf.Sizer.getObjectSize(Boolean.TRUE));
        assertEquals(16, ObjectSizeOf.Sizer.getObjectSize(new Byte((byte) 1)));
        assertEquals(16, ObjectSizeOf.Sizer.getObjectSize(new Character((char) 1)));
        assertEquals(16, ObjectSizeOf.Sizer.getObjectSize(new Short((short) 1)));
        assertEquals(16, ObjectSizeOf.Sizer.getObjectSize(new Integer(1)));
        assertEquals(16, ObjectSizeOf.Sizer.getObjectSize(new Float(1)));
        assertEquals(16, ObjectSizeOf.Sizer.getObjectSize(new Long(1)));
        assertEquals(16, ObjectSizeOf.Sizer.getObjectSize(new Double(1)));
    }
    
    @Test
    public void testObjects() {
        List<Object> list = new ArrayList<Object>(10);
        list.add(new Long(1));
        list.add(new Double(1));
        int overhead = 8;
        int arrayoverhead = 12;
        int reference = 4;
        int intsize = 4;
        int numbersize = 16;
        long size = ObjectSizeOf.Sizer.roundUp(overhead + intsize + intsize + reference) + ObjectSizeOf.Sizer.roundUp(arrayoverhead + 10 * reference)
                        + numbersize + numbersize;
        assertEquals(size, ObjectSizeOf.Sizer.getObjectSize(list));
        
        PrimitiveObject testPrimitive = new PrimitiveObject();
        size = numbersize;
        assertEquals(size, ObjectSizeOf.Sizer.getObjectSize(testPrimitive));
        
        ObjectSizeOf testSized = new SizedObject();
        size = ObjectSizeOf.Sizer.roundUp(testSized.sizeInBytes());
        assertEquals(size, ObjectSizeOf.Sizer.getObjectSize(testSized));
        
        RecursiveObject recursiveObject = new RecursiveObject();
        recursiveObject.o = recursiveObject;
        size = ObjectSizeOf.Sizer.roundUp(overhead + reference);
        assertEquals(size, ObjectSizeOf.Sizer.getObjectSize(recursiveObject));
    }
    
    public static class PrimitiveObject {
        private long value = 0;
    }
    
    public static class SizedObject implements ObjectSizeOf {
        @Override
        public long sizeInBytes() {
            return ObjectSizeOf.Sizer.roundUp(20);
        }
        
    }
    
    public static class RecursiveObject {
        public Object o;
    }
}
