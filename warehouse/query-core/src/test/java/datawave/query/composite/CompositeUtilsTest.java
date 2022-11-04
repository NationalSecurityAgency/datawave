package datawave.query.composite;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class CompositeUtilsTest {
    
    @Test
    public void incrementTest() {
        String start = "07393f";
        String finish = "073941";
        
        List<String> result = new ArrayList<>();
        for (String next = start; next.compareTo(finish) <= 0; next = CompositeUtils.incrementBound(next)) {
            if (isValidHex(next))
                result.add(next);
        }
        
        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals("07393f", result.get(0));
        Assertions.assertEquals("073940", result.get(1));
        Assertions.assertEquals("073941", result.get(2));
    }
    
    @Test
    public void decrementTest() {
        String start = "073940";
        String finish = "07393c";
        
        List<String> result = new ArrayList<>();
        for (String next = start; next.compareTo(finish) >= 0; next = CompositeUtils.decrementBound(next)) {
            if (isValidHex(next))
                result.add(next);
        }
        
        Assertions.assertEquals(5, result.size());
        Assertions.assertEquals("073940", result.get(0));
        Assertions.assertEquals("07393f", result.get(1));
        Assertions.assertEquals("07393e", result.get(2));
        Assertions.assertEquals("07393d", result.get(3));
        Assertions.assertEquals("07393c", result.get(4));
    }
    
    @Test
    public void decrementMinString() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            builder.appendCodePoint(Character.MIN_CODE_POINT);
        }
        String minString = builder.toString();
        
        Assertions.assertThrows(RuntimeException.class, () -> CompositeUtils.decrementBound(minString));
    }
    
    @Test
    public void incrementMaxString() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            builder.appendCodePoint(Character.MAX_CODE_POINT);
        }
        
        String minString = builder.toString();
        
        Assertions.assertThrows(RuntimeException.class, () -> CompositeUtils.incrementBound(minString));
    }
    
    private boolean isValidHex(String value) {
        for (char c : value.toCharArray()) {
            if (!((c >= 'a' && c <= 'f') || (c >= '0' && c <= '9')))
                return false;
        }
        return true;
    }
}
