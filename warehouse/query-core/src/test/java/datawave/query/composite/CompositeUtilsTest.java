package datawave.query.composite;

import org.junit.Assert;
import org.junit.Test;
import scala.math.BigInt;

import java.math.BigInteger;
import java.util.List;
import java.util.ArrayList;

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
        
        Assert.assertEquals(3, result.size());
        Assert.assertEquals("07393f", result.get(0));
        Assert.assertEquals("073940", result.get(1));
        Assert.assertEquals("073941", result.get(2));
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
        
        Assert.assertEquals(5, result.size());
        Assert.assertEquals("073940", result.get(0));
        Assert.assertEquals("07393f", result.get(1));
        Assert.assertEquals("07393e", result.get(2));
        Assert.assertEquals("07393d", result.get(3));
        Assert.assertEquals("07393c", result.get(4));
    }
    
    @Test(expected = RuntimeException.class)
    public void decrementMinString() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 10; i++)
            builder.appendCodePoint(Character.MIN_CODE_POINT);
        
        String minString = builder.toString();
        
        CompositeUtils.decrementBound(minString);
    }
    
    @Test(expected = RuntimeException.class)
    public void incrementMaxString() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 10; i++)
            builder.appendCodePoint(Character.MAX_CODE_POINT);
        
        String minString = builder.toString();
        
        CompositeUtils.incrementBound(minString);
    }
    
    private boolean isValidHex(String value) {
        for (char c : value.toCharArray()) {
            if (!((c >= 'a' && c <= 'f') || (c >= '0' && c <= '9')))
                return false;
        }
        return true;
    }
}
