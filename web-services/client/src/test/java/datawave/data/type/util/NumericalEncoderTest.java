package datawave.data.type.util;

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import datawave.data.type.util.NumericalEncoder;

public class NumericalEncoderTest {
    
    @Test
    public void testIsPossiblyEncoded() {
        Assert.assertFalse(NumericalEncoder.isPossiblyEncoded("1"));
        Assert.assertTrue(NumericalEncoder.isPossiblyEncoded("+1"));
        Assert.assertFalse(NumericalEncoder.isPossiblyEncoded(null));
        Assert.assertFalse(NumericalEncoder.isPossiblyEncoded(""));
        Assert.assertFalse(NumericalEncoder.isPossiblyEncoded(Long.valueOf(Long.MAX_VALUE).toString()));
    }
    
    @Test
    public void testEncode() {
        Assert.assertEquals("+aE5", NumericalEncoder.encode("5"));
        Assert.assertEquals("+aE6", NumericalEncoder.encode("6"));
        Assert.assertEquals("+dE1", NumericalEncoder.encode("1000"));
        Assert.assertEquals("+dE1.001", NumericalEncoder.encode("1001"));
        Assert.assertEquals("+eE1.0001", NumericalEncoder.encode("10001"));
        Assert.assertEquals("+fE1.00001", NumericalEncoder.encode("100001"));
        Assert.assertEquals("+gE1.000001", NumericalEncoder.encode("1000001"));
        Assert.assertEquals("+iE1.00000001", NumericalEncoder.encode("100000001"));
        Assert.assertEquals("+iE1.00000008", NumericalEncoder.encode("100000008"));
    }
    
    @Test
    public void testDecode() {
        for (long i = 0; i < 10000; i++) {
            Assert.assertEquals(i, NumericalEncoder.decode(NumericalEncoder.encode(Long.valueOf(i).toString())).longValue());
        }
        
    }
    
    @Test
    public void testDecodeBigNums() {
        for (long i = 5; i < Long.MAX_VALUE; i *= 1.0002) {
            Assert.assertEquals(i, NumericalEncoder.decode(NumericalEncoder.encode(Long.valueOf(i).toString())).longValue());
            i++;
        }
        
    }
    
    @Test
    public void testDecodeBigNumsRandomIncrement() {
        int increment = new Random().nextInt(9) + 1;
        for (long i = 1; i < Long.MAX_VALUE; i *= 1.0002) {
            Assert.assertEquals(i, NumericalEncoder.decode(NumericalEncoder.encode(Long.valueOf(i).toString())).longValue());
            i += increment;
        }
        
    }
}
