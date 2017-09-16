package datawave.data.type;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 * 
 */
public class TypeFactoryTest {
    
    @Test
    public void testWithCorrectType() throws Exception {
        Type<?> type = Type.Factory.createType("datawave.data.type.LcType");
        Assert.assertTrue(type instanceof LcType);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testWithIncorrectType() throws Exception {
        Type<?> type = Type.Factory.createType("datawave.ingest.data.normalizer.LcNoDiacriticsNormalizer");
    }
    
}
