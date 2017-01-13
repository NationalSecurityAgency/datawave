package nsa.datawave.data.type;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 * 
 */
public class TypeFactoryTest {
    
    @Test
    public void testWithCorrectType() throws Exception {
        Type<?> type = Type.Factory.createType("nsa.datawave.data.type.LcType");
        Assert.assertTrue(type instanceof LcType);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testWithIncorrectType() throws Exception {
        Type<?> type = Type.Factory.createType("nsa.datawave.ingest.data.normalizer.LcNoDiacriticsNormalizer");
    }
    
}
