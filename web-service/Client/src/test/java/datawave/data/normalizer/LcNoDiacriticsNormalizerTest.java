package datawave.data.normalizer;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 * 
 */
public class LcNoDiacriticsNormalizerTest {
    @Test
    public void test1() throws Exception {
        LcNoDiacriticsNormalizer norm = new LcNoDiacriticsNormalizer();
        String a = "field";
        String b = null;
        String n1 = norm.normalize(b);
        
        Assert.assertTrue(n1 == null);
        
    }
}
