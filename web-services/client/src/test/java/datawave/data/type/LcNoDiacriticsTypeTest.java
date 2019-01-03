package datawave.data.type;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 * 
 */
public class LcNoDiacriticsTypeTest {
    @Test
    public void test1() throws Exception {
        LcNoDiacriticsType norm = new LcNoDiacriticsType();
        String a = "field";
        String b = null;
        String n1 = norm.normalize(b);
        
        Assert.assertNull(n1);
        
    }
}
