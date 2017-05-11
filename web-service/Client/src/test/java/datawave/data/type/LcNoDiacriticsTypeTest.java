package datawave.data.type;

import datawave.data.type.LcNoDiacriticsType;

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
        
        Assert.assertTrue(n1 == null);
        
    }
}
