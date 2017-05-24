package nsa.datawave.query.rewrite.function;

import org.apache.accumulo.core.data.Key;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AncestorEqualityTest {
    private AncestorEquality equality;
    
    @Before
    public void setup() {
        equality = new AncestorEquality();
    }
    
    @Test
    public void testSameAccept() {
        Assert.assertTrue(equality.partOf(new Key("abc", "1234.123.12345"), new Key("abc", "1234.123.12345")));
    }
    
    @Test
    public void testParentAccept() {
        Assert.assertTrue(equality.partOf(new Key("abc", "1234.123.12345"), new Key("abc", "1234.123")));
    }
    
    @Test
    public void testRootAccept() {
        Assert.assertTrue(equality.partOf(new Key("abc", "1234.123.12345"), new Key("abc", "1234")));
    }
    
    @Test
    public void testSiblingReject() {
        Assert.assertFalse(equality.partOf(new Key("abc", "1234.123.12345"), new Key("abc", "1234.123.12346")));
    }
    
    @Test
    public void testSubstringReject() {
        Assert.assertFalse(equality.partOf(new Key("abc", "1234.123.12345"), new Key("abc", "1234.123.1234")));
    }
    
    @Test
    public void testChildReject() {
        Assert.assertFalse(equality.partOf(new Key("abc", "1234.123.12345"), new Key("abc", "1234.123.12345.1")));
    }
}
