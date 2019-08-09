package datawave.query.function;

import org.apache.accumulo.core.data.Key;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AncestorEqualityTest {
    private AncestorEquality equality;
    
    @Before
    public void setup() {
        equality = new AncestorEquality();
    }
    
    @Test
    public void testSameAccept() {
        Key key = new Key("abc", "1234.123.12345");
        Key other = new Key("abc", "1234.123.12345");
        assertTrue(equality.partOf(key, other));
    }
    
    @Test
    public void testParentAccept() {
        Key key = new Key("abc", "1234.123.12345");
        Key other = new Key("abc", "1234.123");
        assertTrue(equality.partOf(key, other));
    }
    
    @Test
    public void testRootAccept() {
        Key key = new Key("abc", "1234.123.12345");
        Key other = new Key("abc", "1234");
        assertTrue(equality.partOf(key, other));
    }
    
    @Test
    public void testSiblingReject() {
        Key key = new Key("abc", "1234.123.12345");
        Key other = new Key("abc", "1234.123.12346");
        assertFalse(equality.partOf(key, other));
    }
    
    @Test
    public void testSubstringReject() {
        Key key = new Key("abc", "1234.123.12345");
        Key other = new Key("abc", "1234.123.1234");
        assertFalse(equality.partOf(key, other));
    }
    
    @Test
    public void testChildReject() {
        Key key = new Key("abc", "1234.123.12345");
        Key other = new Key("abc", "1234.123.12345.1");
        assertFalse(equality.partOf(key, other));
    }
    
    @Test
    public void testGreatGrandChild() {
        Key key = new Key("abc", "parent.document.id.child.grandchild.greatgrandchild");
        Key other = new Key("abc", "parent.document.id.child.grandchild");
        assertTrue(equality.partOf(key, other));
    }
}
