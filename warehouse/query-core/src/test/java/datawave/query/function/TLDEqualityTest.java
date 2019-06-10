package datawave.query.function;

import org.apache.accumulo.core.data.Key;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TLDEqualityTest {
    
    private TLDEquality equality = new TLDEquality();
    
    @Test
    public void testSameParent() {
        Key docKey = new Key("row", "parent.document.id");
        Key otherKey = new Key("row", "parent.document.id");
        assertTrue(equality.partOf(docKey, otherKey));
        assertTrue(equality.partOf(otherKey, docKey));
    }
    
    @Test
    public void testDifferentParents() {
        Key docKey = new Key("row", "parent.document.id");
        Key otherKey = new Key("row", "parent.document.id2");
        assertFalse(equality.partOf(docKey, otherKey));
        assertFalse(equality.partOf(otherKey, docKey));
    }
    
    @Test
    public void testKeysOfDifferentDepths() {
        Key docKey = new Key("row", "parent.document.id");
        Key otherKey = new Key("row", "parent.document.id.child");
        assertFalse(equality.partOf(docKey, otherKey));
        assertFalse(equality.partOf(otherKey, docKey));
    }
    
    @Test
    public void testSameParentSameChildren() {
        Key docKey = new Key("row", "parent.document.id.child");
        Key otherKey = new Key("row", "parent.document.id.child");
        assertTrue(equality.partOf(docKey, otherKey));
        assertTrue(equality.partOf(otherKey, docKey));
    }
    
    @Test
    public void testSameParentDifferentChildren() {
        Key docKey = new Key("row", "parent.document.id.child");
        Key otherKey = new Key("row", "parent.document.id.child2");
        assertFalse(equality.partOf(docKey, otherKey));
        assertFalse(equality.partOf(otherKey, docKey));
    }
    
    @Test
    public void testDifferentParentSameChildren() {
        Key docKey = new Key("row", "parent.document.id.child");
        Key otherKey = new Key("row", "parent.document.id2.child");
        assertFalse(equality.partOf(docKey, otherKey));
        assertFalse(equality.partOf(otherKey, docKey));
    }
    
    @Test
    public void testDifferentParentDifferentChildren() {
        Key docKey = new Key("row", "parent.document.id.child");
        Key otherKey = new Key("row", "parent.document.id2.child2");
        assertFalse(equality.partOf(docKey, otherKey));
        assertFalse(equality.partOf(otherKey, docKey));
    }
}
