package datawave.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

public class UniversalSetTest {
    
    Collection<String> universalSet = UniversalSet.instance();
    
    @Test
    public void testExpectedBehavior() {
        
        assertTrue(universalSet.isEmpty());
        
        assertTrue(universalSet.contains(new Object()));
        
        assertTrue(universalSet.containsAll(Sets.newHashSet("foo", "bar", "baz")));
        
        int originalSize = universalSet.size();
        
        assertFalse(universalSet.remove(this));
        assertEquals(universalSet.size(), originalSize);
        
        assertFalse(universalSet.removeAll(Sets.newHashSet(this)));
        assertEquals(universalSet.size(), originalSize);
        
        assertFalse(universalSet.add("trouble-maker"));
        assertEquals(universalSet.size(), originalSize);
        
        assertFalse(universalSet.addAll(Sets.newHashSet("foo", "bar", "baz")));
        assertEquals(universalSet.size(), originalSize);
        
        assertFalse(universalSet.retainAll(Sets.newHashSet("foo", "bar", "baz")));
        assertEquals(universalSet.size(), originalSize);
        
        assertFalse(universalSet.removeIf(x -> true));
        assertEquals(universalSet.size(), originalSize);
        
        Iterator<String> iterator = universalSet.iterator();
        assertFalse(iterator.hasNext());
        
        assertArrayEquals(universalSet.toArray(), new String[0]);
        
        assertArrayEquals(universalSet.toArray(new String[0]), new String[0]);
        
        assertEquals(universalSet.size(), 0);
        
        assertTrue(universalSet instanceof Set);
    }
}
