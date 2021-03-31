package datawave.core.iterators;

import org.apache.accumulo.core.data.Key;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DatawaveFieldIndexCachingIteratorJexlTest {
    
    @Test
    public void testUidMatching() {
        DatawaveFieldIndexCachingIteratorJexl iter = new DatawaveFieldIndexFilterIteratorJexl();
        
        Key tldKey = new Key("shard", "fi\0FIELD", "value\0datatype\0uid");
        Key childKey = new Key("shard", "fi\0FIELD", "value\0datatype\0uid.2");
        Key grandchildKey = new Key("shard", "fi\0FIELD", "value\0datatype\0uid.2.5");
        
        // tld uid matches
        String uid = "uid";
        assertTrue(iter.uidMatches(uid, tldKey));
        assertTrue(iter.uidMatches(uid, childKey));
        assertTrue(iter.uidMatches(uid, grandchildKey));
        
        // child uid matches
        uid = "uid.2";
        assertFalse(iter.uidMatches(uid, tldKey));
        assertTrue(iter.uidMatches(uid, childKey));
        assertTrue(iter.uidMatches(uid, grandchildKey));
        
        // grandchild uid matches
        uid = "uid.2.5";
        assertFalse(iter.uidMatches(uid, tldKey));
        assertFalse(iter.uidMatches(uid, childKey));
        assertTrue(iter.uidMatches(uid, grandchildKey));
        
        // alternate child uid should not match
        uid = "uid.7";
        assertFalse(iter.uidMatches(uid, tldKey));
        assertFalse(iter.uidMatches(uid, childKey));
        assertFalse(iter.uidMatches(uid, grandchildKey));
        
        // alternate grandchild uid should not match either
        uid = "uid.6.9";
        assertFalse(iter.uidMatches(uid, tldKey));
        assertFalse(iter.uidMatches(uid, childKey));
        assertFalse(iter.uidMatches(uid, grandchildKey));
    }
}
