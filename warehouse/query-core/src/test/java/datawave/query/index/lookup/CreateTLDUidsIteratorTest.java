package datawave.query.index.lookup;

import datawave.query.tld.TLD;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CreateTLDUidsIteratorTest {
    
    /*
     * The uids coming off the CreateUidsIterator is actually a concatenation of the datatype + uid.
     */
    @Test
    public void testParseRootPointerFromId() {
        String uidA = "datatypeA\u0000parent.document.id";
        String uidB = "datatypeB\u0000parent.document.id.child";
        
        assertEquals("datatypeA\u0000parent.document.id", TLD.parseRootPointerFromId(uidA));
        assertEquals("datatypeB\u0000parent.document.id", TLD.parseRootPointerFromId(uidB));
    }
}
