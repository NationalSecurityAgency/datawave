package nsa.datawave.webservice.operations.user;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;

import javax.ws.rs.core.MultivaluedMap;

import org.jboss.resteasy.specimpl.MultivaluedMapImpl;
import org.junit.Test;

public class LookupBeanTest {
    
    @Test
    public void testDeduplicateQueryParameters() {
        MultivaluedMap<String,String> dupes = new MultivaluedMapImpl<>();
        dupes.add("1", "A");
        dupes.add("1", "A");
        dupes.add("2", "B");
        dupes.add("2", "B");
        dupes.add("2", "C");
        dupes.add("3", "C");
        dupes.add("4", "A, B, C, D");
        
        MultivaluedMap<String,String> nodupes = LookupBean.deduplicateQueryParameters(dupes);
        
        assertEquals("Should still have 4 keys", 4, nodupes.entrySet().size());
        assertEquals("Key 1 should have 1 value that equals A", Collections.singletonList("A"), nodupes.get("1"));
        assertEquals("Key 2 should have 2 values, B and C", Arrays.asList("B", "C"), nodupes.get("2"));
        assertEquals("Key 3 should have 1 value that equals C", Collections.singletonList("C"), nodupes.get("3"));
        assertEquals("Key 4 should have 1 value 'A, B, C, D'", Collections.singletonList("A, B, C, D"), nodupes.get("4"));
        
    }
}
