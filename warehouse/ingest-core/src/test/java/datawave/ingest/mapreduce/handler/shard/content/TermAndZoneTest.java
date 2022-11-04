package datawave.ingest.mapreduce.handler.shard.content;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * 
 */
public class TermAndZoneTest {
    
    @Test
    public void testConstructor() {
        
        String term = "hello";
        String zone = "world";
        String token = String.format("%s:%s", term, zone);
        
        TermAndZone uut = new TermAndZone(token);
        
        Assertions.assertEquals(term, uut.term, "TermAndZone Constructor failed to parse 'term' correctly from the token.");
        Assertions.assertEquals(zone, uut.zone, "TermAndZone Constructor failed to parse 'zone' correctly from the token.");
        
        IllegalArgumentException iae = Assertions.assertThrows(IllegalArgumentException.class, () -> new TermAndZone("Good-bye"));
        Assertions.assertTrue(iae.getMessage().startsWith("Missing zone in token: "),
                        "TermAndZone Constructor threw the expected exception but it contained the wrong message.");
    }
    
    @Test
    public void testEquals() {
        
        String token = "Hello:World";
        String otherToken = "term:zone";
        
        TermAndZone uut = new TermAndZone(token);
        TermAndZone match = new TermAndZone(token);
        TermAndZone other = new TermAndZone(otherToken);
        Object obj = new Object();
        
        Assertions.assertTrue(uut.equals(match), "Equals failed to recognize a matching instance");
        Assertions.assertFalse(uut.equals(other), "Equals incorrectly matched different instance");
        Assertions.assertFalse(uut.equals(obj), "Equals incorrectly matched a non-instance");
    }
    
    @Test
    public void testCompare() {
        
        String token = "Hello:World";
        String lessZonetoken = "Hello:world";
        String greaterZonetoken = "Hello:WORLD";
        String lessTermToken = "term:World";
        String greaterTermToken = "Ello:World";
        
        TermAndZone uut = new TermAndZone(token);
        TermAndZone match = new TermAndZone(token);
        TermAndZone lessZone = new TermAndZone(lessZonetoken);
        TermAndZone greaterZone = new TermAndZone(greaterZonetoken);
        
        TermAndZone lessTerm = new TermAndZone(lessTermToken);
        TermAndZone greaterTerm = new TermAndZone(greaterTermToken);
        
        Assertions.assertTrue((0 == uut.compareTo(match)), "CompareTo failed to recognize a matching instance.");
        Assertions.assertTrue((0 > uut.compareTo(lessZone)), "CompareTo failed to recognize an instance containing a less than Zone value.");
        Assertions.assertTrue((0 > uut.compareTo(lessTerm)), "CompareTo failed to recognize an instance containing a less than Term value.");
        Assertions.assertTrue((0 < uut.compareTo(greaterZone)), "CompareTo failed to recognize an instance containing a greater than Zone value.");
        Assertions.assertTrue((0 < uut.compareTo(greaterTerm)), "CompareTo failed to recognize an instance containing a greater than Term value.");
    }
}
