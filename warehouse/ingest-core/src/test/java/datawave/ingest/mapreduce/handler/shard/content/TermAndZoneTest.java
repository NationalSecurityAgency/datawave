package datawave.ingest.mapreduce.handler.shard.content;

import org.junit.Assert;

import org.junit.Test;

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
        
        Assert.assertEquals("TermAndZone Constructor failed to parse 'term' correctly from the token.", term, uut.term);
        Assert.assertEquals("TermAndZone Constructor failed to parse 'zone' correctly from the token.", zone, uut.zone);
        
        try {
            
            token = "Good-bye";
            
            uut = new TermAndZone(token);
            
            Assert.fail("TermAndZone Constructor failed to throw expected exception.");
            
        } catch (IllegalArgumentException iae) {
            
            String msg = iae.getMessage();
            
            Assert.assertTrue("TermAndZone Constructor threw the expected exception but it contained the wrong message.",
                            msg.startsWith("Missing zone in token: "));
        }
    }
    
    @Test
    public void testEquals() {
        
        String token = "Hello:World";
        String otherToken = "term:zone";
        
        TermAndZone uut = new TermAndZone(token);
        TermAndZone match = new TermAndZone(token);
        TermAndZone other = new TermAndZone(otherToken);
        Object obj = new Object();
        
        Assert.assertTrue("Equals failed to recognize a matching instance", uut.equals(match));
        Assert.assertFalse("Equals incorrectly matched different instance", uut.equals(other));
        Assert.assertFalse("Equals incorrectly matched a non-instance", uut.equals(obj));
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
        
        Assert.assertTrue("CompareTo failed to recognize a matching instance.", (0 == uut.compareTo(match)));
        Assert.assertTrue("CompareTo failed to recognize an instance containing a less than Zone value.", (0 > uut.compareTo(lessZone)));
        Assert.assertTrue("CompareTo failed to recognize an instance containing a less than Term value.", (0 > uut.compareTo(lessTerm)));
        Assert.assertTrue("CompareTo failed to recognize an instance containing a greater than Zone value.", (0 < uut.compareTo(greaterZone)));
        Assert.assertTrue("CompareTo failed to recognize an instance containing a greater than Term value.", (0 < uut.compareTo(greaterTerm)));
    }
}
