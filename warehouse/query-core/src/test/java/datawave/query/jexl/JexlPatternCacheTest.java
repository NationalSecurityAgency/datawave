package datawave.query.jexl;

import org.junit.Test;

import java.util.regex.Pattern;

import static org.junit.Assert.assertTrue;

public class JexlPatternCacheTest {
    
    @Test
    public void testDotAll() {
        Pattern p = JexlPatternCache.getPattern(".*word.*");
        assertTrue(p.matcher("bla word bla").matches());
        assertTrue(p.matcher("bla\nbla word bla\n bla").matches());
        p = JexlPatternCache.getPattern("(\\s|.)*word(\\s|.)*");
        assertTrue(p.matcher("bla\nbla word bla\n bla").matches());
    }
}
