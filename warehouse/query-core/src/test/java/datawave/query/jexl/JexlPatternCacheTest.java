package datawave.query.jexl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.regex.Pattern;

import org.junit.Test;

public class JexlPatternCacheTest {

    @Test
    public void testDotAll() {
        Pattern p = JexlPatternCache.getPattern(".*word.*");
        assertTrue(p.matcher("bla word bla").matches());
        assertTrue(p.matcher("bla\nbla word bla\n bla").matches());
        p = JexlPatternCache.getPattern("(\\s|.)*word(\\s|.)*");
        assertTrue(p.matcher("bla\nbla word bla\n bla").matches());
    }

    /**
     * Verify that {@link JexlPatternCache#getPattern(String)} will return a new {@link Pattern} that has case-insensitive and multiline matching.
     */
    @Test
    public void testRetrievingNewPattern() {
        Pattern pattern = JexlPatternCache.getPattern("bar");
        assertFalse(pattern.matcher("foobar").matches());
        assertTrue(pattern.matcher("bar").matches());
        assertTrue(pattern.matcher("BAR").matches());
        assertTrue(pattern.matcher("foo\nbar").find());
    }

    /**
     * Verify that {@link JexlPatternCache#getPattern(String)} returns a cached pattern when available.
     */
    @Test
    public void testRetrievingExistingPattern() {
        Pattern pattern = JexlPatternCache.getPattern("foobar.*");
        Pattern cached = JexlPatternCache.getPattern("foobar.*");
        assertSame(pattern, cached);
    }
}
