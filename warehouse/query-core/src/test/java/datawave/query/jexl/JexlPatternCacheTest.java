package datawave.query.jexl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.regex.Pattern;

import org.junit.Test;

public class JexlPatternCacheTest {

    public static final String ASTERISK_WORD = ".*word.*";
    public static final String BLA_WORD_BLA = "bla word bla";
    public static final String BLA_PATTERN_2 = "bla\nbla word bla\n bla";
    public static final String BLA_PATTERN_3 = "(\\s|.)*word(\\s|.)*";

    public static final String FOOBAR_PATTERN = "foobar";
    public static final String BAR_PATTERN_LOWER = "bar";
    public static final String BAR_PATTERN_UPPER = "BAR";
    public static final String FOOBAR_PATTERN_SPACED = "foo\nbar";

    @Test
    public void testDotAll() {
        Pattern p = JexlPatternCache.getPattern(ASTERISK_WORD);
        assertTrue(p.matcher(BLA_WORD_BLA).matches());
        assertTrue(p.matcher(BLA_PATTERN_2).matches());
        p = JexlPatternCache.getPattern(BLA_PATTERN_3);
        assertTrue(p.matcher(BLA_PATTERN_2).matches());
    }

    /**
     * Verify that {@link JexlPatternCache#getPattern(String)} will return a new {@link Pattern} that has case-insensitive and multiline matching.
     */
    @Test
    public void testRetrievingNewPattern() {
        Pattern pattern = JexlPatternCache.getPattern(BAR_PATTERN_LOWER);
        assertFalse(pattern.matcher(FOOBAR_PATTERN).matches());
        assertTrue(pattern.matcher(BAR_PATTERN_LOWER).matches());
        assertTrue(pattern.matcher(BAR_PATTERN_UPPER).matches());
        assertTrue(pattern.matcher(FOOBAR_PATTERN_SPACED).find());
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
