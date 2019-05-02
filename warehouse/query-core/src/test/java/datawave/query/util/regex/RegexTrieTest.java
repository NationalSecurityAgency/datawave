package datawave.query.util.regex;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.regex.Pattern;

public class RegexTrieTest {
    private static final Logger log = Logger.getLogger(RegexTrieTest.class);

    @BeforeClass
    public static void setUpClass() throws Exception {
        Logger.getRootLogger().setLevel(Level.OFF);
    }

    @Before
    public void setUp() {
        log.setLevel(Level.OFF);
        Logger.getLogger(RegexTrie.class).setLevel(Level.OFF);
    }

    public void enableLogging() {
        log.setLevel(Level.DEBUG);
        Logger.getLogger(RegexTrie.class).setLevel(Level.TRACE);
    }

    @Test
    public void testTrie() {
        String[] strings = new String[] {"", "A", "AB", "ABCDEF", "BC", "BB"};
        String[] other = new String[] {"AA", "B", "BBB", "BCB", "ABC", "ABCD", "ABCDE", "ABCDEFF", "C"};
        RegexTrie trie = new RegexTrie(Arrays.asList(strings));
        for (String string : strings) {
            Assert.assertTrue(trie.contains(string));
        }
        for (String string : other) {
            Assert.assertFalse(trie.contains(string));
        }
    }

    @Test
    public void testRegex() {
        String[] strings = new String[] {"", "A", "AB", "ABCDEF", "BC", "BB"};
        String[] other = new String[] {"AA", "B", "BBB", "BCB", "ABC", "ABCD", "ABCDE", "ABCDEFF", "C"};
        String regex = new RegexTrie(Arrays.asList(strings)).toRegex();
        Pattern pattern = Pattern.compile(regex);
        for (String string : strings) {
            Assert.assertTrue(pattern.matcher(string).matches());
        }
        for (String string : other) {
            Assert.assertFalse(pattern.matcher(string).matches());
        }
    }
}
