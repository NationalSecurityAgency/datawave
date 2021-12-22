package datawave.iterators.filter;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import datawave.iterators.filter.TokenTtlTrie.Builder.MERGE_MODE;
import datawave.iterators.filter.ageoff.AgeOffPeriod;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;

public class TokenTtlTrieTest {
    public static Logger log = Logger.getLogger(TokenTtlTrieTest.class);
    public static final long MILLIS_IN_DAY = 24 * 60 * 60 * 1000L;
    public static int BENCHMARK_SIZE = 10000;
    
    @BeforeClass
    public static void setLogging() {
        log.setLevel(Level.INFO);
    }
    
    @Test
    public void testTrie() {
        TokenTtlTrie trie = new TokenTtlTrie.Builder().setDelimiters(",;".getBytes()).addToken("foo".getBytes(), 2).addToken("bar".getBytes(), 3)
                        .addToken("baz".getBytes(), 4).build();
        
        assertNull(trie.scan("foobar,barbaz;bazfoo".getBytes()));
        assertEquals((Long) 2L, trie.scan("foobar,foo;barfoo".getBytes()));
        assertEquals((Long) 2L, trie.scan("bar;foo".getBytes()));
        assertEquals((Long) 3L, trie.scan("bar,baz,foobar".getBytes()));
        assertEquals((Long) 4L, trie.scan("buffer,baz".getBytes()));
        assertNull(trie.scan("b;ba,banana,bread,apple,pie".getBytes()));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void addedTokensMayNotContainDelimiters() {
        new TokenTtlTrie.Builder().setDelimiters(",".getBytes()).addToken("foo,".getBytes(), 1).build();
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void tokensMustBeUnique() {
        new TokenTtlTrie.Builder().setDelimiters(",".getBytes()).addToken("foo".getBytes(), 1).addToken("foo".getBytes(), 2).build();
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void parsedTokensMayNotContainDelimiters() {
        new TokenTtlTrie.Builder().setDelimiters(",".getBytes()).parse("\"foo,\":10s").build();
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testTokensMustBeUniqueAcrossFormats() {
        String input = "foo bar : 2s\n\"bar\"=42s";
        new TokenTtlTrie.Builder().setDelimiters("/".getBytes()).parse(input).build();
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testColonEqualsFails() {
        String input = "foo bar =: 2s";
        new TokenTtlTrie.Builder().setDelimiters("/".getBytes()).parse(input).build();
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testEqualsColonFails() {
        String input = "foo bar:=2s";
        new TokenTtlTrie.Builder().setDelimiters("/".getBytes()).parse(input).build();
    }
    
    @Test
    public void tokensOverridesPermittedWhenMergeEnabled() {
        byte[] token = "foo".getBytes();
        
        //@formatter:off
        TokenTtlTrie trie = new TokenTtlTrie.Builder(MERGE_MODE.ON)
                .addToken(token, 1)
                .addToken(token, 2)
                .build();
        //@formatter:on
        
        assertEquals((Long) 2L, trie.scan(token));
    }
    
    @Test
    public void fuzzTtlTrie() {
        List<Long> trieEntries = new ArrayList<>();
        Random r = new Random();
        TokenTtlTrie.Builder builder = new TokenTtlTrie.Builder().setDelimiters(",;".getBytes());
        
        long nextKey = 0;
        for (int i = 0; i < BENCHMARK_SIZE; i++) {
            // Advance by at least 2 so that we can easily generate entries which will be guaranteed not to be in the trie.
            nextKey += 2 + (Math.abs(r.nextInt()) % 100);
            trieEntries.add(nextKey);
            builder.addToken(String.format("%06x00", nextKey).getBytes(), nextKey);
        }
        
        long startTime = System.nanoTime();
        TokenTtlTrie trie = builder.build();
        long duration = System.nanoTime() - startTime;
        
        log.info(String.format("Built trie in %d ns/entry", duration / BENCHMARK_SIZE));
        
        for (long entry : trieEntries) {
            Assert.assertEquals((Long) entry, trie.scan(String.format("%06x00", entry).getBytes()));
            Assert.assertNull(trie.scan(String.format("%06x00", entry - 1).getBytes()));
        }
    }
    
    @Test
    public void testParser() {
        TokenTtlTrie trie = new TokenTtlTrie.Builder().setDelimiters(",;".getBytes())
                        .parse("" + "\"foo\":2ms\n" + "\"b\\u0061r\":3d,\n" + "\"b\\x61z\":4m,\n\n").build();
        
        Assert.assertNull(trie.scan("foobar,barbaz;bazfoo".getBytes()));
        Assert.assertEquals((Long) 2L, trie.scan("foobar,foo;barfoo".getBytes()));
        Assert.assertEquals((Long) 2L, trie.scan("bar;foo".getBytes()));
        Assert.assertEquals((long) AgeOffPeriod.getTtlUnitsFactor("d") * 3L, (long) trie.scan("bar,baz,foobar".getBytes()));
        Assert.assertEquals((Long) (AgeOffPeriod.getTtlUnitsFactor("m") * 4L), trie.scan("buffer,baz".getBytes()));
        Assert.assertNull(trie.scan("b;ba,banana,bread,apple,pie".getBytes()));
    }
    
    @Test
    public void testParseAndMerge() {
        String initial = "\"foo\" : 1ms\n" + "\"bar\" : 3ms\n" + "\"baz\" : 5ms\n" + "\"zip\" : 7ms\n";
        
        String override = "\"bar\" : 5ms\n" + "\"zip\" : 9ms\n";
        
        TokenTtlTrie trie = new TokenTtlTrie.Builder(MERGE_MODE.ON).setDelimiters("/".getBytes()).parse(initial + override).build();
        // original values
        assertEquals((Long) 1L, trie.scan("foo".getBytes()));
        assertEquals((Long) 5L, trie.scan("baz".getBytes()));
        // overridden values
        assertEquals((Long) 5L, trie.scan("bar".getBytes()));
        assertEquals((Long) 9L, trie.scan("zip".getBytes()));
    }
    
    @Test
    public void testParseWithWhiteSpaces() {
        String initial = "\"baking powder\" : 1d\n\t\t\t\t" + "\"dried beans\" : 2d\n\t\t\t\t" + "\"baking soda\" : 3d\n\t\t\t\t"
                        + "\"coffee grounds\" : 4d\n\t\t\t\t" + "\"coffee whole bean\" : 5d\n\t\t\t\t" + "\"coffee instant\" : 6d";
        
        TokenTtlTrie trie = new TokenTtlTrie.Builder().setDelimiters("/".getBytes()).parse(initial).build();
        assertEquals((Long) (1 * MILLIS_IN_DAY), trie.scan("baking powder".getBytes()));
        assertEquals((Long) (2 * MILLIS_IN_DAY), trie.scan("dried beans".getBytes()));
        assertEquals((Long) (3 * MILLIS_IN_DAY), trie.scan("baking soda".getBytes()));
        // not one of the ones we configured the tree with
        assertNull(trie.scan("zip foo".getBytes()));
    }
    
    @Test
    public void testNewFormatOnly() {
        String initial = "foobar 1234abcd=42d\nbarbaz ABCD1234=9001d";
        
        TokenTtlTrie trie = new TokenTtlTrie.Builder().setDelimiters("/".getBytes()).parse(initial).build();
        assertEquals((Long) (42 * MILLIS_IN_DAY), trie.scan("1234abcd".getBytes()));
        assertEquals((Long) (9001L * MILLIS_IN_DAY), trie.scan("ABCD1234".getBytes()));
    }
    
    @Test
    public void testNewFormatOnlyAndSpaces() {
        String initial = "    foobar 1234abcd=42d   \n\tbarbaz ABCD1234=9001d\n\n\t";
        
        TokenTtlTrie trie = new TokenTtlTrie.Builder().setDelimiters("/".getBytes()).parse(initial).build();
        assertEquals((Long) (42 * MILLIS_IN_DAY), trie.scan("1234abcd".getBytes()));
        assertEquals((Long) (9001 * MILLIS_IN_DAY), trie.scan("ABCD1234".getBytes()));
    }
    
    @Test
    public void testNewFormatWithOldFormat() {
        String initial = "foobar 3001futurama=42d\nbarbaz 42planetExpress=9001d\n\"moocow\" : 1234d";
        
        TokenTtlTrie trie = new TokenTtlTrie.Builder().setDelimiters("/".getBytes()).parse(initial).build();
        verifyMixedFormats(trie);
    }
    
    @Test
    public void testNewFormatWithOldFormatAndSpaces() {
        //@formatter:off
        String initial = "    foobar 3001futurama=42d   \n" +
                "   007 buggy=9001d   \n" +
                "   Barbaz 42planetExpress=9001d  \n" +
                "  \"moocow\" : 1234d   ";
        //@formatter:on
        
        TokenTtlTrie trie = new TokenTtlTrie.Builder().setDelimiters("/".getBytes()).parse(initial).build();
        verifyMixedFormats(trie);
    }
    
    @Test
    public void testEmptyStrLiteral() {
        String input = "\"\" : 42s";
        String expectedKey = "";
        Long expectedValue = 42 * 1000L;
        verifyParsing(input, expectedKey, expectedValue);
    }
    
    private void verifyParsing(String input, String expectedKey, Long expectedValue) {
        TokenTtlTrie trie = new TokenTtlTrie.Builder().setDelimiters("/".getBytes()).parse(input).build();
        assertEquals(expectedValue, trie.scan(expectedKey.getBytes()));
    }
    
    @Test
    public void testOldStrLiteralWithEqualSign() {
        String input = "\"blah\" = 42s";
        String expectedKey = "blah";
        Long expectedValue = 42 * 1000L;
        verifyParsing(input, expectedKey, expectedValue);
    }
    
    @Test
    public void testNewStrLiteralWithColon() {
        String input = "foo bar : 42s";
        String expectedKey = "bar";
        Long expectedValue = 42 * 1000L;
        verifyParsing(input, expectedKey, expectedValue);
    }
    
    @Test
    public void testOverrideMixedFormats() {
        String input = "foo bar : 2s\n\"bar\"=42s";
        String expectedKey = "bar";
        TokenTtlTrie trie = new TokenTtlTrie.Builder(MERGE_MODE.ON).setDelimiters("/".getBytes()).parse(input).build();
        assertEquals(42 * 1000L, (long) trie.scan(expectedKey.getBytes()));
    }
    
    @Test
    public void testOverrideMixedFormatsBackwards() {
        // Same as testOverrideMixedFormats, just reversed the lines
        String input = "\"bar\"=42s\nfoo bar : 2s";
        String expectedKey = "bar";
        Long expectedValue = 2 * 1000L;
        
        TokenTtlTrie trie = new TokenTtlTrie.Builder(MERGE_MODE.ON).setDelimiters("/".getBytes()).parse(input).build();
        
        assertEquals(expectedValue, trie.scan(expectedKey.getBytes()));
    }
    
    @Test
    public void testNoDurationOldFormat() {
        String input = "\"blah\"";
        String expectedKey = "blah";
        Long expectedValue = -1L;
        verifyParsing(input, expectedKey, expectedValue);
    }
    
    @Test
    public void testNoDurationNewFormat() {
        String input = "foo bar";
        String expectedKey = "bar";
        Long expectedValue = -1L;
        verifyParsing(input, expectedKey, expectedValue);
    }
    
    private void verifyMixedFormats(TokenTtlTrie trie) {
        assertEquals(42 * MILLIS_IN_DAY, (long) trie.scan("3001futurama".getBytes()));
        assertEquals(9001 * MILLIS_IN_DAY, (long) trie.scan("42planetExpress".getBytes()));
        assertNull(trie.scan("momCorp".getBytes()));
        assertEquals(1234 * MILLIS_IN_DAY, (long) trie.scan("moocow".getBytes()));
    }
}
