package datawave.iterators.filter;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;

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
    public void tokensMayNotContainDelimiters() {
        new TokenTtlTrie.Builder().setDelimiters(",".getBytes()).addToken("foo,".getBytes(), 1).build();
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void tokensMustBeUnique() {
        new TokenTtlTrie.Builder().setDelimiters(",".getBytes()).addToken("foo".getBytes(), 1).addToken("foo".getBytes(), 2).build();
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
        assertThat(trie.scan("foo".getBytes()), is(equalTo(1L)));
        assertThat(trie.scan("baz".getBytes()), is(equalTo(5L)));
        // overridden values
        assertThat(trie.scan("bar".getBytes()), is(equalTo(5L)));
        assertThat(trie.scan("zip".getBytes()), is(equalTo(9L)));
    }
    
    @Test
    public void testParseWithWhiteSpaces() {
        String initial = "\"baking powder\" : 1d\n\t\t\t\t" + "\"dried beans\" : 2d\n\t\t\t\t" + "\"baking soda\" : 3d\n\t\t\t\t"
                        + "\"coffee grounds\" : 4d\n\t\t\t\t" + "\"coffee whole bean\" : 5d\n\t\t\t\t" + "\"coffee instant\" : 6d";
        
        TokenTtlTrie trie = new TokenTtlTrie.Builder().setDelimiters("/".getBytes()).parse(initial).build();
        assertThat(trie.scan("baking powder".getBytes()), is(notNullValue()));
        assertThat(trie.scan("dried beans".getBytes()), is(notNullValue()));
        assertThat(trie.scan("baking soda".getBytes()), is(notNullValue()));
        // not one of the ones we configured the tree with
        assertThat(trie.scan("zip foo".getBytes()), is(nullValue()));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void parsedTokensMayNotContainDelimiters() {
        new TokenTtlTrie.Builder().setDelimiters(",".getBytes()).parse("\"foo,\":10s").build();
    }
    
    @Test
    public void testNewFormatOnly() {
        String initial = "foobar 1234abcd=42d\nbarbaz ABCD1234=9001d";
        
        TokenTtlTrie trie = new TokenTtlTrie.Builder().setDelimiters("/".getBytes()).parse(initial).build();
        assertThat(trie.scan("1234abcd".getBytes()), is(notNullValue()));
        assertThat(trie.scan("ABCD1234".getBytes()), is(notNullValue()));
    }
    
    @Test
    public void testNewFormatWithOldFormat() {
        String initial = "foobar 3001futurama=42d\nbarbaz 42planetExpress=9001d\n\"moocow\" : 1234d";
        
        TokenTtlTrie trie = new TokenTtlTrie.Builder().setDelimiters("/".getBytes()).parse(initial).build();
        assertThat(trie.scan("3001futurama".getBytes()), is(notNullValue()));
        assertThat(trie.scan("42planetExpress".getBytes()), is(notNullValue()));
        assertThat(trie.scan("momCorp".getBytes()), is(nullValue()));
        assertThat(trie.scan("moocow".getBytes()), is(notNullValue()));
    }
    
}
