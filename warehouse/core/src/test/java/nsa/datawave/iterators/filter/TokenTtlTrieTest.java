package nsa.datawave.iterators.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import nsa.datawave.iterators.filter.ageoff.AgeOffPeriod;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

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
        
        Assert.assertEquals(null, trie.scan("foobar,barbaz;bazfoo".getBytes()));
        Assert.assertEquals((Long) 2L, trie.scan("foobar,foo;barfoo".getBytes()));
        Assert.assertEquals((Long) 2L, trie.scan("bar;foo".getBytes()));
        Assert.assertEquals((Long) 3L, trie.scan("bar,baz,foobar".getBytes()));
        Assert.assertEquals((Long) 4L, trie.scan("buffer,baz".getBytes()));
        Assert.assertNull(trie.scan("b;ba,banana,bread,apple,pie".getBytes()));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void tokensMayNotContainDelimiters() {
        TokenTtlTrie trie = new TokenTtlTrie.Builder().setDelimiters(",".getBytes()).addToken("foo,".getBytes(), 1).build();
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void tokensMustBeUnique() {
        TokenTtlTrie trie = new TokenTtlTrie.Builder().setDelimiters(",".getBytes()).addToken("foo".getBytes(), 1).addToken("foo".getBytes(), 2).build();
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
        
        Assert.assertEquals(null, trie.scan("foobar,barbaz;bazfoo".getBytes()));
        Assert.assertEquals((Long) 2L, trie.scan("foobar,foo;barfoo".getBytes()));
        Assert.assertEquals((Long) 2L, trie.scan("bar;foo".getBytes()));
        Assert.assertEquals((long) AgeOffPeriod.getTtlUnitsFactor("d") * 3L, (long) trie.scan("bar,baz,foobar".getBytes()));
        Assert.assertEquals((Long) (AgeOffPeriod.getTtlUnitsFactor("m") * 4L), trie.scan("buffer,baz".getBytes()));
        Assert.assertNull(trie.scan("b;ba,banana,bread,apple,pie".getBytes()));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void parsedTokensMayNotContainDelimiters() {
        TokenTtlTrie trie = new TokenTtlTrie.Builder().setDelimiters(",".getBytes()).parse("\"foo,\":10s").build();
    }
}
