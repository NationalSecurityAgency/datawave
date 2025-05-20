package datawave.util.ssdeep;

import static java.util.stream.Collectors.toSet;

import static datawave.util.ssdeep.SSDeepHashTest.GOOD_HASHES;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Multimap;

public class NGramGeneratorTest {

    private static final String[][] GOOD_NGRAMS = {
            {"12288:/VG4Gje", "12288:Q1lK55p", "12288:eZHkwuP", "12288:ZHkwuPi", "12288:5H9x1be", "12288:02r/VG4", "12288:izQ1lK5", "12288:K55pGxl",
                    "12288:r/VG4Gj", "12288:lK55pGx", "12288:1beZHkw", "12288:Q7lKH5p", "12288:uPikQ7l", "12288:H9x1beZ", "12288:ZHkwuli", "12288:002r/VG",
                    "12288:x1beZHk", "12288:PikQ7lK", "12288:G4GjeZH", "12288:4GjeZHk", "12288:kQ7lKH5", "12288:wuPikQ7", "12288:H5p5H9x", "12288:5p5H9x1",
                    "12288:Hkwuliz", "12288:ikQ7lKH", "12288:5pGxlXT", "12288:ulizQ1l", "12288:lXTd8zb", "12288:beZHkwu", "12288:jeZHkwu", "12288:wulizQ1",
                    "12288:pGxlXTd", "12288:55pGxlX", "12288:GjeZHkw", "12288:1lK55pG", "12288:lKH5p5H", "12288:HkwuPik", "12288:kwulizQ", "12288:p5H9x1b",
                    "12288:eZHkwul", "12288:lizQ1lK", "12288:xlXTd8z", "12288:zQ1lK55", "12288:VG4GjeZ", "12288:7lKH5p5", "12288:kwuPikQ", "12288:2r/VG4G",
                    "12288:GxlXTd8", "12288:KH5p5H9", "12288:9x1beZH", "12288:XTd8zbW"},
            {"6144:6e8o4to", "6144:6y7ppFj", "6144:2C3nq73", "6144:gC6e8o4", "6144:3NRW6/f", "6144:NRW6/ft", "6144:93NRW6/", "6144:TgC6e8o", "6144:3v1kHGh",
                    "6144:ZxoXb0n", "6144:6ZxoXb0", "6144:/ftZTgC", "6144:mk6ZxoX", "6144:73v1kHG", "6144:ppFj93N", "6144:nq73v1k", "6144:s6y7ppF",
                    "6144:Fj93NRW", "6144:tZTgC6e", "6144:toHZmk6", "6144:C6e8o4t", "6144:4toHZmk", "6144:pFj93NR", "6144:HZmk6Zx", "6144:k6ZxoXb",
                    "6144:HGhs6y7", "6144:ftZTgC6", "6144:j93NRW6", "6144:6/ftZTg", "6144:q73v1kH", "6144:1kHGhs6", "6144:y7ppFj9", "6144:Zmk6Zxo",
                    "6144:hs6y7pp", "6144:7ppFj93", "6144:C3nq73v", "6144:3nq73v1", "6144:v1kHGhs", "6144:ZTgC6e8", "6144:8o4toHZ", "6144:o4toHZm",
                    "6144:oHZmk6Z", "6144:W6/ftZT", "6144:02C3nq7", "6144:RW6/ftZ", "6144:Ghs6y7p", "6144:kHGhs6y", "6144:e8o4toH", "6144:xoXb0ns"}};

    @Test
    public void testGenerateNGrams() {
        int ngramSize = NGramGenerator.DEFAULT_NGRAM_SIZE;
        NGramGenerator eng = new NGramGenerator(ngramSize);
        SSDeepHash[] hashes = Arrays.stream(GOOD_HASHES).limit(2).map(SSDeepHash::parse).toArray(SSDeepHash[]::new);
        for (int i = 0; i < hashes.length; i++) {
            Set<NGramTuple> expected = Arrays.stream(GOOD_NGRAMS[i]).map(NGramTuple::parse).collect(toSet());
            List<NGramTuple> unexpected = new ArrayList<>();

            final SSDeepHash ssdh = hashes[i];
            Set<NGramTuple> output = new HashSet<>();
            eng.generateNgrams(ssdh.getChunkSize(), ssdh.getChunk(), output);
            for (NGramTuple ct : output) {
                assertEquals(ssdh.getChunkSize(), ct.getChunkSize());
                assertEquals(ngramSize, ct.getChunk().length());
                if (!expected.remove(ct)) {
                    unexpected.add(ct);
                }
            }

            final StringBuilder failMessage = new StringBuilder();
            if (!expected.isEmpty()) {
                failMessage.append("Did not observe expected elements for hash: ").append(ssdh).append(" expected: ").append(expected).append(". ");
            }
            if (!unexpected.isEmpty()) {
                failMessage.append("Observed unexpected elements for hash: ").append(ssdh).append(" unexpected: ").append(unexpected).append(". ");
            }
            Assert.assertTrue(failMessage.toString(), expected.isEmpty() && unexpected.isEmpty());
        }
    }

    @Test
    public void testProcessMultipleHashes() {
        NGramGenerator eng = new NGramGenerator(7);
        Set<SSDeepHash> list = Arrays.stream(GOOD_HASHES).map(SSDeepHash::parse).collect(toSet());
        Multimap<NGramTuple,SSDeepHash> result = eng.preprocessQueries(list);
    }

}
