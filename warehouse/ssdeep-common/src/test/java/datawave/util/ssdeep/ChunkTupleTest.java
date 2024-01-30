package datawave.util.ssdeep;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ChunkTupleTest {
    public static final String[] GOOD_TUPLES = {"12288:/VG4Gje", "12288:Q1lK55p", "6144:6e8o4to", "6144:6y7ppFj", "3072:DMxud7f", "3072:D6pIJLI",
            "3072:AFYDMxu", "48:0qzNkVk", "48:69/UXX0", "48:iUw69/U"};

    public static final int[] EXPECTED_CHUNK_SIZES = {12288, 12288, 6144, 6144, 3072, 3072, 3072, 48, 48, 48};

    @Test
    public void testParse() {
        for (int i = 0; i < GOOD_TUPLES.length; i++) {
            NGramTuple ct = NGramTuple.parse(GOOD_TUPLES[i]);
            assertEquals(EXPECTED_CHUNK_SIZES[i], ct.getChunkSize());
        }
    }

    // TODO: test equality, parse failures, etc.
}
