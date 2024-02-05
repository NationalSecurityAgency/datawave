package datawave.util.ssdeep;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChunkSizeEncodingTest {

    private static final Logger log = LoggerFactory.getLogger(ChunkSizeEncodingTest.class);

    @Test
    public void testChunkSizeIndexMapping() {
        IntegerEncoding integerEncoding = new IntegerEncoding(64, 1);
        ChunkSizeEncoding chunkSizeEncoding = new ChunkSizeEncoding();

        int index = 0;
        List<String> mismatches = new ArrayList<>();
        while (true) {
            final long chunkSize = chunkSizeEncoding.findNthChunkSize(index);
            final int calcIndex = chunkSizeEncoding.findChunkSizeIndex(chunkSize);
            final String encoded = integerEncoding.encode(calcIndex);
            final int decodedIndex = integerEncoding.decode(encoded);
            final long max = chunkSize * ChunkSizeEncoding.SPAMSUM_LENGTH;

            if (max < 0) {
                break; // overflow.
            }

            final String mark = max > Integer.MAX_VALUE ? "  " : "X ";
            final String output = mark + " index: " + index + " " + calcIndex + " " + decodedIndex + " cs: " + chunkSize + " encoded: " + encoded + " max: "
                            + max;

            if (index != calcIndex || calcIndex != decodedIndex) {
                mismatches.add(output);
            }
            log.info(output);
            index++;
        }
        assertTrue("Observed mismatches in calculated index for chunksize: " + mismatches, mismatches.isEmpty());
    }
}
