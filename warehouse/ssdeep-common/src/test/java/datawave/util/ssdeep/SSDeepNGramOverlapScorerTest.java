package datawave.util.ssdeep;

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

public class SSDeepNGramOverlapScorerTest {
    public static final String[][] testData = {
            {"3072:02irbxzGAFYDMxud7fKg3dXVmbOn5u46Kjnz/G8VYrs123D6pIJLIOSP:02MKlWQ7Sg3d4bO968rm7JO",
                    "3072:02irbxzGAFYDMxud7fKg3dXVmbOn5u46Kjnz/G8VYrs123D6pIJLIOSP:02MKlWQ7Sg3d4bO968rm7JO"},
            // repeated character case
            {"3072:02irbxzGAFYDMxud7fKg3dXVmbOn5u46Kjnz/G8VYEEEEEEEEEEEEEEE:02MKlWQ7Sg3d4bEEEEEEEE",
                    "3072:02irbxzGAFYDMxud7fKg3dXVmbOn5u46Kjnz/G8VYrs123D6EEEEEEEE:02MKlWQ7Sg3d4bEEEE"},
            // chunk difference is less than 2 scales, so we can compare these.
            {"3072:02irbxzGAFYDMxud7fKg3dXVmbOn5u46Kjnz/G8VYrs123D6pIJLIOSP:02MKlWQ7Sg3d4bO968rm7JO",
                    "6144:02MKlWQ7Sg3d4bO968rm7JORW6/ftZTgC6e8o4toHZmk6ZxoXb0ns:02C4cGCLjj9Swfj9koHEk6/Fns"},
            // inverse of the last example tests symmetry
            {"6144:02MKlWQ7Sg3d4bO968rm7JORW6/ftZTgC6e8o4toHZmk6ZxoXb0ns:02C4cGCLjj9Swfj9koHEk6/Fns",
                    "3072:02irbxzGAFYDMxud7fKg3dXVmbOn5u46Kjnz/G8VYrs123D6pIJLIOSP:02MKlWQ7Sg3d4bO968rm7JO"},
            // chunk mismatch case
            {"3072:02irbxzGAFYDMxud7fKg3dXVmbOn5u46Kjnz/G8VYrs123D6pIJLIOSP:02MKlWQ7Sg3d4bO968rm7JO",
                    "48:1aBhsiUw69/UXX0x0qzNkVkydf2klA8a7Z35:155w69MXAlNkmkWTF5"},
            // short hash case
            {"3:aabbcc:abc", "3:aabbccdd:abcd"}, {"6:aabbcc:abc", "6:aabbccdd:abcd"}

    };

    public static final int[] expectedScores = {65, 47, 16, 16, 0, 0, 0};

    @Test
    public void testCompare() {
        SSDeepHashScorer<Set<NGramTuple>> scorer = new SSDeepNGramOverlapScorer(7, 3, 6);
        for (int i = 0; i < testData.length; i++) {
            SSDeepHash queryHash = SSDeepHash.parse(testData[i][0]);
            SSDeepHash targetHash = SSDeepHash.parse(testData[i][1]);
            Set<NGramTuple> overlappingTuples = scorer.apply(queryHash, targetHash);
            int score = overlappingTuples.size();
            Assert.assertEquals("Expected score of " + expectedScores[i] + " for query: " + queryHash + ", target: " + targetHash, expectedScores[i], score);
        }
    }
}
