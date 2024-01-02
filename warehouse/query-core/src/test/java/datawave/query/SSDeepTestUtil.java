package datawave.query;

import datawave.query.util.Tuple2;
import datawave.query.util.ssdeep.BucketAccumuloKeyGenerator;
import datawave.query.util.ssdeep.NGramByteHashGenerator;
import datawave.query.util.ssdeep.NGramTuple;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.Locale;
import java.util.stream.Stream;

import static org.junit.Assert.fail;

public class SSDeepTestUtil {

    private static final Logger log = Logger.getLogger(SSDeepSimilarityQueryTest.class);

    public static String[] TEST_SSDEEPS = {"12288:002r/VG4GjeZHkwuPikQ7lKH5p5H9x1beZHkwulizQ1lK55pGxlXTd8zbW:002LVG4GjeZEXi37l6Br1beZEdic1lmu",
            "6144:02C3nq73v1kHGhs6y7ppFj93NRW6/ftZTgC6e8o4toHZmk6ZxoXb0ns:02C4cGCLjj9Swfj9koHEk6/Fns",
            "3072:02irbxzGAFYDMxud7fKg3dXVmbOn5u46Kjnz/G8VYrs123D6pIJLIOSP:02MKlWQ7Sg3d4bO968rm7JO",
            "3072:03jscyaGAFYDMxud7fKg3dXVmbOn5u46Kjnz/G8VYrs123D6pIJLIOSP:03NLmXR7Sg3d4bO968rm7JO",
            "3072:03jscyaZZZZZYYYYXXXWWdXVmbOn5u46KjnzWWWXXXXYYYYYYZZZZZZZ:03NLmXR7ZZZYYXW9WXYYZZZ",
            "48:1aBhsiUw69/UXX0x0qzNkVkydf2klA8a7Z35:155w69MXAlNkmkWTF5", "196608:wEEE+EEEEE0LEEEEEEEEEEREEEEhEEETEEEEEWUEEEJEEEEcEEEEEEEE3EEEEEEN:",
            "1536:0YgNvw/OmgPgiQeI+25Nh6+RS5Qa8LmbyfAiIRgizy1cBx76UKYbD+iD/RYgNvw6:", "12288:222222222222222222222222222222222:"

            "384:nv/fP9FmWVMdRFj2aTgSO+u5QT4ZE1PIVS:nDmWOdRFNTTs504cQS"
            "192:1sSAdFXYvPToUCpabe0qHEbM0NkaA80W31bixZzS7:1r4X6oU6ZHEbp1biW7"

    };

    public static String[] TEST_SSDEEP_DOCUMENTS = {
      "",
      "",
      "",
      "",
      "2017-04-02 13:57:30,AAR7808.pdf-1-att-48,NONE,67096,67096,TIFF,3b3d3551ee44d0ece3045853e1f2ea33,fb86f018806c3fa83b9b4487f26b92c56b342f8d,a75f7d749262b8a5aa0bedf55f8856f2bcf99b0f84f5bf709078d61887e1605d,2017-04-02 13:47:24,,CHECKSUM_SSDEEP=1536:a+xKllrcbBbTKWHKOaA29xbXY58enGBDMrINPzSjAaOQBP:aDldcdDm923GG08jAadV,IMAGEHEIGHT=3200,IMAGEWIDTH=2576",
      "2017-04-02 13:57:30,AAR7808.pdf-1-att-49,NONE,107984,107984,TIFF,bd3069e6bc2247cc6d62102f246aad97,b8cd63bf4e5bda4ec973c2c85b3a79e700e19657,68bbcf0701beb8cabd7cfa0f1a397d6a8b55fb197f42a844ba88c1494d21c25c,2017-04-02 13:47:24,,CHECKSUM_SSDEEP=3072:WsvD5V0/eca2XMzkt4VyqW0iwIF4eMfqxU4I7:WsvL3Z2mkyVyqhiD4e83,IMAGEHEIGHT=3200,IMAGEWIDTH=2560"
    };

    public static final int BUCKET_COUNT = BucketAccumuloKeyGenerator.DEFAULT_BUCKET_COUNT;
    public static final int BUCKET_ENCODING_BASE = BucketAccumuloKeyGenerator.DEFAULT_BUCKET_ENCODING_BASE;
    public static final int BUCKET_ENCODING_LENGTH = BucketAccumuloKeyGenerator.DEFAULT_BUCKET_ENCODING_LENGTH;

    public static void loadSSDeepIndexTextData(AccumuloClient accumuloClient) throws Exception {
        // configuration
        String ssdeepTableName = "ssdeepIndex";
        int ngramSize = 7;
        int minHashSize = 3;

        // input
        Stream<String> ssdeepLines = Stream.of(TEST_SSDEEPS);

        // processing
        final NGramByteHashGenerator nGramGenerator = new NGramByteHashGenerator(ngramSize, BUCKET_COUNT, minHashSize);
        final BucketAccumuloKeyGenerator accumuloKeyGenerator = new BucketAccumuloKeyGenerator(BUCKET_COUNT, BUCKET_ENCODING_BASE, BUCKET_ENCODING_LENGTH);

        // output
        BatchWriterConfig batchWriterConfig = new BatchWriterConfig();
        final BatchWriter bw = accumuloClient.createBatchWriter(ssdeepTableName, batchWriterConfig);

        // operations
        ssdeepLines.forEach(s -> {
            try {
                Iterator<Tuple2<NGramTuple,byte[]>> it = nGramGenerator.call(s);
                while (it.hasNext()) {
                    Tuple2<NGramTuple,byte[]> nt = it.next();
                    Tuple2<Key, Value> at = accumuloKeyGenerator.call(nt);
                    Key k = at.first();
                    Mutation m = new Mutation(k.getRow());
                    ColumnVisibility cv = new ColumnVisibility(k.getColumnVisibility());
                    m.put(k.getColumnFamily(), k.getColumnQualifier(), cv, k.getTimestamp(), at.second());
                    bw.addMutation(m);
                }
                bw.flush();
            } catch (Exception e) {
                log.error("Exception loading ssdeep hashes", e);
                fail("Exception while loading ssdeep hashes: " + e.getMessage());
            }
        });

        bw.close();
    }
}
