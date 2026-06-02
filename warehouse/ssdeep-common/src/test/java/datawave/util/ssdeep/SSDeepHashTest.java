package datawave.util.ssdeep;

import static org.junit.Assert.assertEquals;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSDeepHashTest {

    private static final Logger log = LoggerFactory.getLogger(SSDeepHashTest.class);

    /*
     * Valid Chunk Sizes: 3, 6, 12, 24, 48, 96, 192, 384, 768, 1536, 3072, 6144, 12288, 24576, 49152, 98304, 196608, 393216, 786432, 1572864, 3145728, 6291456,
     * 12582912
     */

    public static String[] GOOD_HASHES = {"12288:002r/VG4GjeZHkwuPikQ7lKH5p5H9x1beZHkwulizQ1lK55pGxlXTd8zbW:002LVG4GjeZEXi37l6Br1beZEdic1lmu",
            "6144:02C3nq73v1kHGhs6y7ppFj93NRW6/ftZTgC6e8o4toHZmk6ZxoXb0ns:02C4cGCLjj9Swfj9koHEk6/Fns",
            "3072:02irbxzGAFYDMxud7fKg3dXVmbOn5u46Kjnz/G8VYrs123D6pIJLIOSP:02MKlWQ7Sg3d4bO968rm7JO",
            "48:1aBhsiUw69/UXX0x0qzNkVkydf2klA8a7Z35:155w69MXAlNkmkWTF5", "196608:wEEE+EEEEE0LEEEEEEEEEEREEEEhEEETEEEEEWUEEEJEEEEcEEEEEEEE3EEEEEEN:",
            "1536:0YgNvw/OmgPgiQeI+25Nh6+RS5Qa8LmbyfAiIRgizy1cBx76UKYbD+iD/RYgNvw6:", "12288:222222222222222222222222222222222:"};

    public static String[] BAD_HASHES = {"12289:002r/VG4GjeZHkwuPikQ7lKH5p5H9x1beZHkwulizQ1lK55pGxlXTd8zbW:002LVG4GjeZEXi37l6Br1beZEdic1lmu",
            "9:02C3nq73v1kHGhs6y7ppFj93NRW6/ftZTgC6e8o4toHZmk6ZxoXb0ns:02C4cGCLjj9Swfj9koHEk6/Fns",
            "3072:02irbxzGAFYDMxud7fKg3dXVmbOn5u46Kjnz/G8VYrs123D6pIJLIOSP02irbxzGAFYDMxud7fKg3dXVmbOn5u46Kjnz:02MKlWQ7Sg3d4bO968rm7JO",
            "48:1aBhsiUw69/UXX0x0qzNkVkydf2klA8a7Z35:02irbxzGAFYDMxud7fKg3dXVmbOn5u46Kjnz/G8VYrs123D6pIJLIOSP02irbxzGAFYDMxud7fKg3dXVmbOn5u46Kjnz",
            "abc:def:ghi", "32:32:32:32", "32:32"};

    public static final int ILLEGAL_ARGUMENT = 0;
    public static final int PARSE = 1;

    public static int[] BAD_HASH_TYPE = {ILLEGAL_ARGUMENT, ILLEGAL_ARGUMENT, ILLEGAL_ARGUMENT, ILLEGAL_ARGUMENT, PARSE, PARSE, ILLEGAL_ARGUMENT};

    public static int[] EXPECTED_CHUNK_SIZES = {12288, 6144, 3072, 48, 196608, 1536, 12288};

    public static String[] EXPECTED_CHUNKS = {"002r/VG4GjeZHkwuPikQ7lKH5p5H9x1beZHkwulizQ1lK55pGxlXTd8zbW",
            "02C3nq73v1kHGhs6y7ppFj93NRW6/ftZTgC6e8o4toHZmk6ZxoXb0ns", "02irbxzGAFYDMxud7fKg3dXVmbOn5u46Kjnz/G8VYrs123D6pIJLIOSP",
            "1aBhsiUw69/UXX0x0qzNkVkydf2klA8a7Z35", "wEEE+EEEEE0LEEEEEEEEEEREEEEhEEETEEEEEWUEEEJEEEEcEEEEEEEE3EEEEEEN",
            "0YgNvw/OmgPgiQeI+25Nh6+RS5Qa8LmbyfAiIRgizy1cBx76UKYbD+iD/RYgNvw6", "222222222222222222222222222222222"};

    public static String[] EXPECTED_DOUBLE_CHUNKS = {"002LVG4GjeZEXi37l6Br1beZEdic1lmu", "02C4cGCLjj9Swfj9koHEk6/Fns", "02MKlWQ7Sg3d4bO968rm7JO",
            "155w69MXAlNkmkWTF5", "", "", "", ""};

    @Test
    public void testParse() {
        for (int i = 0; i < GOOD_HASHES.length; i++) {
            SSDeepHash ssdh = SSDeepHash.parse(GOOD_HASHES[i]);
            assertEquals("chunksize is not expected", EXPECTED_CHUNK_SIZES[i], ssdh.getChunkSize());
            assertEquals("chunk is not expected", EXPECTED_CHUNKS[i], ssdh.getChunk());
            assertEquals("double chunk is not expected", EXPECTED_DOUBLE_CHUNKS[i], ssdh.getDoubleChunk());

        }
    }

    @Test
    public void testParseFailures() {
        for (int i = 0; i < BAD_HASHES.length; i++) {
            boolean parseFailure = false;

            try {
                SSDeepHash.parse(BAD_HASHES[i]);
            } catch (IllegalArgumentException ia) {
                parseFailure = true;
                if (BAD_HASH_TYPE[i] != ILLEGAL_ARGUMENT) {
                    log.info("IllegalArgumentException received", ia);
                    Assert.fail("Received unexpected IllegalArgumentException");
                }
            } catch (SSDeepParseException pe) {
                parseFailure = true;
                if (BAD_HASH_TYPE[i] != PARSE) {
                    log.info("SSDeepParseException received", pe);
                    Assert.fail("Received unexpected SSDeepParseException");
                }
            } finally {
                if (!parseFailure) {
                    Assert.fail("Did not receive expected parse failure");
                }
            }
        }
    }

    @Test
    public void testNormalize() {
        String[][] input = {{"x", "x"}, {"xx", "xx"}, {"xxx", "xxx"}, {"xxxx", "xxx"}, // whole
                {"xxxxxx", "xxx"}, // whole
                {"yyyxxxy", "yyyxxxy"}, {"yyyxxxxy", "yyyxxxy"}, // middle
                {"yyyxxyyyy", "yyyxxyyy"}, // end
                {"yyyyxxxy", "yyyxxxy"}, // beginning
                {"yyyxxxy", "yyyxxxy"}, {"xyyyyyzyyyyy", "xyyyzyyy"}, // mixe
                {"yyyyzzzzyyyy", "yyyzzzyyy"}, // mixed
        };

        boolean hasNoFailure = true;

        for (String[] item : input) {
            String norm = SSDeepHash.normalizeSSDeepChunk(item[0], SSDeepHash.DEFAULT_MAX_REPEATED_CHARACTERS);
            if (norm == null && item[1].equals("")) {
                log.info("OK '" + item[0] + "' -> null");
            } else if (item[1].equals(norm)) {
                log.info("OK '" + item[0] + "' -> '" + norm + "'");
            } else {
                log.info("FAIL: expected: '" + item[0] + "' -> '" + item[1] + "', was: '" + norm + "'");
                hasNoFailure = false;
            }
        }

        Assert.assertTrue(hasNoFailure);
    }

    @Test
    public void testSSDeepNormalize() {
        String[][] input = {
                {"98304:OlRQ486yJed1++++++++++++++++++++++++Eo+++++++++++++++++++++++/+o:OlK4vAeb+++++++++++++++++++++++7",
                        "98304:OlRQ486yJed1+++Eo+++/+o:OlK4vAeb+++7"},
                {"1536:KGJkCvodTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT+AcMw:TJkCnAcMCgOlclhkMb+I+++++++RpTt",
                        "1536:KGJkCvodTTT+AcMw:TJkCnAcMCgOlclhkMb+I+++RpTt"},
                {"12288:KP206WUPHARVhtly4dHXl0jU+jnjedJOYUbOD+aZi+8cfyyHbfiR:025Wesf02x+jad++Q+cM+R",
                        "12288:KP206WUPHARVhtly4dHXl0jU+jnjedJOYUbOD+aZi+8cfyyHbfiR:025Wesf02x+jad++Q+cM+R"}};

        boolean hasNoFailure = true;

        for (String[] item : input) {
            SSDeepHash s = SSDeepHash.parse(item[0]);
            SSDeepHash n = SSDeepHash.normalize(s);
            if (n.toString().equals(item[1])) {
                log.info("OK '" + item[0] + "' -> '" + item[1] + "'");
            } else {
                log.info("FAIL expected: '" + item[0] + "' -> '" + item[1] + "', was: '" + n + "'");
                hasNoFailure = false;
            }
        }

        Assert.assertTrue(hasNoFailure);
    }
}
