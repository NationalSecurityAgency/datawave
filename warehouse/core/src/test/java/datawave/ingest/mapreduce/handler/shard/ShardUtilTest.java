package datawave.ingest.mapreduce.handler.shard;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.nio.charset.CharacterCodingException;

import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 * Pins the UTF-8 encoding fidelity of {@link ShardUtil#utf8(String)} / {@link ShardUtil#utf8(String, boolean)} against {@link Text}, which is the type they
 * replaced at call sites throughout {@code ShardedDataTypeHandler}. In particular this covers malformed input (lone surrogates), which no fixture exercised
 * prior to this test.
 */
public class ShardUtilTest {

    /**
     * A corpus mixing well-formed ASCII/Latin-1/CJK/surrogate-pair strings with malformed lone-surrogate strings, so both the "replace" and "strict" paths are
     * exercised.
     */
    private static final String[] CORPUS = new String[] {"", "FIELD_NAME", "hello world", "café", "中文", "\uD83D\uDE00", // valid surrogate pair (an emoji)
            "\uD800", // lone high surrogate
            "\uDC00", // lone low surrogate
            "A\uD800B", // lone high surrogate embedded in otherwise valid text
            "A\uDC00B", // lone low surrogate embedded in otherwise valid text
            "\uD800\uD800", // two high surrogates in a row (still malformed, not a valid pair)
            "\uDC00\uD800", // low surrogate followed by high surrogate (reversed order, malformed)
    };

    /**
     * {@link ShardUtil#utf8(String)} must be byte-identical to {@code new Text(s)} for every input, including malformed ones, since it is a drop-in replacement
     * for {@code new Text(String)} call sites.
     */
    @Test
    public void utf8SingleArg_matchesTextConstructor() {
        for (String s : CORPUS) {
            assertArrayEquals("utf8(String) mismatch for " + describe(s), new Text(s).copyBytes(), ShardUtil.utf8(s));
        }
    }

    /**
     * {@link ShardUtil#utf8(String, boolean)} with {@code replaceMalformedUTF8 = true} must be byte-identical to {@code new Text(s)}, since that is the same
     * "replace" semantics {@code Text}'s constructor uses internally.
     */
    @Test
    public void utf8TwoArgReplace_matchesTextConstructor() {
        for (String s : CORPUS) {
            assertArrayEquals("utf8(String, true) mismatch for " + describe(s), new Text(s).copyBytes(), ShardUtil.utf8(s, true));
        }
    }

    /**
     * {@link ShardUtil#utf8(String, boolean)} with {@code replaceMalformedUTF8 = false} must throw if and only if {@code Text.encode(s, false)} throws, and
     * when it doesn't throw the bytes must match exactly.
     */
    @Test
    public void utf8TwoArgStrict_throwsIffTextEncodeThrows() {
        for (String s : CORPUS) {
            byte[] expectedBytes = null;
            boolean textThrows = false;
            try {
                java.nio.ByteBuffer buffer = Text.encode(s, false);
                expectedBytes = new byte[buffer.limit()];
                System.arraycopy(buffer.array(), 0, expectedBytes, 0, expectedBytes.length);
            } catch (CharacterCodingException e) {
                textThrows = true;
            }

            boolean utf8Throws = false;
            byte[] actualBytes = null;
            try {
                actualBytes = ShardUtil.utf8(s, false);
            } catch (IllegalArgumentException e) {
                utf8Throws = true;
            }

            assertEquals("utf8(String, false) throw mismatch for " + describe(s), textThrows, utf8Throws);
            if (!textThrows) {
                assertArrayEquals("utf8(String, false) byte mismatch for " + describe(s), expectedBytes, actualBytes);
            }
        }
    }

    @Test
    public void joinWithNulls_zeroArgsReturnsEmptyArray() {
        assertArrayEquals(new byte[0], ShardUtil.joinWithNulls());
    }

    @Test
    public void joinWithNulls_singlePartReturnsItUnchanged() {
        byte[] part = {1, 2, 3};
        assertArrayEquals(part, ShardUtil.joinWithNulls(part));
    }

    @Test
    public void joinWithNulls_multiplePartsSeparatedByNulByte() {
        byte[] a = {1, 2};
        byte[] b = {3, 4};
        byte[] c = {5};
        assertArrayEquals(new byte[] {1, 2, 0, 3, 4, 0, 5}, ShardUtil.joinWithNulls(a, b, c));
    }

    @Test
    public void cannotInstantiateShardUtil() throws Exception {
        java.lang.reflect.Constructor<ShardUtil> constructor = ShardUtil.class.getDeclaredConstructor();
        if (constructor.canAccess(null)) {
            fail("ShardUtil's no-arg constructor should not be publicly accessible");
        }
    }

    private static String describe(String s) {
        StringBuilder sb = new StringBuilder("\"");
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c >= 0x20 && c < 0x7f) {
                sb.append(c);
            } else {
                sb.append(String.format("\\u%04X", (int) c));
            }
        }
        return sb.append('"').toString();
    }
}
