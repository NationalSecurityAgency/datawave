package datawave.ingest.mapreduce.handler.shard;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 * Pins the UTF-8 encoding fidelity of {@link ShardUtil#utf8(String)} against {@link Text}, which is the type it replaced at call sites throughout
 * {@code ShardedDataTypeHandler}. In particular this covers malformed input (lone surrogates), which no fixture exercised prior to this test.
 */
public class ShardUtilTest {

    /**
     * A corpus mixing well-formed ASCII/Latin-1/CJK/surrogate-pair strings with malformed lone-surrogate strings, so that both well-formed encoding and
     * malformed-character replacement are exercised.
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
    public void utf8_matchesTextConstructor() {
        for (String s : CORPUS) {
            assertArrayEquals("utf8(String) mismatch for " + describe(s), new Text(s).copyBytes(), ShardUtil.utf8(s));
        }
    }

    /**
     * {@link ShardUtil#utf8(String)} must never throw, even for malformed input: it must match the "replace" semantics of {@code Text.encode(s, true)} rather
     * than the strict semantics of {@code Text.encode(s, false)}, so that key construction can never fail a record.
     */
    @Test
    public void utf8_replacesMalformedInputInsteadOfThrowing() throws Exception {
        for (String s : CORPUS) {
            java.nio.ByteBuffer buffer = Text.encode(s, true);
            byte[] expectedBytes = new byte[buffer.limit()];
            System.arraycopy(buffer.array(), 0, expectedBytes, 0, expectedBytes.length);
            assertArrayEquals("utf8(String) mismatch for " + describe(s), expectedBytes, ShardUtil.utf8(s));
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
