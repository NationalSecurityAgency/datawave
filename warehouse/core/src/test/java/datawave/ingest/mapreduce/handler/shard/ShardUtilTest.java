package datawave.ingest.mapreduce.handler.shard;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.accumulo.core.data.Key;
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

    /**
     * Explicit, hard-coded verification that well-formed multi-byte Unicode -- a Latin-1 accented character, CJK characters, and a surrogate-pair emoji --
     * round-trips through {@link ShardUtil#utf8(String)} as its correct UTF-8 encoding, and is <b>not</b> replaced with {@code '?'} the way malformed input is
     * (see {@link #utf8_malformedLoneSurrogatesReplacedWithQuestionMark()}).
     */
    @Test
    public void utf8_encodesWellFormedMultibyteCharactersCorrectly() {
        // "café" - 'é' (U+00E9) is 2 UTF-8 bytes
        assertArrayEquals(new byte[] {0x63, 0x61, 0x66, (byte) 0xC3, (byte) 0xA9}, ShardUtil.utf8("café"));
        // "中文" - each CJK character is 3 UTF-8 bytes
        assertArrayEquals(new byte[] {(byte) 0xE4, (byte) 0xB8, (byte) 0xAD, (byte) 0xE6, (byte) 0x96, (byte) 0x87}, ShardUtil.utf8("中文"));
        // U+1F600 GRINNING FACE, represented in Java as the surrogate pair \uD83D\uDE00, is 4 UTF-8 bytes
        assertArrayEquals(new byte[] {(byte) 0xF0, (byte) 0x9F, (byte) 0x98, (byte) 0x80}, ShardUtil.utf8("\uD83D\uDE00"));
    }

    /**
     * Explicit, hard-coded verification that malformed lone/unpaired surrogates are each replaced with a single ASCII {@code '?'} (0x3F) byte -- the JDK's
     * default substitution for unmappable/malformed characters -- rather than being dropped, throwing, or replaced with the multi-byte Unicode replacement
     * character U+FFFD.
     */
    @Test
    public void utf8_malformedLoneSurrogatesReplacedWithQuestionMark() {
        assertArrayEquals(new byte[] {'?'}, ShardUtil.utf8("\uD800")); // lone high surrogate
        assertArrayEquals(new byte[] {'?'}, ShardUtil.utf8("\uDC00")); // lone low surrogate
        assertArrayEquals(new byte[] {'A', '?', 'B'}, ShardUtil.utf8("A\uD800B")); // lone high surrogate embedded in valid text
        assertArrayEquals(new byte[] {'A', '?', 'B'}, ShardUtil.utf8("A\uDC00B")); // lone low surrogate embedded in valid text
        assertArrayEquals(new byte[] {'?', '?'}, ShardUtil.utf8("\uD800\uD800")); // two high surrogates in a row
        assertArrayEquals(new byte[] {'?', '?'}, ShardUtil.utf8("\uDC00\uD800")); // low surrogate followed by high surrogate (reversed order)
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

    /**
     * {@link ShardUtil#createIndexKey(byte[], byte[], byte[], byte[], long, boolean)} must produce a byte-identical {@link Key} to
     * {@link ShardUtil#createIndexKey(byte[], Text, Text, byte[], long, boolean)}, since it exists purely to let callers who already have {@code colf}/
     * {@code colq} as raw bytes avoid allocating intermediate {@link Text} objects.
     */
    @Test
    public void createIndexKey_byteArrayOverload_matchesTextOverload() {
        byte[] row = ShardUtil.utf8("fieldValue");
        Text colfText = new Text("fieldName");
        Text colqText = new Text("20240101_0\u0000datatype");
        byte[] colf = ShardUtil.utf8("fieldName");
        byte[] colq = ShardUtil.utf8("20240101_0\u0000datatype");
        byte[] vis = ShardUtil.utf8("A&B");
        long ts = 1704067200123L;

        for (boolean delete : new boolean[] {false, true}) {
            Key expected = ShardUtil.createIndexKey(row, colfText, colqText, vis, ts, delete);
            Key actual = ShardUtil.createIndexKey(row, colf, colq, vis, ts, delete);
            assertEquals("createIndexKey(byte[], byte[], byte[], ...) mismatch for delete=" + delete, expected, actual);
        }
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
