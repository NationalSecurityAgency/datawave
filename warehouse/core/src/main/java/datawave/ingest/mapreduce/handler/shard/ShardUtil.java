package datawave.ingest.mapreduce.handler.shard;

import java.nio.charset.StandardCharsets;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;

import datawave.ingest.mapreduce.handler.dateindex.DateIndexUtil;

public class ShardUtil {
    private ShardUtil() {
        // static utility class
    }

    /**
     * The single place where an Accumulo {@link Key} is actually constructed. All {@code createKey}/{@code createIndexKey} overloads funnel through here.
     * <p>
     * The column family and qualifier are passed as a backing array plus an explicit length so that a {@link Text}'s over-allocated backing array can be used
     * directly (see {@link Text#getBytes()} vs {@link Text#getLength()}) without copying it first.
     *
     * @param row
     *            the row bytes
     * @param colf
     *            the column family backing array
     * @param colfLen
     *            the number of valid bytes in {@code colf}
     * @param colq
     *            the column qualifier backing array
     * @param colqLen
     *            the number of valid bytes in {@code colq}
     * @param vis
     *            the column visibility bytes
     * @param ts
     *            the timestamp
     * @param delete
     *            the delete flag of the key
     * @return Accumulo Key object
     */
    public static Key buildKey(byte[] row, byte[] colf, int colfLen, byte[] colq, int colqLen, byte[] vis, long ts, boolean delete) {
        Key k = new Key(row, 0, row.length, colf, 0, colfLen, colq, 0, colqLen, vis, 0, vis.length, ts);
        k.setDeleted(delete);
        return k;
    }

    /**
     * Encode a String as UTF-8 bytes. Key components are always UTF-8 encoded.
     * <p>
     * This uses the JDK encoder ({@link String#getBytes(java.nio.charset.Charset)}), which never throws: malformed input (e.g. an unpaired surrogate) is
     * silently replaced, byte for byte identically to {@code new Text(s)} and to {@link Text#encode(String, boolean)} with {@code replace = true}. Key
     * construction is therefore never able to fail a record on account of a malformed field name or value.
     *
     * @param s
     *            the string to encode
     * @return the UTF-8 bytes
     */
    public static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Concatenates byte array segments with a single {@code NUL} byte between each pair, e.g. {@code joinWithNulls(a, b, c)} produces
     * {@code a + '\0' + b + '\0' + c}. This computes the exact final length up front and copies each segment once, avoiding the repeated encode-then-grow-
     * then-copy behavior of building the same layout via {@link Text#append(byte[], int, int)}.
     *
     * @param parts
     *            the byte array segments to join
     * @return the joined bytes, or an empty array if no parts are given
     */
    public static byte[] joinWithNulls(byte[]... parts) {
        if (parts.length == 0) {
            return new byte[0];
        }
        int len = parts.length - 1;
        for (byte[] part : parts) {
            len += part.length;
        }
        byte[] result = new byte[len];
        int pos = 0;
        for (int i = 0; i < parts.length; i++) {
            System.arraycopy(parts[i], 0, result, pos, parts[i].length);
            pos += parts[i].length;
            if (i < parts.length - 1) {
                result[pos++] = 0;
            }
        }
        return result;
    }

    /**
     * Builds a shard-prefixed byte array of the form {@code shardId[0, prefixLen) + '\0' + suffix}. Used to construct the row (day/year index tables) or column
     * qualifier (bitset index table) for the alternate global index implementations, mirroring the string concatenation
     * {@code shard.substring(0, prefixLen) + '\u0000' + suffix} without decoding the shard id to a String or re-encoding the result back to bytes.
     *
     * @param shardId
     *            the full shard id bytes
     * @param prefixLen
     *            the number of leading bytes of the shard id to retain (8 for day granularity, 4 for year granularity)
     * @param suffix
     *            the UTF-8 encoded suffix bytes
     * @return the shard-prefixed bytes
     */
    public static byte[] shardPrefixedBytes(byte[] shardId, int prefixLen, byte[] suffix) {
        byte[] result = new byte[prefixLen + 1 + suffix.length];
        System.arraycopy(shardId, 0, result, 0, prefixLen);
        result[prefixLen] = 0;
        System.arraycopy(suffix, 0, result, prefixLen + 1, suffix.length);
        return result;
    }

    /**
     * Create Key from input parameters
     *
     * @param row
     *            the row
     * @param colf
     *            the column family
     * @param colq
     *            the column qualifier
     * @param vis
     *            the column visibility
     * @param ts
     *            the timestamp
     * @param delete
     *            the delete flag of the key
     * @return Accumulo Key object
     */
    public static Key createKey(byte[] row, Text colf, Text colq, byte[] vis, long ts, boolean delete) {
        return buildKey(row, colf.getBytes(), colf.getLength(), colq.getBytes(), colq.getLength(), vis, ts, delete);
    }

    /**
     * Create Key from input parameters. Overload of {@link #createKey(byte[], Text, Text, byte[], long, boolean)} for callers that already have the column
     * family and qualifier as raw bytes, avoiding a pair of intermediate {@link Text} objects.
     *
     * @param row
     *            the row
     * @param colf
     *            the column family
     * @param colq
     *            the column qualifier
     * @param vis
     *            the column visibility
     * @param ts
     *            the timestamp
     * @param delete
     *            the delete flag of the key
     * @return Accumulo Key object
     */
    public static Key createKey(byte[] row, byte[] colf, byte[] colq, byte[] vis, long ts, boolean delete) {
        return buildKey(row, colf, colf.length, colq, colq.length, vis, ts, delete);
    }

    /**
     * Create Key from input parameters. Overload of {@link #createKey(byte[], Text, Text, byte[], long, boolean)} for callers whose column qualifier is a
     * String, avoiding an intermediate {@link Text}.
     *
     * @param row
     *            the row
     * @param colf
     *            the column family
     * @param colq
     *            the column qualifier
     * @param vis
     *            the column visibility
     * @param ts
     *            the timestamp
     * @param delete
     *            the delete flag of the key
     * @return Accumulo Key object
     */
    public static Key createKey(byte[] row, Text colf, String colq, byte[] vis, long ts, boolean delete) {
        byte[] colqBytes = utf8(colq);
        return buildKey(row, colf.getBytes(), colf.getLength(), colqBytes, colqBytes.length, vis, ts, delete);
    }

    /**
     * Create Key from input parameters. Overload of {@link #createKey(byte[], Text, Text, byte[], long, boolean)} for callers who have already assembled the
     * column qualifier bytes directly (e.g. via {@link ShardUtil#joinWithNulls(byte[]...)}), avoiding an intermediate {@link Text}.
     *
     * @param row
     *            the row
     * @param colf
     *            the column family
     * @param colq
     *            the column qualifier
     * @param vis
     *            the column visibility
     * @param ts
     *            the timestamp
     * @param delete
     *            the delete flag of the key
     * @return Accumulo Key object
     */
    public static Key createKey(byte[] row, Text colf, byte[] colq, byte[] vis, long ts, boolean delete) {
        return buildKey(row, colf.getBytes(), colf.getLength(), colq, colq.length, vis, ts, delete);
    }

    /**
     * Create Key from input parameters. Overload of {@link #createKey(byte[], Text, Text, byte[], long, boolean)} for callers whose column family and qualifier
     * are Strings, avoiding a pair of intermediate {@link Text} objects.
     *
     * @param row
     *            the row
     * @param colf
     *            the column family
     * @param colq
     *            the column qualifier
     * @param vis
     *            the column visibility
     * @param ts
     *            the timestamp
     * @param delete
     *            the delete flag of the key
     * @return Accumulo Key object
     */
    public static Key createKey(byte[] row, String colf, String colq, byte[] vis, long ts, boolean delete) {
        return createKey(row, utf8(colf), utf8(colq), vis, ts, delete);
    }

    /**
     * Create Key from input parameters
     *
     * For global index keys, the granularity of the timestamp is to the millisecond, where the semantics of the index record is to the day. This makes
     * MapReduce unable to reduce all index keys together unless they occurred at the same millisecond. If we truncate the timestamp to the day, we should
     * reduce the number of keys output from a job.
     *
     * @param row
     *            the row
     * @param colf
     *            the column family
     * @param colq
     *            the column qualifier
     * @param vis
     *            the column visibility
     * @param ts
     *            the timestamp
     * @param delete
     *            the delete flag of the key
     * @return Accumulo Key object
     */
    public static Key createIndexKey(byte[] row, Text colf, Text colq, byte[] vis, long ts, boolean delete) {
        return buildKey(row, colf.getBytes(), colf.getLength(), colq.getBytes(), colq.getLength(), vis, DateIndexUtil.getIndexTimestamp(ts), delete);
    }

    /**
     * Create Key from input parameters. Overload of {@link #createIndexKey(byte[], Text, Text, byte[], long, boolean)} for callers that already have the column
     * family and qualifier as raw bytes, avoiding a pair of intermediate {@link Text} objects.
     *
     * @param row
     *            the row
     * @param colf
     *            the column family
     * @param colq
     *            the column qualifier
     * @param vis
     *            the column visibility
     * @param ts
     *            the timestamp
     * @param delete
     *            the delete flag of the key
     * @return Accumulo Key object
     */
    public static Key createIndexKey(byte[] row, byte[] colf, byte[] colq, byte[] vis, long ts, boolean delete) {
        return buildKey(row, colf, colf.length, colq, colq.length, vis, DateIndexUtil.getIndexTimestamp(ts), delete);
    }

    /**
     * Create Key from input parameters. Overload of {@link #createIndexKey(byte[], Text, Text, byte[], long, boolean)} for the common global index case where
     * the row is a field value and the column family is a field name, avoiding an intermediate byte array and {@link Text}.
     *
     * @param row
     *            the row
     * @param colf
     *            the column family
     * @param colq
     *            the column qualifier
     * @param vis
     *            the column visibility
     * @param ts
     *            the timestamp
     * @param delete
     *            the delete flag of the key
     * @return Accumulo Key object
     */
    public static Key createIndexKey(String row, String colf, Text colq, byte[] vis, long ts, boolean delete) {
        byte[] colfBytes = utf8(colf);
        return buildKey(utf8(row), colfBytes, colfBytes.length, colq.getBytes(), colq.getLength(), vis, DateIndexUtil.getIndexTimestamp(ts), delete);
    }

    /**
     * Create Key from input parameters. Overload of {@link #createIndexKey(byte[], Text, Text, byte[], long, boolean)} for the term dictionary case where the
     * column family is a shared constant and the column qualifier is a field name, avoiding an intermediate byte array and {@link Text}.
     *
     * @param row
     *            the row
     * @param colf
     *            the column family
     * @param colq
     *            the column qualifier
     * @param vis
     *            the column visibility
     * @param ts
     *            the timestamp
     * @param delete
     *            the delete flag of the key
     * @return Accumulo Key object
     */
    public static Key createIndexKey(String row, Text colf, String colq, byte[] vis, long ts, boolean delete) {
        byte[] colqBytes = utf8(colq);
        return buildKey(utf8(row), colf.getBytes(), colf.getLength(), colqBytes, colqBytes.length, vis, DateIndexUtil.getIndexTimestamp(ts), delete);
    }

    /**
     * Create Key from input parameters. Overload of {@link #createIndexKey(byte[], Text, Text, byte[], long, boolean)} for the global index case where the
     * column qualifier has already been assembled directly as bytes (e.g. via {@link ShardUtil#joinWithNulls(byte[]...)}), avoiding an intermediate
     * {@link Text}.
     *
     * @param row
     *            the row
     * @param colf
     *            the column family
     * @param colq
     *            the column qualifier
     * @param vis
     *            the column visibility
     * @param ts
     *            the timestamp
     * @param delete
     *            the delete flag of the key
     * @return Accumulo Key object
     */
    public static Key createIndexKey(String row, String colf, byte[] colq, byte[] vis, long ts, boolean delete) {
        byte[] colfBytes = utf8(colf);
        return buildKey(utf8(row), colfBytes, colfBytes.length, colq, colq.length, vis, DateIndexUtil.getIndexTimestamp(ts), delete);
    }
}
