package datawave.query.util.ssdeep;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.collect.ImmutableMap;

/**
 * Utility class that provides methods to map a ssdeep hash or ssdeep hash ngram to a position in a linear index. This is used when partitioning data so that
 * items that would be adjacent lexically are written to the same partition. It is an alternative to hash partitioning.
 */
public class HashReverse {
    /** Lookup table for the Base-64 encoding used in the SSDEEP Hashes, but sorted. 64 distinct values per place */
    public static final byte[] LEXICAL_B64_TABLE = getBytes("+/0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz");

    /** A maps of characters in the hash encoding to their position in the lookup table */
    private static final Map<Byte,Integer> reverseMap = new TreeMap<>();
    static {
        for (int i = 0; i < LEXICAL_B64_TABLE.length; i++) {
            reverseMap.put(LEXICAL_B64_TABLE[i], i);
        }
    }
    public static final ImmutableMap<Byte,Integer> REVERSE_LEXICAL_B64_MAP = ImmutableMap.copyOf(reverseMap);

    /** The smallest possible index value */
    public static final int MIN_VALUE = 0;

    /** Largest possible hash */
    public static final byte[] MAX_HASH = "zzzzz".getBytes(StandardCharsets.UTF_8);

    /** Anything larger than 64^5 will overflow an integer, so our prefix can't be larger */
    public static final int MAX_PREFIX_SIZE = 5;

    /** The largest possible index value */
    public static final int MAX_VALUE = getPrefixIndex(MAX_HASH, MAX_PREFIX_SIZE);

    public static byte[] getBytes(String str) {
        byte[] r = new byte[str.length()];
        for (int i = 0; i < r.length; i++) {
            r[i] = (byte) str.charAt(i);
        }
        return r;
    }

    public static int getPrefixIndex(final String hash, final int length) {
        return getPrefixIndex(hash.getBytes(StandardCharsets.UTF_8), length);
    }

    /**
     * Return the 'index' of the specified hash in the space of all possible hashes for the specified length. Thinking of the hash string as a 'number' with
     * each position as a 'place', indexes are calculated by collecting the sum of 'value * 64^place', where value is derived based on the position of the
     * character in the array of acceptable base64 characters.
     *
     * @param hash
     * @param length
     * @return
     */
    public static int getPrefixIndex(final byte[] hash, final int length) {
        int result = 0;

        final int limit = Math.min(hash.length, length);
        if (limit > MAX_PREFIX_SIZE) {
            throw new IndexOutOfBoundsException("Generating indexes for prefixes > 5 in length will lead to an integer overflow");
        }

        for (int i = 0; i < limit; i++) {
            int place = (limit - i) - 1;
            Integer value = REVERSE_LEXICAL_B64_MAP.get(hash[i]);
            if (value == null) {
                throw new SSDeepParseException("Character at offset " + i + " is out of range", hash);
            }
            result += Math.pow(64, place) * value;
        }

        return result;
    }

    /**
     * Return the max possible value for the provided prefix length
     *
     * @param length
     * @return
     */
    public static int getPrefixMax(final int length) {
        return getPrefixIndex(MAX_HASH, length);
    }

    /** Utility to generate splits for the ssdeep table based on a prefix of 2 - 64^64 (4096) splits in size */
    public static void main(String[] args) throws IOException {
        int len = LEXICAL_B64_TABLE.length;
        byte[] output = new byte[2];
        try (FileWriter fw = new FileWriter("ssdeep-splits.txt"); PrintWriter writer = new PrintWriter(fw)) {
            for (int i = 0; i < len; i++) {
                output[0] = LEXICAL_B64_TABLE[i];
                for (int j = 0; j < len; j++) {
                    output[1] = LEXICAL_B64_TABLE[j];
                    writer.println(new String(output));
                }
            }
        }
    }
}
