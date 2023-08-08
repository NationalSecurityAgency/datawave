package datawave.metrics.util;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import datawave.metrics.mapreduce.util.EmptyValue;

/**
 * A utility class that provides static methods that make dealing with Writables, and primarily Text, easier when dealing with parsing into primitive types.
 *
 */
public class WritableUtil {
    public static final Text EmptyText = new Text(new byte[0]);

    public static Text getLong(long l) {
        return new Text(Long.toString(l));
    }

    /**
     * Natively parses a text object into a long in base 10.
     *
     * @param t
     *            the text object
     * @return a parsed long
     * @throws NumberFormatException
     *             if there is a problem parsing
     */
    public static long parseLong(Text t) {
        return parseLong(t, 10);
    }

    /**
     * Natively parses a text object into a long with the specified radix.
     *
     * @param text
     *            the text object
     * @param radix
     *            the radix
     * @return a parsed long
     * @throws NumberFormatException
     *             if there is a problem parsing
     */
    public static long parseLong(Text text, int radix) throws NumberFormatException {
        if (text == null) {
            throw new NumberFormatException("null");
        }

        if (radix < Character.MIN_RADIX) {
            throw new NumberFormatException("radix " + radix + " less than Character.MIN_RADIX");
        }
        if (radix > Character.MAX_RADIX) {
            throw new NumberFormatException("radix " + radix + " greater than Character.MAX_RADIX");
        }

        long result = 0;
        boolean negative = false;
        int i = 0, max = text.getLength();
        long limit;
        long multmin;
        int digit;

        if (max > 0) {
            if (text.charAt(0) == '-') {
                negative = true;
                limit = Long.MIN_VALUE;
                i++;
            } else {
                limit = -Long.MAX_VALUE;
            }
            multmin = limit / radix;
            if (i < max) {
                digit = Character.digit(text.charAt(i++), radix);
                if (digit < 0) {
                    throw new NumberFormatException();
                } else {
                    result = -digit;
                }
            }
            while (i < max) {
                // Accumulating negatively avoids surprises near MAX_VALUE
                digit = Character.digit(text.charAt(i++), radix);
                if (digit < 0) {
                    throw new NumberFormatException();
                }
                if (result < multmin) {
                    throw new NumberFormatException();
                }
                result *= radix;
                if (result < limit + digit) {
                    throw new NumberFormatException();
                }
                result -= digit;
            }
        } else {
            throw new NumberFormatException();
        }
        if (negative) {
            if (i > 1) {
                return result;
            } else { /* Only got "-" */
                throw new NumberFormatException();
            }
        } else {
            return -result;
        }
    }

    /**
     * A convenience method for serializing a long into a byte array.
     *
     * @param l
     *            the specified long
     * @return a byte array
     */
    public static byte[] getLongBytes(long l) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            new LongWritable(l).write(new DataOutputStream(baos));
            return baos.toByteArray();
        } catch (IOException e) {
            return EmptyValue.getEmptyBytes();
        }
    }

    public static int find(Text what, Text in) {
        return find(what, in, 0);
    }

    /**
     * Modified from Hadoop's text to search for text within text. I'm surprised they don't support this natively, as it's safer with regards to types and
     * slightly faster since there's no conversion from UTF-16 to UTF-8.
     *
     * @param in
     *            the input text
     * @param start
     *            the start index
     * @param what
     *            the what text
     * @return byte position of the first occurence of the search string in the UTF-8 buffer or -1 if not found
     */
    public static int find(Text what, Text in, int start) {
        ByteBuffer src = ByteBuffer.wrap(in.getBytes(), 0, in.getLength());
        ByteBuffer tgt = ByteBuffer.wrap(what.getBytes(), 0, what.getLength());
        byte b = tgt.get();
        src.position(start);

        while (src.hasRemaining()) {
            if (b == src.get()) { // matching first byte
                src.mark(); // save position in loop
                tgt.mark(); // save position in target
                boolean found = true;
                int pos = src.position() - 1;
                while (tgt.hasRemaining()) {
                    if (!src.hasRemaining()) { // src expired first
                        tgt.reset();
                        src.reset();
                        found = false;
                        break;
                    }
                    if (!(tgt.get() == src.get())) {
                        tgt.reset();
                        src.reset();
                        found = false;
                        break; // no match
                    }
                }
                if (found)
                    return pos;
            }
        }
        return -1; // not found
    }

    /**
     * Finds the nth occurance of b in a given Text object.
     *
     * @param t
     *            the text
     * @param nth
     *            the index
     * @param b
     *            the byte
     * @return location of the byte
     */
    public static int findNth(Text t, int nth, byte b) {
        byte[] bytes = t.getBytes();
        int currCount = 0;
        for (int i = 0; i < t.getLength(); ++i) {
            if (bytes[i] == b) {
                ++currCount;
                if (currCount == nth) {
                    return i;
                }
            }
        }
        return -1;
    }
}
