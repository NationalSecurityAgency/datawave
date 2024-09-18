package datawave.util;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;

import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.hadoop.io.Text;

public class TextUtil {
    /**
     * Appends a null byte followed by the UTF-8 bytes of the given string to the given {@link Text}
     *
     * @param text
     *            the Text to which to append
     * @param string
     *            the String to append
     */
    public static void textAppend(Text text, String string) {
        appendNullByte(text);
        textAppendNoNull(text, string);
    }

    public static void textAppend(Text text, String string, boolean replaceBadChar) {
        appendNullByte(text);
        textAppendNoNull(text, string, replaceBadChar);
    }

    public static void textAppend(Text t, long s) {
        t.append(nullByte, 0, 1);
        t.append(LongCombiner.FIXED_LEN_ENCODER.encode(s), 0, 8);
    }

    private static final byte[] nullByte = {0};

    /**
     * Appends a null byte to the given text
     *
     * @param text
     *            the text to which to append the null byte
     */
    public static void appendNullByte(Text text) {
        text.append(nullByte, 0, nullByte.length);
    }

    /**
     * Appends the UTF-8 bytes of the given string to the given {@link Text}
     *
     * @param t
     *            the Text to which to append
     * @param s
     *            the String to append
     */
    public static void textAppendNoNull(Text t, String s) {
        textAppendNoNull(t, s, false);
    }

    /**
     * Appends the UTF-8 bytes of the given string to the given {@link Text}
     *
     * @param t
     *            the Text to which append
     * @param s
     *            the String to append
     * @param replaceBadChar
     *            flag to replace bad characters
     */
    public static void textAppendNoNull(Text t, String s, boolean replaceBadChar) {
        try {
            ByteBuffer buffer = Text.encode(s, replaceBadChar);
            t.append(buffer.array(), 0, buffer.limit());
        } catch (CharacterCodingException cce) {
            throw new IllegalArgumentException(cce);
        }
    }

    /**
     * Converts the given string its UTF-8 bytes. This uses Hadoop's method for converting string to UTF-8 and is much faster than calling
     * {@link String#getBytes(String)}.
     *
     * @param string
     *            the string to convert
     * @return the UTF-8 representation of the string
     */
    public static byte[] toUtf8(String string) {
        ByteBuffer buffer;
        try {
            buffer = Text.encode(string, false);
        } catch (CharacterCodingException cce) {
            throw new IllegalArgumentException(cce);
        }
        byte[] bytes = new byte[buffer.limit()];
        System.arraycopy(buffer.array(), 0, bytes, 0, bytes.length);
        return bytes;
    }

    /**
     * Converts a UTF-8 encoded byte array back into a String.
     *
     * @param bytes
     *            UTF8 string of bytes
     * @return string
     */
    public static String fromUtf8(byte[] bytes) {
        try {
            return Text.decode(bytes);
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static byte[] getBytes(Text text) {
        byte[] bytes = text.getBytes();
        if (bytes.length != text.getLength()) {
            bytes = new byte[text.getLength()];
            System.arraycopy(text.getBytes(), 0, bytes, 0, bytes.length);
        }
        return bytes;
    }
}
