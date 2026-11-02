package datawave.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class TextUtilTest {
    @Test
    public void testAppend_multiByteChars() throws UnsupportedEncodingException {
        String prefix = "prefix\u6C34";
        int prefixLength = prefix.getBytes(UTF_8).length;
        Text text = new Text(prefix.getBytes(UTF_8));
        // A random multi-byte char string I found. Don't know what it means.
        String multiByteCharString = "\u007A\u6C34\uD834\uDD1E";
        TextUtil.textAppend(text, multiByteCharString);
        byte[] stringBytes = multiByteCharString.getBytes(UTF_8);
        assertEquals(1 + stringBytes.length, text.getLength() - prefixLength, "Length was wrong");
        byte[] textBytes = text.getBytes();
        assertEquals((byte) 0, textBytes[prefixLength], "First byte was wrong");
        byte[] restOfText = new byte[text.getLength() - prefixLength - 1];
        System.arraycopy(textBytes, 1 + prefixLength, restOfText, 0, restOfText.length);
        assertArrayEquals(stringBytes, restOfText, "Contents were wrong");
    }

    @Test
    public void testAppend_long() throws IOException {
        String prefix = "prefix\u6C34";
        int prefixLength = prefix.getBytes(UTF_8).length;
        Text text = new Text(prefix.getBytes(UTF_8));
        long appendedLong = 0x0123456789ABCDEFl;
        TextUtil.textAppend(text, appendedLong);
        assertEquals(1 + 8, text.getLength() - prefixLength, "Length was wrong");
        byte[] textBytes = text.getBytes();
        assertEquals((byte) 0, textBytes[prefixLength], "First byte was wrong");
        byte[] restOfText = new byte[text.getLength() - prefixLength - 1];
        System.arraycopy(textBytes, 1 + prefixLength, restOfText, 0, restOfText.length);
        assertArrayEquals(new byte[] {(byte) 0x01, (byte) 0x23, (byte) 0x45, (byte) 0x67, (byte) 0x89, (byte) 0xAB, (byte) 0xCD, (byte) 0xEF}, restOfText,
                        "Contents were wrong");
    }

    @Test
    public void testAppendNullByte() {
        Text text = new Text(new byte[] {1, 2, 3});
        TextUtil.appendNullByte(text);
        assertEquals(4, text.getLength());
        assertEquals(0, text.getBytes()[3]);
    }

    @Test
    public void testToUtf8() throws UnsupportedEncodingException {
        String multiByteCharString = "\u007A\u6C34\uD834\uDD1E";
        byte[] utf8 = TextUtil.toUtf8(multiByteCharString);
        assertArrayEquals(multiByteCharString.getBytes(UTF_8), utf8);
    }
}
