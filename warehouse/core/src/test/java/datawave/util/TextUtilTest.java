package datawave.util;

import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class TextUtilTest {
    @Test
    public void testAppend_multiByteChars() throws UnsupportedEncodingException {
        String prefix = "prefix\u6C34";
        int prefixLength = prefix.getBytes("UTF-8").length;
        Text text = new Text(prefix.getBytes("UTF-8"));
        // A random multi-byte char string I found. Don't know what it means.
        String multiByteCharString = "\u007A\u6C34\uD834\uDD1E";
        TextUtil.textAppend(text, multiByteCharString);
        byte[] stringBytes = multiByteCharString.getBytes("UTF-8");
        Assertions.assertEquals(1 + stringBytes.length, text.getLength() - prefixLength, "Length was wrong");
        byte[] textBytes = text.getBytes();
        Assertions.assertEquals((byte) 0, textBytes[prefixLength], "First byte was wrong");
        byte[] restOfText = new byte[text.getLength() - prefixLength - 1];
        System.arraycopy(textBytes, 1 + prefixLength, restOfText, 0, restOfText.length);
        Assertions.assertArrayEquals(stringBytes, restOfText, "Contents were wrong");
    }
    
    @Test
    public void testAppend_long() throws IOException {
        String prefix = "prefix\u6C34";
        int prefixLength = prefix.getBytes("UTF-8").length;
        Text text = new Text(prefix.getBytes("UTF-8"));
        long appendedLong = 0x0123456789ABCDEFl;
        TextUtil.textAppend(text, appendedLong);
        Assertions.assertEquals(1 + 8, text.getLength() - prefixLength, "Length was wrong");
        byte[] textBytes = text.getBytes();
        Assertions.assertEquals((byte) 0, textBytes[prefixLength], "First byte was wrong");
        byte[] restOfText = new byte[text.getLength() - prefixLength - 1];
        System.arraycopy(textBytes, 1 + prefixLength, restOfText, 0, restOfText.length);
        Assertions.assertArrayEquals(new byte[] {(byte) 0x01, (byte) 0x23, (byte) 0x45, (byte) 0x67, (byte) 0x89, (byte) 0xAB, (byte) 0xCD, (byte) 0xEF},
                        restOfText, "Contents were wrong");
    }
    
    @Test
    public void testAppendNullByte() {
        Text text = new Text(new byte[] {1, 2, 3});
        TextUtil.appendNullByte(text);
        Assertions.assertEquals(4, text.getLength());
        Assertions.assertEquals(0, text.getBytes()[3]);
    }
    
    @Test
    public void testToUtf8() throws UnsupportedEncodingException {
        String multiByteCharString = "\u007A\u6C34\uD834\uDD1E";
        byte[] utf8 = TextUtil.toUtf8(multiByteCharString);
        Assertions.assertArrayEquals(multiByteCharString.getBytes("UTF-8"), utf8);
    }
}
