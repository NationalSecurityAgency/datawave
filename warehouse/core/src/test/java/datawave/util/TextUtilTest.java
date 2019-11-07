package datawave.util;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

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
        Assert.assertEquals("Length was wrong", 1 + stringBytes.length, text.getLength() - prefixLength);
        byte[] textBytes = text.getBytes();
        Assert.assertEquals("First byte was wrong", (byte) 0, textBytes[prefixLength]);
        byte[] restOfText = new byte[text.getLength() - prefixLength - 1];
        System.arraycopy(textBytes, 1 + prefixLength, restOfText, 0, restOfText.length);
        Assert.assertArrayEquals("Contents were wrong", stringBytes, restOfText);
    }
    
    @Test
    public void testAppend_long() throws IOException {
        String prefix = "prefix\u6C34";
        int prefixLength = prefix.getBytes("UTF-8").length;
        Text text = new Text(prefix.getBytes("UTF-8"));
        long appendedLong = 0x0123456789ABCDEFl;
        TextUtil.textAppend(text, appendedLong);
        Assert.assertEquals("Length was wrong", 1 + 8, text.getLength() - prefixLength);
        byte[] textBytes = text.getBytes();
        Assert.assertEquals("First byte was wrong", (byte) 0, textBytes[prefixLength]);
        byte[] restOfText = new byte[text.getLength() - prefixLength - 1];
        System.arraycopy(textBytes, 1 + prefixLength, restOfText, 0, restOfText.length);
        Assert.assertArrayEquals("Contents were wrong", new byte[] {(byte) 0x01, (byte) 0x23, (byte) 0x45, (byte) 0x67, (byte) 0x89, (byte) 0xAB, (byte) 0xCD,
                (byte) 0xEF}, restOfText);
    }
    
    @Test
    public void testAppendNullByte() {
        Text text = new Text(new byte[] {1, 2, 3});
        TextUtil.appendNullByte(text);
        Assert.assertEquals(4, text.getLength());
        Assert.assertEquals(0, text.getBytes()[3]);
    }
    
    @Test
    public void testToUtf8() throws UnsupportedEncodingException {
        String multiByteCharString = "\u007A\u6C34\uD834\uDD1E";
        byte[] utf8 = TextUtil.toUtf8(multiByteCharString);
        Assert.assertArrayEquals(multiByteCharString.getBytes("UTF-8"), utf8);
    }
}
