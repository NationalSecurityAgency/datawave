package datawave.util.ssdeep;

import org.junit.Assert;
import org.junit.Test;

public class IntegerEncodingTest {
    @Test
    public void integerEncodingFullRange() {
        int base = 32;
        int length = 2;
        IntegerEncoding encoding = new IntegerEncoding(base, length);
        int limit = encoding.getLimit();
        int count = 0;
        for (int i = 0; i < limit; i++) {
            byte[] encoded = new byte[length];
            encoding.encodeToBytes(i, encoded, 0);
            String encodedString = new String(encoded);
            String otherEncodedString = encoding.encode(i);
            int decoded = encoding.decode(encoded, 0);
            int otherDecoded = encoding.decode(otherEncodedString);
            Assert.assertEquals("encoder/decoder mismatch: input:" + i + " output:" + decoded, i, decoded);
            Assert.assertEquals("Encoded string is no expected length " + length, length, encodedString.length());
            Assert.assertEquals("Error with build-in string encoding", otherEncodedString, encodedString);
            Assert.assertEquals("Error with built-in string methods", decoded, otherDecoded);
            count++;
        }
        Assert.assertEquals("Unexpected number of values tested", (int) Math.pow(base, length), count);
    }

    @Test(expected = IllegalArgumentException.class)
    public void integerEncodingBaseException() {
        int base = 65;
        int length = 2;
        IntegerEncoding encoding = new IntegerEncoding(base, length);
    }

    @Test(expected = IllegalArgumentException.class)
    public void integerEncodingLengthException() {
        int base = 64;
        int length = 500;
        IntegerEncoding encoding = new IntegerEncoding(base, length);
    }

    @Test(expected = IllegalArgumentException.class)
    public void integerEncodingBaseUnderflow() {
        int base = -1;
        int length = 2;
        IntegerEncoding encoding = new IntegerEncoding(base, length);
    }

    @Test(expected = IllegalArgumentException.class)
    public void integerEncodingLengthUnderflow() {
        int base = 2;
        int length = -1;
        IntegerEncoding encoding = new IntegerEncoding(base, length);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void integerEncodingDecodeBufferUnderflow() {
        int base = 64;
        int length = 3;
        IntegerEncoding encoding = new IntegerEncoding(base, length);
        byte[] b = new byte[2];
        encoding.encodeToBytes(5, b, 0);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void integerEncodingDecodeBufferUnderflowOffset() {
        int base = 64;
        int length = 3;
        IntegerEncoding encoding = new IntegerEncoding(base, length);
        byte[] b = new byte[3];
        encoding.encodeToBytes(5, b, 1);
    }

    @Test
    public void integerEncodingDecodeBufferOffset() {
        int base = 64;
        int length = 3;
        IntegerEncoding encoding = new IntegerEncoding(base, length);
        byte[] b = new byte[4];
        encoding.encodeToBytes(5, b, 1);
        Assert.assertEquals((byte) 0, b[0]);
        Assert.assertEquals('+', (char) b[1]);
        Assert.assertEquals('+', (char) b[2]);
        Assert.assertEquals('3', (char) b[3]);
        int result = encoding.decode(b, 1);
        Assert.assertEquals(5, result);
    }

    @Test
    public void integerEncodingDecodeMultiOffset() {
        int base = 64;
        int length = 3;
        IntegerEncoding encoding = new IntegerEncoding(base, length);
        byte[] b = new byte[8];
        encoding.encodeToBytes(5, b, 1);
        encoding.encodeToBytes(12, b, 5);
        int resultA = encoding.decode(b, 1);
        int resultB = encoding.decode(b, 5);
        Assert.assertEquals(5, resultA);
        Assert.assertEquals(12, resultB);
    }

    @Test(expected = IllegalArgumentException.class)
    public void integerEncodingValueOverflow() {
        int base = 4;
        int length = 2;
        IntegerEncoding encoding = new IntegerEncoding(base, length);
        byte[] b = new byte[2];
        encoding.encodeToBytes(17, b, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void integerEncodingStringDecodeUnderflow() {
        int base = 4;
        int length = 2;
        IntegerEncoding encoding = new IntegerEncoding(base, length);
        String input = "+";
        encoding.decode(input);
    }

    @Test(expected = IllegalArgumentException.class)
    public void integerEncodingStringDecodeInvalid() {
        int base = 4;
        int length = 2;
        IntegerEncoding encoding = new IntegerEncoding(base, length);
        String input = "%%";
        encoding.decode(input);
    }

    @Test(expected = IllegalArgumentException.class)
    public void integerEncodingStringDecodeOverflow() {
        int base = 4;
        int length = 2;
        IntegerEncoding encoding = new IntegerEncoding(base, length);
        String input = "zz";
        encoding.decode(input);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void integerEncodingByteDecodeUnderflow() {
        int base = 4;
        int length = 2;
        IntegerEncoding encoding = new IntegerEncoding(base, length);
        byte[] input = new byte[1];
        encoding.decode(input, 0);
    }

    @Test
    public void encodeBaseTenBytes() {
        int maxDigits = 9;
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < maxDigits; i++) {
            buf.append(i);
            int input = Integer.parseInt(buf.toString());
            byte[] b = IntegerEncoding.encodeBaseTenDigitBytes(input);
            String s = new String(b);
            String e = String.valueOf(input);
            Assert.assertEquals("Failed to encode number: " + input, e, s);
        }
    }
}
