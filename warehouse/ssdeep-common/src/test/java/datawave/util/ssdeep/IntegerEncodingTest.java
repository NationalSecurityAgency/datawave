package datawave.util.ssdeep;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

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
            assertEquals(i, decoded, "encoder/decoder mismatch: input:" + i + " output:" + decoded);
            assertEquals(length, encodedString.length(), "Encoded string is no expected length " + length);
            assertEquals(otherEncodedString, encodedString, "Error with build-in string encoding");
            assertEquals(decoded, otherDecoded, "Error with built-in string methods");
            count++;
        }
        assertEquals((int) Math.pow(base, length), count, "Unexpected number of values tested");
    }

    @Test
    public void integerEncodingBaseException() {
        int base = 65;
        int length = 2;
        assertThrows(IllegalArgumentException.class, () -> new IntegerEncoding(base, length));
    }

    @Test
    public void integerEncodingLengthException() {
        int base = 64;
        int length = 500;
        assertThrows(IllegalArgumentException.class, () -> new IntegerEncoding(base, length));
    }

    @Test
    public void integerEncodingBaseUnderflow() {
        int base = -1;
        int length = 2;
        assertThrows(IllegalArgumentException.class, () -> new IntegerEncoding(base, length));
    }

    @Test
    public void integerEncodingLengthUnderflow() {
        int base = 2;
        int length = -1;
        assertThrows(IllegalArgumentException.class, () -> new IntegerEncoding(base, length));
    }

    @Test
    public void integerEncodingDecodeBufferUnderflow() {
        int base = 64;
        int length = 3;
        IntegerEncoding encoding = new IntegerEncoding(base, length);
        byte[] b = new byte[2];
        assertThrows(IndexOutOfBoundsException.class, () -> encoding.encodeToBytes(5, b, 0));
    }

    @Test
    public void integerEncodingDecodeBufferUnderflowOffset() {
        int base = 64;
        int length = 3;
        IntegerEncoding encoding = new IntegerEncoding(base, length);
        byte[] b = new byte[3];
        assertThrows(IndexOutOfBoundsException.class, () -> encoding.encodeToBytes(5, b, 1));
    }

    @Test
    public void integerEncodingDecodeBufferOffset() {
        int base = 64;
        int length = 3;
        IntegerEncoding encoding = new IntegerEncoding(base, length);
        byte[] b = new byte[4];
        encoding.encodeToBytes(5, b, 1);
        assertEquals((byte) 0, b[0]);
        assertEquals('+', (char) b[1]);
        assertEquals('+', (char) b[2]);
        assertEquals('3', (char) b[3]);
        int result = encoding.decode(b, 1);
        assertEquals(5, result);
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
        assertEquals(5, resultA);
        assertEquals(12, resultB);
    }

    @Test
    public void integerEncodingValueOverflow() {
        int base = 4;
        int length = 2;
        IntegerEncoding encoding = new IntegerEncoding(base, length);
        byte[] b = new byte[2];
        assertThrows(IllegalArgumentException.class, () -> encoding.encodeToBytes(17, b, 0));
    }

    @Test
    public void integerEncodingStringDecodeUnderflow() {
        int base = 4;
        int length = 2;
        IntegerEncoding encoding = new IntegerEncoding(base, length);
        String input = "+";
        assertThrows(IllegalArgumentException.class, () -> encoding.decode(input));
    }

    @Test
    public void integerEncodingStringDecodeInvalid() {
        int base = 4;
        int length = 2;
        IntegerEncoding encoding = new IntegerEncoding(base, length);
        String input = "%%";
        assertThrows(IllegalArgumentException.class, () -> encoding.decode(input));
    }

    @Test
    public void integerEncodingStringDecodeOverflow() {
        int base = 4;
        int length = 2;
        IntegerEncoding encoding = new IntegerEncoding(base, length);
        String input = "zz";
        assertThrows(IllegalArgumentException.class, () -> encoding.decode(input));
    }

    @Test
    public void integerEncodingByteDecodeUnderflow() {
        int base = 4;
        int length = 2;
        IntegerEncoding encoding = new IntegerEncoding(base, length);
        byte[] input = new byte[1];
        assertThrows(IndexOutOfBoundsException.class, () -> encoding.decode(input, 0));
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
            assertEquals(e, s, "Failed to encode number: " + input);
        }
    }
}
