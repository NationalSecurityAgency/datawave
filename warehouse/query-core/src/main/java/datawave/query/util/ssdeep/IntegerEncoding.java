package datawave.query.util.ssdeep;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

/**
 * Class for encoding integers into a lexically sorted output of constant length. Employs the sorted Base64 alphabet captured in the HashReverse class.
 */
public class IntegerEncoding implements Serializable {
    
    // The number of distinct characters used for encoding
    final int base;
    // the target length of the encoding
    final int length;
    // the max integer value we can encode, derived from the base and length parameters.
    final int limit;
    
    /**
     * We are using the LEXICAL_B64_TABLE to encode integers to characters, our max base (the unique characters we use for encoding) is based on the size of
     * this alphabet.
     */
    private static final int MAX_BASE = HashReverse.LEXICAL_B64_TABLE.length;
    
    /**
     * Create an unsigned integer encoder that uses the specified base (up to 64) and length (which can't generate numbers larger than Integer.MAX_VALUE). This
     * uses the lexically sorted Base 64 alphabet for encoding.
     *
     * @param base
     *            base for encoding, this is the number of distinct characters that will be used to encode integers must be larger than 2, less than 64.
     * @param length
     *            the length (in bytes) of the final encoding produced by this encoding
     */
    public IntegerEncoding(int base, int length) {
        if (base < 2 || base > 64) {
            throw new IllegalArgumentException("Base must be between 2 and 64");
        }
        if (length < 1) {
            throw new IllegalArgumentException("Length must be greater than 0");
        }
        this.base = base;
        this.length = length;
        double calculatedLimit = Math.pow(base, length);
        if (calculatedLimit > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Calculated limit " + calculatedLimit + " is larger than Integer.MAX_VALUE");
        }
        this.limit = (int) calculatedLimit; // truncation is fine here.
    }
    
    /** Return the maximum value this encoder can encode */
    public int getLimit() {
        return limit;
    }
    
    public int getLength() {
        return length;
    }
    
    /** Encode the provided value, return a string result */
    public String encode(int value) {
        return new String(encodeToBytes(value, new byte[length], 0));
    }
    
    /**
     * encode the provided value, writing the result to the provided buffer starting offset
     *
     * @param value
     *            the value to encode
     * @param buffer
     *            the buffer to write to
     * @param offset
     *            the offset to write into
     * @return the buffer written to
     */
    public byte[] encodeToBytes(int value, byte[] buffer, int offset) {
        if (value < 0 || value >= limit) {
            throw new IllegalArgumentException("Can't encode " + value + " is it out of range, max: " + limit + " was: " + value);
        }
        
        if (buffer.length < offset + length) {
            throw new IndexOutOfBoundsException("Can't encode a value of length " + length + " at offset " + offset + " buffer too small: " + buffer.length);
        }
        
        int remaining = value;
        for (int place = length; place > 0; place--) {
            final int scale = ((int) Math.pow(base, place - 1));
            int pos = 0;
            if (remaining >= scale) {
                pos = remaining / scale;
                remaining = remaining % scale;
            }
            buffer[offset + (length - place)] = HashReverse.LEXICAL_B64_TABLE[pos];
        }
        return buffer;
    }
    
    // TODO: make this just like encodeToBytes?
    public static byte[] encodeBaseTenDigitBytes(int value) {
        int remaining = value;
        int digits = (int) Math.log10(remaining);
        if (digits < 0)
            digits = 0;
        digits += 1;
        // System.err.println(remaining + " " + digits);
        byte[] results = new byte[digits];
        for (int place = digits - 1; place >= 0; place--) {
            results[place] = (byte) ((remaining % 10) + 48);
            remaining = remaining / 10;
        }
        return results;
    }
    
    /**
     * Decode the first _length_ characters in the encoded value into an integer, where length is specified in the constructor.
     *
     * @param encodedValue
     *            the string to decode
     * @return the decoded result
     */
    public int decode(String encodedValue) {
        if (encodedValue.length() < length) {
            throw new IllegalArgumentException("Encoded value is not the expected length, expected: " + length + ", was: " + encodedValue);
        }
        return decode(encodedValue.getBytes(StandardCharsets.UTF_8), 0);
    }
    
    /**
     * decode the value contained within the provided byte[] starting at the specified offset
     *
     * @param encoded
     *            the encoded integer
     * @param offset
     *            the offset to read from in the input byte array
     * @return the integer encoded at this place.
     * @throws IndexOutOfBoundsException
     *             if the provided byte[] and offset doesn't provide sufficient space.
     * @throws IllegalArgumentException
     *             if the byte[] contains an item that is not in range.
     */
    public int decode(byte[] encoded, int offset) {
        if (encoded.length < offset + length) {
            throw new IndexOutOfBoundsException("Can't decode a value of length " + length + " from offset " + offset + " buffer too small: " + encoded.length);
        }
        
        int result = 0;
        for (int place = length; place > 0; place--) {
            int pos = offset + (length - place);
            Integer value = HashReverse.REVERSE_LEXICAL_B64_MAP.get(encoded[pos]);
            if (value == null) {
                throw new IllegalArgumentException("Character at offset " + pos + " is out of range '" + encoded[pos] + "'");
            }
            result += (int) Math.pow(base, place - 1) * value;
        }
        
        if (result > limit) {
            throw new IllegalArgumentException("Can't decode input is it out of range, max: " + limit + " was: " + result);
        }
        
        return result;
    }
}
