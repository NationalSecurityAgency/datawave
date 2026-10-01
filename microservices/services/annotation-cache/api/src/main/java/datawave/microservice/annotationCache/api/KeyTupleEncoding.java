package datawave.microservice.annotationCache.api;

import java.io.InvalidObjectException;
import java.nio.ByteBuffer;

/**
 * Encodes three strings as one canonical payload so equal keys serialize identically regardless of shared String references. For example, {@code (s, s, s)} and
 * {@code (new String(s), new String(s), new String(s))} have equal values but can produce different Java streams without this encoding. Hazelcast uses
 * serialized key data for map and lock identity, so different streams can make equal keys behave as different keys.
 */
final class KeyTupleEncoding {
    private static final int VERSION = 1;
    private static final int FIELD_COUNT = 3;

    private KeyTupleEncoding() {}

    /** Version byte, then three big-endian char counts and their exact UTF-16 code units, in tuple order. */
    static byte[] encode(String first, String second, String third) {
        String[] fields = {first, second, third};
        long size = 1L + FIELD_COUNT * Integer.BYTES;
        for (String field : fields) {
            size += (long) field.length() * Character.BYTES;
        }
        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Key tuple exceeds the maximum byte-array size");
        }
        ByteBuffer buffer = ByteBuffer.allocate((int) size);
        buffer.put((byte) VERSION);
        for (String field : fields) {
            buffer.putInt(field.length());
            for (int index = 0; index < field.length(); index++) {
                buffer.putChar(field.charAt(index));
            }
        }
        return buffer.array();
    }

    static String[] decode(byte[] payload) throws InvalidObjectException {
        if (payload == null || payload.length == 0) {
            throw new InvalidObjectException("Missing key tuple payload");
        }
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        if (Byte.toUnsignedInt(buffer.get()) != VERSION) {
            throw new InvalidObjectException("Unsupported key tuple version");
        }
        String[] fields = new String[FIELD_COUNT];
        for (int field = 0; field < FIELD_COUNT; field++) {
            if (buffer.remaining() < Integer.BYTES) {
                throw new InvalidObjectException("Truncated key tuple length");
            }
            int length = buffer.getInt();
            // Check against actual bytes before allocating; division avoids overflow for malicious lengths.
            if (length < 0 || length > buffer.remaining() / Character.BYTES) {
                throw new InvalidObjectException("Invalid key tuple length");
            }
            char[] characters = new char[length];
            for (int index = 0; index < length; index++) {
                characters[index] = buffer.getChar();
            }
            fields[field] = new String(characters);
        }
        if (buffer.hasRemaining()) {
            throw new InvalidObjectException("Trailing key tuple data");
        }
        return fields;
    }
}
