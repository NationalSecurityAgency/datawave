package datawave.util;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * This code was repurposed from {@link org.apache.accumulo.core.util.ByteBufferUtil}. It was not part of the public API, so we've created a DataWave
 * equivalent. This should be used in place of {@link org.apache.accumulo.core.util.ByteBufferUtil} moving forward.
 */
public class ByteUtil {

    /**
     * Converts a ByteBuffer to a byte array. The ByteBuffer is not modified.
     *
     * @param buffer
     *            the ByteBuffer to convert
     * @return a byte array containing the same data as the ByteBuffer.
     */
    public static byte[] toBytes(ByteBuffer buffer) {
        if (buffer == null) {
            return null;
        }
        if (buffer.hasArray()) {
            // did not use buffer.get() because it changes the position
            return Arrays.copyOfRange(buffer.array(), buffer.position() + buffer.arrayOffset(), buffer.limit() + buffer.arrayOffset());
        } else {
            byte[] data = new byte[buffer.remaining()];
            // duplicate inorder to avoid changing position
            buffer.duplicate().get(data);
            return data;
        }
    }
}
