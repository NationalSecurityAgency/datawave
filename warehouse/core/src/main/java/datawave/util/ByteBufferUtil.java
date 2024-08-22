package datawave.util;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class ByteBufferUtil {
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
