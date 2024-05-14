package datawave.accumulo.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.ByteBuffer;

import org.apache.hadoop.io.Text;

public final class TextUtil {
    public static byte[] getBytes(Text text) {
        byte[] bytes = text.getBytes();
        if (bytes.length != text.getLength()) {
            bytes = new byte[text.getLength()];
            System.arraycopy(text.getBytes(), 0, bytes, 0, bytes.length);
        }
        return bytes;
    }

    public static ByteBuffer getByteBuffer(Text text) {
        if (text == null) {
            return null;
        }
        byte[] bytes = text.getBytes();
        return ByteBuffer.wrap(bytes, 0, text.getLength());
    }

    public static Text truncate(Text text, int maxLen) {
        if (text.getLength() > maxLen) {
            Text newText = new Text();
            newText.append(text.getBytes(), 0, maxLen);
            String suffix = "... TRUNCATED";
            newText.append(suffix.getBytes(UTF_8), 0, suffix.length());
            return newText;
        }

        return text;
    }
}
