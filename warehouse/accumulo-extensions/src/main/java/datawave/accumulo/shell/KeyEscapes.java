package datawave.accumulo.shell;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.io.Text;

/**
 * Translates between the printable escape syntax used on the shell command line and the raw bytes of a DataWave key, whose components are delimited by a null
 * byte that cannot be typed at a terminal.
 * <p>
 * The recognized escapes are {@code \0}, {@code \n}, {@code \r}, {@code \t}, {@code \xHH} for an arbitrary byte, and {@code \\}. Any other character following
 * a backslash is rejected rather than passed through, since a typo would otherwise silently scan the wrong range.
 */
public class KeyEscapes {

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private KeyEscapes() {}

    /**
     * Decodes the escape sequences in the given value.
     *
     * @param value
     *            the value as typed on the command line
     * @return the decoded bytes
     * @throws IllegalArgumentException
     *             if the value ends in a dangling backslash or contains an unrecognized escape
     */
    public static Text decode(String value) {
        byte[] raw = value.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream decoded = new ByteArrayOutputStream(raw.length);
        for (int i = 0; i < raw.length; i++) {
            if (raw[i] != '\\') {
                decoded.write(raw[i]);
                continue;
            }
            if (++i == raw.length) {
                throw new IllegalArgumentException("Dangling backslash at the end of: " + value);
            }
            switch (raw[i]) {
                case '0':
                    decoded.write(0);
                    break;
                case 'n':
                    decoded.write('\n');
                    break;
                case 'r':
                    decoded.write('\r');
                    break;
                case 't':
                    decoded.write('\t');
                    break;
                case '\\':
                    decoded.write('\\');
                    break;
                case 'x':
                    if (i + 2 >= raw.length) {
                        throw new IllegalArgumentException("Truncated \\xHH escape at index " + (i - 1) + " of: " + value);
                    }
                    decoded.write((hexDigit(raw[++i], value) << 4) | hexDigit(raw[++i], value));
                    break;
                default:
                    throw new IllegalArgumentException("Unrecognized escape \\" + (char) raw[i] + " at index " + (i - 1) + " of: " + value);
            }
        }
        return new Text(decoded.toByteArray());
    }

    /**
     * Decodes the given value only when escape processing is enabled, so that a caller can honor an option turning it off.
     *
     * @param value
     *            the value as typed on the command line
     * @param escaped
     *            whether escape sequences should be decoded
     * @return the resulting bytes
     */
    public static Text decode(String value, boolean escaped) {
        return escaped ? decode(value) : new Text(value.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Renders raw key bytes using the same escape syntax {@link #decode(String)} accepts, so that output can be pasted back into a scan.
     *
     * @param value
     *            the raw bytes
     * @return the printable form
     */
    public static String encode(Text value) {
        StringBuilder sb = new StringBuilder(value.getLength());
        for (int i = 0; i < value.getLength(); i++) {
            int b = value.getBytes()[i] & 0xff;
            switch (b) {
                case 0:
                    sb.append("\\0");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                default:
                    if (b < 0x20 || b > 0x7e) {
                        sb.append("\\x").append(HEX[b >>> 4]).append(HEX[b & 0xf]);
                    } else {
                        sb.append((char) b);
                    }
            }
        }
        return sb.toString();
    }

    private static int hexDigit(byte digit, String value) {
        int d = Character.digit(digit, 16);
        if (d < 0) {
            throw new IllegalArgumentException("Invalid hex digit '" + (char) digit + "' in: " + value);
        }
        return d;
    }
}
