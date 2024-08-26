package datawave.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.Text;

public class ColumnSetUtil {

    public static String encodeColumns(Text columnFamily, Text columnQualifier) {
        StringBuilder sb = new StringBuilder();

        encode(sb, columnFamily);
        if (columnQualifier != null) {
            sb.append(':');
            encode(sb, columnQualifier);
        }

        return sb.toString();
    }

    public static Pair<Text,Text> decodeColumns(String columns) {
        if (!isValidEncoding(columns)) {
            throw new IllegalArgumentException("Invalid encoding " + columns);
        }

        String[] cols = columns.split(":");

        if (cols.length == 1) {
            return new Pair<>(decode(cols[0]), null);
        } else if (cols.length == 2) {
            return new Pair<>(decode(cols[0]), decode(cols[1]));
        } else {
            throw new IllegalArgumentException(columns);
        }
    }


    public static boolean isValidEncoding(String enc) {
        for (char c : enc.toCharArray()) {
            boolean validChar = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
                    || c == '_' || c == '-' || c == ':' || c == '%';
            if (!validChar) {
                return false;
            }
        }

        return true;
    }

    static void encode(StringBuilder sb, Text t) {
        for (int i = 0; i < t.getLength(); i++) {
            int b = (0xff & t.getBytes()[i]);

            // very inefficient code
            if ((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_'
                    || b == '-') {
                sb.append((char) b);
            } else {
                sb.append('%');
                sb.append(String.format("%02x", b));
            }
        }
    }

    static Text decode(String s) {
        Text t = new Text();

        byte[] sb = s.getBytes(UTF_8);

        // very inefficient code
        for (int i = 0; i < sb.length; i++) {
            if (sb[i] == '%') {
                int x = ++i;
                int y = ++i;
                if (y < sb.length) {
                    byte[] hex = {sb[x], sb[y]};
                    String hs = new String(hex, UTF_8);
                    int b = Integer.parseInt(hs, 16);
                    t.append(new byte[] {(byte) b}, 0, 1);
                } else {
                    throw new IllegalArgumentException("Invalid characters in encoded string (" + s + ")."
                            + " Expected two characters after '%'");
                }
            } else {
                t.append(new byte[] {sb[i]}, 0, 1);
            }
        }

        return t;
    }
}
