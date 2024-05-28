package datawave.ingest.mapreduce.job.reduce;

import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;

import org.apache.hadoop.io.Text;

/**
 * This class was mostly copied from Accumulo's ColumnSet.java. The usage was blocking removal of calls to classes in Accumulo's private API
 */
public class ColumnTransform {

    public ColumnTransform() {}

    public static String encodeColumns(Text columnFamily, Text columnQualifier) {
        StringBuilder sb = new StringBuilder();
        encode(sb, columnFamily);
        if (columnQualifier != null) {
            sb.append(':');
            encode(sb, columnQualifier);
        }

        return sb.toString();
    }

    static void encode(StringBuilder sb, Text t) {
        for (int i = 0; i < t.getLength(); ++i) {
            int b = 255 & t.getBytes()[i];
            if ((b < 97 || b > 122) && (b < 65 || b > 90) && (b < 48 || b > 57) && b != 95 && b != 45) {
                sb.append('%');
                sb.append(String.format("%02x", b));
            } else {
                sb.append((char) b);
            }
        }

    }

    public static boolean isValidEncoding(String enc) {
        char[] var1 = enc.toCharArray();
        int var2 = var1.length;

        for (int var3 = 0; var3 < var2; ++var3) {
            char c = var1[var3];
            boolean validChar = c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' || c == ':' || c == '%';
            if (!validChar) {
                return false;
            }
        }

        return true;
    }

    public static AbstractMap.SimpleEntry<Text,Text> decodeColumns(String columns) {
        if (!isValidEncoding(columns)) {
            throw new IllegalArgumentException("Invalid encoding " + columns);
        } else {
            String[] cols = columns.split(":");
            if (cols.length == 1) {
                return new AbstractMap.SimpleEntry<>(decode(cols[0]), null);
            } else if (cols.length == 2) {
                return new AbstractMap.SimpleEntry<>(decode(cols[0]), decode(cols[1]));
            } else {
                throw new IllegalArgumentException(columns);
            }
        }
    }

    static Text decode(String s) {
        Text t = new Text();
        byte[] sb = s.getBytes(StandardCharsets.UTF_8);

        for (int i = 0; i < sb.length; ++i) {
            if (sb[i] == 37) {
                ++i;
                int x = i++;
                if (i >= sb.length) {
                    throw new IllegalArgumentException("Invalid characters in encoded string (" + s + "). Expected two characters after '%'");
                }

                byte[] hex = new byte[] {sb[x], sb[i]};
                String hs = new String(hex, StandardCharsets.UTF_8);
                int b = Integer.parseInt(hs, 16);
                t.append(new byte[] {(byte) b}, 0, 1);
            } else {
                t.append(new byte[] {sb[i]}, 0, 1);
            }
        }

        return t;
    }
}
