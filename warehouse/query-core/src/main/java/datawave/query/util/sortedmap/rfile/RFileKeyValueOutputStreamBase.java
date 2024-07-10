package datawave.query.util.sortedmap.rfile;

import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

public class RFileKeyValueOutputStreamBase {
    private RFileWriter writer;
    static final Value EMPTY_VALUE = new Value(new byte[0]);

    public RFileKeyValueOutputStreamBase(OutputStream stream) throws IOException {
        super();
        this.writer = RFile.newWriter().to(stream).withVisibilityCacheSize(10).build();
    }

    public void writeKeyValue(Key key, Value value) throws IOException {
        writer.append(key, value);
    }

    public void writeKeyValue(Map.Entry<Key,Value> keyValue) throws IOException {
        writer.append(keyValue.getKey(), keyValue.getValue());
    }

    public void writeSize(int i) throws IOException {
        writeKeyValue(SizeKeyUtil.getKey(i), EMPTY_VALUE);
    }

    public void close() throws IOException {
        writer.close();
        writer = null;
    }

    public static class SizeKeyUtil {
        private static final char MAX_UNICODE = (char) Character.MAX_CODE_POINT;
        public static final Text SIZE_ROW = new Text(MAX_UNICODE + "_SIZE_" + MAX_UNICODE);

        public static Key getKey(int size) {
            return new Key(SIZE_ROW, new Text(Integer.toString(size)));
        }

        public static boolean isSizeKey(Key key) {
            return key.getRow().equals(SIZE_ROW);
        }

        public static int getSize(Key key) {
            return Integer.parseInt(key.getColumnFamily().toString());
        }
    }
}
