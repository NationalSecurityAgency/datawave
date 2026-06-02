package datawave.query.util.sortedmap.rfile;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

public abstract class RFileKeyValueOutputStreamBase {
    private final OutputStream outputStream;
    private RFileWriter writer;
    private static final Value EMPTY_VALUE = new Value(new byte[0]);

    public RFileKeyValueOutputStreamBase(OutputStream outputStream) throws IOException {
        this.outputStream = outputStream;
        this.writer = RFile.newWriter().to(outputStream).withVisibilityCacheSize(10).build();
    }

    public void writeKeyValue(Key k, Value v) throws IOException {
        writer.append(k, v);
    }

    public void writeSize(int size) throws IOException {
        writer.append(SizeKeyUtil.getKey(size), EMPTY_VALUE);
    }

    public void close() throws IOException {
        if (writer != null) {
            writer.close();
            writer = null;
        }
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
