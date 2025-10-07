package datawave.query.util.sortedset;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

public class RFileKeyOutputStream implements FileSortedSet.SortedSetOutputStream<Key> {
    private RFileWriter writer;
    private static final Value EMPTY_VALUE = new Value(new byte[0]);

    public RFileKeyOutputStream(OutputStream stream) throws IOException {
        super();
        this.writer = RFile.newWriter().to(stream).withVisibilityCacheSize(10).build();
    }

    @Override
    public void writeObject(Key o) throws IOException {
        writer.append(o, EMPTY_VALUE);
    }

    @Override
    public void writeSize(int i) throws IOException {
        writeObject(SizeKeyUtil.getKey(i));
    }

    @Override
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
