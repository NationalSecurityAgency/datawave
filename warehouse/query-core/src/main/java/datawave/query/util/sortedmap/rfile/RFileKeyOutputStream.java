package datawave.query.util.sortedmap.rfile;

import datawave.query.util.sortedmap.FileSortedMap;
import org.apache.accumulo.core.data.Key;

import java.io.IOException;
import java.io.OutputStream;

public class RFileKeyOutputStream extends RFileKeyValueOutputStreamBase implements FileSortedMap.SortedSetOutputStream<Key> {
    public RFileKeyOutputStream(OutputStream stream) throws IOException {
        super(stream);
    }

    @Override
    public void writeObject(Key o) throws IOException {
        writeKeyValue(o, EMPTY_VALUE);
    }
}
