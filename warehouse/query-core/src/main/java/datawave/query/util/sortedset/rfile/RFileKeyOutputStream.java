package datawave.query.util.sortedset.rfile;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.accumulo.core.data.Key;

import datawave.query.util.sortedset.FileSortedSet;

public class RFileKeyOutputStream extends RFileKeyValueOutputStreamBase implements FileSortedSet.SortedSetOutputStream<Key> {
    public RFileKeyOutputStream(OutputStream stream) throws IOException {
        super(stream);
    }

    @Override
    public void writeObject(Key o) throws IOException {
        writeKeyValue(o, EMPTY_VALUE);
    }
}
