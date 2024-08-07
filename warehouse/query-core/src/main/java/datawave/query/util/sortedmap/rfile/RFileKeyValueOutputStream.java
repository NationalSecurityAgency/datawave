package datawave.query.util.sortedmap.rfile;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import datawave.query.util.sortedmap.FileSortedMap;

public class RFileKeyValueOutputStream extends RFileKeyValueOutputStreamBase implements FileSortedMap.SortedMapOutputStream<Key,Value> {
    public RFileKeyValueOutputStream(OutputStream stream) throws IOException {
        super(stream);
    }

    @Override
    public void writeObject(Key k, Value v) throws IOException {
        writeKeyValue(k, v);
    }
}
