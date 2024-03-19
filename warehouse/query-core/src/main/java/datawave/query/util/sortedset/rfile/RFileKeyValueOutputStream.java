package datawave.query.util.sortedset.rfile;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import datawave.query.util.sortedset.FileSortedSet;

public class RFileKeyValueOutputStream extends RFileKeyValueOutputStreamBase implements FileSortedSet.SortedSetOutputStream<Map.Entry<Key,Value>> {
    public RFileKeyValueOutputStream(OutputStream stream) throws IOException {
        super(stream);
    }

    @Override
    public void writeObject(Map.Entry<Key,Value> obj) throws IOException {
        writeKeyValue(obj);
    }
}
