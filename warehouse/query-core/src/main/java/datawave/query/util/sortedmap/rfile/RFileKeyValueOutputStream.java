package datawave.query.util.sortedmap.rfile;

import datawave.query.util.sortedmap.FileSortedMap;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

public class RFileKeyValueOutputStream extends RFileKeyValueOutputStreamBase implements FileSortedMap.SortedSetOutputStream<Map.Entry<Key,Value>> {
    public RFileKeyValueOutputStream(OutputStream stream) throws IOException {
        super(stream);
    }

    @Override
    public void writeObject(Map.Entry<Key,Value> obj) throws IOException {
        writeKeyValue(obj);
    }
}
