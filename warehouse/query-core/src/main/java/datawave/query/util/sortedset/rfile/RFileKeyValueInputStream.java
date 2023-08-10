package datawave.query.util.sortedset.rfile;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import datawave.query.util.sortedset.FileSortedSet;

public class RFileKeyValueInputStream extends RFileKeyValueInputStreamBase<Map.Entry<Key,Value>> {

    public RFileKeyValueInputStream(InputStream inputStream, long length) throws IOException {
        super(inputStream, length);
    }

    public RFileKeyValueInputStream(InputStream inputStream, long length, Key start, Key end) throws IOException {
        super(inputStream, length, start, end);
    }

    public RFileKeyValueInputStream(InputStream inputStream, long length, Map.Entry<Key,Value> start, Map.Entry<Key,Value> end) throws IOException {
        super(inputStream, length, start, end);
    }

    @Override
    public Map.Entry<Key,Value> readObject() throws IOException {
        return super.readKeyValue();
    }
}
