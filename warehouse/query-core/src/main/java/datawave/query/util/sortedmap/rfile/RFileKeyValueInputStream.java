package datawave.query.util.sortedmap.rfile;

import datawave.query.util.sortedmap.FileSortedMap;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class RFileKeyValueInputStream extends RFileKeyValueInputStreamBase implements FileSortedMap.SortedMapInputStream<Key,Value>  {

    public RFileKeyValueInputStream(InputStream inputStream, long length) throws IOException {
        super(inputStream, length);
    }

    public RFileKeyValueInputStream(InputStream inputStream, long length, Key start, Key end) throws IOException {
        super(inputStream, length, start, end);
    }

    @Override
    public Map.Entry<Key,Value> readObject() throws IOException {
        return super.readKeyValue();
    }
}
