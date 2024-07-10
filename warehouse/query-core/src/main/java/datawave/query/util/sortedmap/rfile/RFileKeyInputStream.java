package datawave.query.util.sortedmap.rfile;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class RFileKeyInputStream extends RFileKeyValueInputStreamBase<Key> {

    public RFileKeyInputStream(InputStream inputStream, long length) throws IOException {
        super(inputStream, length);
    }

    public RFileKeyInputStream(InputStream inputStream, long length, Key start, Key end) throws IOException {
        super(inputStream, length, start, end);
    }

    @Override
    public Key readObject() throws IOException {
        Map.Entry<Key,Value> obj = readKeyValue();
        return (obj == null ? null : obj.getKey());
    }

}
