package datawave.query.util.sortedmap.rfile;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import datawave.query.util.sortedmap.FileSortedMap;

public class RFileByteIntegerInputStream extends RFileKeyValueInputStreamBase implements FileSortedMap.SortedMapInputStream<byte[],Integer> {

    public RFileByteIntegerInputStream(InputStream inputStream, long length) throws IOException {
        super(inputStream, length);
    }

    public RFileByteIntegerInputStream(InputStream inputStream, long length, byte[] start, byte[] end) throws IOException {
        super(inputStream, length, KeyValueTransformUtils.byteToKey(start), KeyValueTransformUtils.byteToKey(end));
    }

    @Override
    public Map.Entry<byte[],Integer> readObject() throws IOException {
        return KeyValueTransformUtils.keyValueToByteInteger(readKeyValue());
    }
}
