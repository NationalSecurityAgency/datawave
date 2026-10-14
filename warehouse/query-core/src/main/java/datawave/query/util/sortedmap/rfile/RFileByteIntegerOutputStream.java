package datawave.query.util.sortedmap.rfile;

import java.io.IOException;
import java.io.OutputStream;

import datawave.query.util.sortedmap.FileSortedMap;

public class RFileByteIntegerOutputStream extends RFileKeyValueOutputStreamBase implements FileSortedMap.SortedMapOutputStream<byte[],Integer> {

    public RFileByteIntegerOutputStream(OutputStream stream) throws IOException {
        super(stream);
    }

    @Override
    public void writeObject(byte[] key, Integer value) throws IOException {
        writeKeyValue(KeyValueTransformUtils.byteToKey(key), KeyValueTransformUtils.intToValue(value));

    }
}
