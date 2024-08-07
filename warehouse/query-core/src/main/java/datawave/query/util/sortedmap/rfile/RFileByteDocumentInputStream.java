package datawave.query.util.sortedmap.rfile;

import datawave.query.attributes.Document;
import datawave.query.util.sortedmap.FileSortedMap;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static datawave.query.util.sortedmap.rfile.KeyValueByteDocumentTransforms.byteToKey;
import static datawave.query.util.sortedmap.rfile.KeyValueByteDocumentTransforms.keyValueToByteDocument;

public class RFileByteDocumentInputStream extends RFileKeyValueInputStreamBase implements FileSortedMap.SortedMapInputStream<byte[],Document> {

    public RFileByteDocumentInputStream(InputStream inputStream, long length) throws IOException {
        super(inputStream, length);
    }

    public RFileByteDocumentInputStream(InputStream inputStream, long length, byte[] start, byte[] end) throws IOException {
        super(inputStream, length, byteToKey(start), byteToKey(end));
    }

    @Override
    public Map.Entry<byte[],Document> readObject() throws IOException {
        return keyValueToByteDocument(readKeyValue());
    }
}
