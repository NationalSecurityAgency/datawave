package datawave.query.util.sortedmap.rfile;

import static datawave.query.util.sortedmap.rfile.KeyValueByteDocumentTransforms.byteToKey;
import static datawave.query.util.sortedmap.rfile.KeyValueByteDocumentTransforms.keyValueToByteDocument;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import datawave.query.attributes.Document;
import datawave.query.util.sortedmap.FileSortedMap;

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
