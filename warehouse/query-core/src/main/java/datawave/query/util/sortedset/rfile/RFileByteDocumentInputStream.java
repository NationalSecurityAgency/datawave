package datawave.query.util.sortedset.rfile;

import static datawave.query.util.sortedset.rfile.KeyValueByteDocumentTransforms.byteDocumentToKeyValue;
import static datawave.query.util.sortedset.rfile.KeyValueByteDocumentTransforms.keyValueToByteDocument;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.accumulo.core.data.Key;

import datawave.query.attributes.Document;
import datawave.query.util.sortedset.FileSortedSet;

public class RFileByteDocumentInputStream extends RFileKeyValueInputStreamBase implements FileSortedSet.SortedSetInputStream<Map.Entry<byte[],Document>> {

    public RFileByteDocumentInputStream(InputStream inputStream, long length) throws IOException {
        super(inputStream, length);
    }

    public RFileByteDocumentInputStream(InputStream inputStream, long length, Key start, Key end) throws IOException {
        super(inputStream, length, start, end);
    }

    public RFileByteDocumentInputStream(InputStream inputStream, long length, Map.Entry<byte[],Document> start, Map.Entry<byte[],Document> end)
                    throws IOException {
        super(inputStream, length, byteDocumentToKeyValue(start), byteDocumentToKeyValue(end));
    }

    @Override
    public Map.Entry<byte[],Document> readObject() throws IOException {
        return keyValueToByteDocument(readKeyValue());
    }
}
