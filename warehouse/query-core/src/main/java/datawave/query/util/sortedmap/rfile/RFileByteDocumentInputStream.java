package datawave.query.util.sortedmap.rfile;

import datawave.query.attributes.Document;
import org.apache.accumulo.core.data.Key;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static datawave.query.util.sortedset.rfile.KeyValueByteDocumentTransforms.byteDocumentToKeyValue;
import static datawave.query.util.sortedset.rfile.KeyValueByteDocumentTransforms.keyValueToByteDocument;

public class RFileByteDocumentInputStream extends RFileKeyValueInputStreamBase<Map.Entry<byte[],Document>> {

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
