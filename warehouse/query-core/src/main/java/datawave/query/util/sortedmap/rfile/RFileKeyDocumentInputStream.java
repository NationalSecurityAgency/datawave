package datawave.query.util.sortedmap.rfile;

import static datawave.query.util.sortedmap.rfile.KeyValueByteDocumentTransforms.keyValueToKeyDocument;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.accumulo.core.data.Key;

import datawave.query.attributes.Document;
import datawave.query.util.sortedmap.FileSortedMap;

public class RFileKeyDocumentInputStream extends RFileKeyValueInputStreamBase implements FileSortedMap.SortedMapInputStream<Key,Document> {

    public RFileKeyDocumentInputStream(InputStream inputStream, long length) throws IOException {
        super(inputStream, length);
    }

    public RFileKeyDocumentInputStream(InputStream inputStream, long length, Key start, Key end) throws IOException {
        super(inputStream, length, start, end);
    }

    @Override
    public Map.Entry<Key,Document> readObject() throws IOException {
        return keyValueToKeyDocument(super.readKeyValue());
    }
}
