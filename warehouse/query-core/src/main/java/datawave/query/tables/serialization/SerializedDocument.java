package datawave.query.tables.serialization;

import datawave.query.attributes.Document;
import org.apache.accumulo.core.data.Key;

public class SerializedDocument implements SerializedDocumentIfc{

    private final Document doc;

    public SerializedDocument(Document doc){
        this.doc=doc;
    }
    @Override
    public Key computeKey() {
        return this.doc.isMetadataSet() ? this.doc.getMetadata() : null;
    }

    @Override
    public <T> T getAs(Class<?> as) {
        return null;
    }

    @Override
    public int compareTo(SerializedDocumentIfc other) {
        if (other instanceof SerializedDocument){
            return this.doc.compareTo( ((SerializedDocument) other).doc);
        }
        return -1;
    }

    @Override
    public long size() {
        return doc.sizeInBytes();
    }
}
