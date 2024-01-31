package datawave.query.function.serializer;

import java.nio.ByteBuffer;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

import datawave.query.DocumentSerialization;
import datawave.query.attributes.Document;

public abstract class DocumentSerializer implements Function<Entry<Key,Document>,Entry<Key,Value>> {
    protected boolean reducedResponse;
    protected final int compression;
    protected final int minCompressionSize;
    protected final String concreteName;

    public static final int DEFAULT_MIN_COMPRESS_SIZE = 1024 * 10;

    public DocumentSerializer(boolean reducedResponse, boolean allowCompression) {
        this(reducedResponse, allowCompression, DEFAULT_MIN_COMPRESS_SIZE);
    }

    public DocumentSerializer(boolean reducedResponse, boolean allowCompression, int minCompressionSize) {
        this.reducedResponse = reducedResponse;
        this.compression = allowCompression ? DocumentSerialization.GZIP : DocumentSerialization.NONE;
        this.minCompressionSize = minCompressionSize;
        this.concreteName = this.getClass().getSimpleName();
    }

    public boolean isReducedResponse() {
        return reducedResponse;
    }

    public void setReducedResponse(boolean reducedResponse) {
        this.reducedResponse = reducedResponse;
    }

    @Override
    public Entry<Key,Value> apply(Entry<Key,Document> from) {
        byte[] bytes = serialize(from.getValue());
        Value v = getValue(bytes);
        return Maps.immutableEntry(from.getKey(), v);
    }

    public abstract byte[] serialize(Document d);

    protected Value getValue(byte[] document) {
        byte[] header;
        byte[] dataToWrite;

        // Only compress the data if it's greater than minCompressionSize in size (bytes)
        if (DocumentSerialization.NONE != this.compression && document.length > minCompressionSize) {
            header = DocumentSerialization.getHeader(compression);
            dataToWrite = DocumentSerialization.writeBody(document, this.compression);
        } else {
            header = DocumentSerialization.getHeader();
            dataToWrite = document;
        }

        ByteBuffer buf = ByteBuffer.allocate(header.length + dataToWrite.length);
        buf.put(header);
        buf.put(dataToWrite);

        return new Value(buf.array());
    }

}
