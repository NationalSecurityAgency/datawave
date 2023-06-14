package datawave.query.iterator;

import datawave.query.attributes.Document;
import org.apache.accumulo.core.data.Key;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 *
 */
public class DocumentSpecificNestedIterator extends NestedQueryIterator<Key> {
    private Map.Entry<Key,Document> documentKey;
    private Map.Entry<Key,Document> next;
    private Map.Entry<Key,Document> current;

    public DocumentSpecificNestedIterator(Map.Entry<Key,Document> documentKey) {
        setDocumentKey(documentKey);
    }

    public void setDocumentKey(Map.Entry<Key,Document> documentKey) {
        this.documentKey = documentKey;
        this.next = documentKey;
    }

    public Map.Entry<Key,Document> getDocumentKey() {
        return documentKey;
    }

    @Override
    public void initialize() {}

    @Override
    public Key move(Key minimum) {
        if (minimum.compareTo(this.documentKey.getKey()) <= 0) {
            this.next = documentKey;
        } else {
            this.next = null;
        }
        return this.next == null ? null : this.next.getKey();
    }

    @Override
    public Collection<NestedIterator<Key>> leaves() {
        return Collections.EMPTY_LIST;
    }

    @Override
    public Collection<NestedIterator<Key>> children() {
        return Collections.EMPTY_LIST;
    }

    @Override
    public Document document() {
        return (this.current == null ? null : this.current.getValue());
    }

    @Override
    public boolean hasNext() {
        return (this.next != null && this.next.getKey() != null);
    }

    @Override
    public Key next() {
        this.current = this.next;
        this.next = null;
        return (this.current == null ? null : this.current.getKey());
    }

    @Override
    public void remove() {
        if (this.current == this.documentKey) {
            this.documentKey = null;
        }
    }
}
