package datawave.query.iterator.aggregation;

import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * A "struct" containing the Key and Iterator&lt;Entry&lt;Key,Value&gt;&gt; for a Document
 *
 *
 *
 */
public class DocumentData implements Comparable<DocumentData> {
    // the master doc key
    private Key key;
    // the other doc keys (e.g. children in the TLD case)
    private Set<Key> docKeys;
    // the attributes
    private List<Entry<Key,Value>> data;
    // whether the data originated from the index
    private final boolean fromIndex;

    public DocumentData(Key key, Set<Key> docKeys, List<Entry<Key,Value>> data, boolean fromIndex) {
        this.setKey(key);
        this.setDocKeys(docKeys);
        this.setData(data);
        this.fromIndex = fromIndex;
    }

    public Key getKey() {
        return key;
    }

    public void setKey(Key key) {
        this.key = key;
    }

    public Set<Key> getDocKeys() {
        return docKeys;
    }

    public void setDocKeys(Set<Key> docKeys) {
        this.docKeys = docKeys;
    }

    public List<Entry<Key,Value>> getData() {
        return data;
    }

    public void setData(List<Entry<Key,Value>> data) {
        this.data = data;
    }

    public boolean isFromIndex() {
        return fromIndex;
    }

    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder(7, 11);
        hcb.append(key.hashCode());
        hcb.append(data.hashCode());
        return hcb.toHashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof DocumentData) {
            DocumentData other = (DocumentData) o;

            return key.equals(other.key) && docKeys.equals(other.docKeys) && data.equals(other.data);
        }

        return false;
    }

    @Override
    public int compareTo(DocumentData o) {
        int keyCompare = key.compareTo(o.getKey());
        if (keyCompare == 0) {
            if (data.equals(o.getData())) {
                return 0;
            } else {
                return -1;
            }
        }

        return keyCompare;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(key).append(", ").append(docKeys).append(", ").append(data);
        return sb.toString();
    }
}
