package datawave.query.predicate;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;

import datawave.query.data.parsers.DatawaveKey;
import datawave.query.jexl.JexlASTHelper;

/**
 * An EventDataQueryFilter for TermFrequencies, for use in a TLDQuery
 */
public class TLDTermFrequencyEventDataQueryFilter implements EventDataQueryFilter {

    private final Set<String> indexOnlyFields;
    private final Set<String> fields;

    public TLDTermFrequencyEventDataQueryFilter(Set<String> indexOnlyFields, Set<String> fields) {
        this.indexOnlyFields = indexOnlyFields;
        this.fields = fields;
    }

    @Override
    public void startNewDocument(Key documentKey) {
        // no-op
    }

    @Override
    public boolean apply(@Nullable Map.Entry<Key,String> entry) {
        // accept all
        return true;
    }

    @Override
    public boolean peek(@Nullable Map.Entry<Key,String> entry) {
        // accept all
        return true;
    }

    /**
     * Only keep the tf key if it isn't the root pointer or if it is index only and contributes to document evaluation
     *
     * @param k
     *            the key
     * @return true if this key should be kept
     */
    @Override
    public boolean keep(Key k) {
        DatawaveKey key = new DatawaveKey(k);
        return (!TLDEventDataFilter.isRootPointer(k) || indexOnlyFields.contains(key.getFieldName())) && fieldMatches(k);
    }

    private boolean fieldMatches(Key key) {
        DatawaveKey parser = new DatawaveKey(key);
        String fieldName = JexlASTHelper.deconstructIdentifier(parser.getFieldName());
        return fields.contains(fieldName);
    }

    @Override
    public EventDataQueryFilter clone() {
        return this;
    }

    @Override
    public Range getSeekRange(Key current, Key endKey, boolean endKeyInclusive) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMaxNextCount() {
        return -1;
    }

    @Override
    public Key transform(Key toTransform) {
        throw new UnsupportedOperationException();
    }
}
