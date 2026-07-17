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
    private final RootPointerPredicate isRootPointer = new RootPointerPredicate();

    public TLDTermFrequencyEventDataQueryFilter(Set<String> indexOnlyFields, Set<String> fields) {
        this.indexOnlyFields = indexOnlyFields;
        this.fields = fields;
    }

    @Override
    public void startNewDocument(Key documentKey) {
        isRootPointer.startNewDocument(documentKey);
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
        return (!isRootPointer.test(k) || containsField(indexOnlyFields, key.getFieldName())) && fieldMatches(k);
    }

    private boolean fieldMatches(Key key) {
        DatawaveKey parser = new DatawaveKey(key);
        String fieldName = JexlASTHelper.deconstructIdentifier(parser.getFieldName());
        return containsField(fields, fieldName);
    }

    /**
     * Return whether the given set of (un-grouped) field names contains the candidate field, accepting both exact matches and grouped/content-context variants
     * of a field in the set, e.g. {@code QUOTE} in the set matching a candidate of {@code QUOTE.1234}.
     *
     * @param fieldNames
     *            a set of field names, expected to not carry grouping/content-context notation
     * @param candidateField
     *            the field to check, which may carry grouping/content-context notation
     * @return true if the candidate field is present in, or a grouped variant of a field in, the given set
     */
    private static boolean containsField(Set<String> fieldNames, String candidateField) {
        if (fieldNames.contains(candidateField)) {
            return true;
        }

        for (String fieldName : fieldNames) {
            if (JexlASTHelper.isGroupedFieldMatch(fieldName, candidateField)) {
                return true;
            }
        }

        return false;
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
