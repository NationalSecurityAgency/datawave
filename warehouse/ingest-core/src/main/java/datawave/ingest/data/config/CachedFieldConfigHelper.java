package datawave.ingest.data.config;

import java.util.Map;
import java.util.function.Predicate;

import org.apache.commons.collections4.map.LRUMap;

import com.google.common.annotations.VisibleForTesting;

public class CachedFieldConfigHelper implements FieldConfigHelper {
    private final FieldConfigHelper underlyingHelper;
    private final Map<String,CachedEntry> resultCache;

    enum AttributeType {
        INDEXED_FIELD, REVERSE_INDEXED_FIELD, TOKENIZED_FIELD, REVERSE_TOKENIZED_FIELD, STORED_FIELD, INDEX_ONLY_FIELD
    }

    public CachedFieldConfigHelper(FieldConfigHelper helper, int limit) {
        if (limit < 1) {
            throw new IllegalArgumentException("Limit must be a positive integer");
        }
        this.underlyingHelper = helper;
        this.resultCache = new LRUMap<>(limit);
    }

    @Override
    public boolean isStoredField(String fieldName) {
        return getFieldResult(AttributeType.STORED_FIELD, fieldName, underlyingHelper::isStoredField);
    }

    @Override
    public boolean isIndexedField(String fieldName) {
        return getFieldResult(AttributeType.INDEXED_FIELD, fieldName, underlyingHelper::isIndexedField);
    }

    @Override
    public boolean isIndexOnlyField(String fieldName) {
        return getFieldResult(AttributeType.INDEX_ONLY_FIELD, fieldName, underlyingHelper::isIndexOnlyField);
    }

    @Override
    public boolean isReverseIndexedField(String fieldName) {
        return getFieldResult(AttributeType.REVERSE_INDEXED_FIELD, fieldName, underlyingHelper::isReverseIndexedField);
    }

    @Override
    public boolean isTokenizedField(String fieldName) {
        return getFieldResult(AttributeType.TOKENIZED_FIELD, fieldName, underlyingHelper::isTokenizedField);
    }

    @Override
    public boolean isReverseTokenizedField(String fieldName) {
        return getFieldResult(AttributeType.REVERSE_TOKENIZED_FIELD, fieldName, underlyingHelper::isReverseTokenizedField);
    }

    @VisibleForTesting
    boolean getFieldResult(AttributeType attributeType, String fieldName, Predicate<String> fn) {
        return resultCache.computeIfAbsent(fieldName, CachedEntry::new).get(attributeType).getResultOrEvaluate(fn);
    }

    private static class CachedEntry {
        private final String fieldName;
        private final MemoizedResult indexed;
        private final MemoizedResult reverseIndexed;
        private final MemoizedResult stored;
        private final MemoizedResult indexedOnly;
        private final MemoizedResult tokenized;
        private final MemoizedResult reverseTokenized;

        private CachedEntry(String fieldName) {
            this.fieldName = fieldName;
            this.indexed = new MemoizedResult();
            this.reverseIndexed = new MemoizedResult();
            this.stored = new MemoizedResult();
            this.indexedOnly = new MemoizedResult();
            this.tokenized = new MemoizedResult();
            this.reverseTokenized = new MemoizedResult();
        }

        private MemoizedResult get(AttributeType attributeType) {
            MemoizedResult result;
            switch (attributeType) {
                case INDEX_ONLY_FIELD:
                    result = indexedOnly;
                    break;
                case INDEXED_FIELD:
                    result = indexed;
                    break;
                case REVERSE_INDEXED_FIELD:
                    result = reverseIndexed;
                    break;
                case TOKENIZED_FIELD:
                    result = tokenized;
                    break;
                case REVERSE_TOKENIZED_FIELD:
                    result = reverseTokenized;
                    break;
                case STORED_FIELD:
                    result = stored;
                    break;
                default:
                    throw new IllegalArgumentException("Undefined attribute type: " + attributeType);
            }
            return result;
        }

        private class MemoizedResult {
            private boolean resultEvaluated;
            private boolean result;

            private boolean getResultOrEvaluate(Predicate<String> evaluateFn) {
                if (!resultEvaluated) {
                    result = evaluateFn.test(fieldName);
                    resultEvaluated = true;
                }
                return result;
            }
        }
    }
}
