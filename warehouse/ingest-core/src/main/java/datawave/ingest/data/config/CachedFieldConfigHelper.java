package datawave.ingest.data.config;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

public class CachedFieldConfigHelper implements FieldConfigHelper {
    private final static Logger log = LoggerFactory.getLogger(CachedFieldConfigHelper.class);

    private final static float DEFAULT_LRU_LF = 0.75f;
    private final static int EMIT_OVER_LIMIT_THRESHOLD = 100;

    private final FieldConfigHelper underlyingHelper;
    private final Map<String,CachedEntry> resultCache;
    private final Function<String,CachedEntry> resultEntryFn;

    private long fieldComputes;
    private boolean fieldLimitExceeded;

    enum AttributeType {
        INDEXED_FIELD, REVERSE_INDEXED_FIELD, TOKENIZED_FIELD, REVERSE_TOKENIZED_FIELD, STORED_FIELD, INDEX_ONLY_FIELD
    }

    public CachedFieldConfigHelper(FieldConfigHelper helper, int limit) {
        this(helper, limit, false);
    }

    public CachedFieldConfigHelper(FieldConfigHelper helper, int limit, boolean debugLimitExceeded) {
        if (limit < 1) {
            throw new IllegalArgumentException("Limit must be a positive integer");
        }
        this.underlyingHelper = helper;
        this.resultCache = lruCache(limit);
        this.resultEntryFn = !debugLimitExceeded ? CachedEntry::new : (String f) -> {
            fieldComputes++;
            if (fieldComputes >= limit && ((fieldComputes == limit) || (fieldComputes % EMIT_OVER_LIMIT_THRESHOLD) == 0)) {
                fieldLimitExceeded = true;
                log.info("Field cache limit exceeded [val: {}, size={}, limit={}]", f, fieldComputes, limit);
            }
            return new CachedEntry(f);
        };
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
        return resultCache.computeIfAbsent(fieldName, resultEntryFn).get(attributeType).getResultOrEvaluate(fn);
    }

    @VisibleForTesting
    boolean hasLimitExceeded() {
        return fieldLimitExceeded;
    }

    private static <K,V> Map<K,V> lruCache(final int maxSize) {
        // Testing showed slightly better or same performance of LRU implementation below
        // when compared to Apache Commons LRUMap
        return new LinkedHashMap<>((int) (maxSize / DEFAULT_LRU_LF) + 1, DEFAULT_LRU_LF, true) {
            protected boolean removeEldestEntry(Map.Entry<K,V> eldest) {
                return size() > maxSize;
            }
        };
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
