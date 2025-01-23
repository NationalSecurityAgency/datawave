package datawave.ingest.data.config;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

public class CachedFieldConfigHelper implements FieldConfigHelper {
    private final static Logger log = LoggerFactory.getLogger(CachedFieldConfigHelper.class);

    private final static float DEFAULT_LRU_LF = 0.75f;
    private final static int DEFAULT_DIAGNOSTIC_SECS = 30;

    private final FieldConfigHelper underlyingHelper;
    private final LruCache<String,CachedEntry> resultCache;
    private final int limit;
    private final boolean diagnosticEnabled;
    private final Set<String> diagnosticUniqueFields;
    private Clock clock;
    private boolean limitMessageEmitted;
    private long diagnosticFieldCompute;
    private long diagnosticEmitIntervalMillis;
    private long diagnosticEmitNextMillis;
    private boolean diagnosticEmitted;

    enum AttributeType {
        INDEXED_FIELD, REVERSE_INDEXED_FIELD, TOKENIZED_FIELD, REVERSE_TOKENIZED_FIELD, STORED_FIELD, INDEX_ONLY_FIELD
    }

    interface Clock {
        default long epochMillis() {
            return System.currentTimeMillis();
        }

        static Clock defaultClock() {
            return new Clock() {};
        }
    }

    public CachedFieldConfigHelper(FieldConfigHelper helper, int limit) {
        this(helper, limit, false);
    }

    public CachedFieldConfigHelper(FieldConfigHelper helper, int limit, boolean diagnosticEnabled) {
        if (limit < 1) {
            throw new IllegalArgumentException("Limit must be a positive integer");
        }
        this.clock = Clock.defaultClock();
        this.underlyingHelper = helper;
        this.resultCache = new LruCache<>(limit);
        this.limit = limit;
        this.diagnosticEnabled = diagnosticEnabled;
        this.diagnosticUniqueFields = new HashSet<>();
        this.diagnosticEmitIntervalMillis = SECONDS.toMillis(DEFAULT_DIAGNOSTIC_SECS);
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
        CachedEntry cachedEntry = resultCache.computeIfAbsent(fieldName, (key) -> {
            if (diagnosticEnabled) {
                diagnosticFieldCompute++;
                diagnosticUniqueFields.add(key);
            }
            return new CachedEntry(key);
        });

        CachedEntry.MemoizedResult memoizedResult = cachedEntry.get(attributeType);

        // when trace state is enabled - emit a message if the field limit has been exceeded
        // the intent is to help adjust the size required for the cache
        if (diagnosticEnabled && clock.epochMillis() > diagnosticEmitNextMillis) {
            diagnosticEmitted = true;
            diagnosticEmitNextMillis = clock.epochMillis() + diagnosticEmitIntervalMillis;
            log.info("Field cache LRU [limit={}, computed={}, size={}, uniq={}]", limit, diagnosticFieldCompute, diagnosticUniqueFields.size(),
                            diagnosticUniqueFields);
        } else if (resultCache.hasLimitExceeded() && !limitMessageEmitted) {
            log.info("Field cache LRU limit exceeded: [limit={}, field={}]", limit, fieldName);
            limitMessageEmitted = true;
        }
        return memoizedResult.getResultOrEvaluate(fn);
    }

    @VisibleForTesting
    boolean hasLimitExceeded() {
        return resultCache.hasLimitExceeded();
    }

    @VisibleForTesting
    Set<String> getCachedFields() {
        return resultCache.keySet();
    }

    @VisibleForTesting
    boolean getDiagnosticEmitted() {
        return diagnosticEmitted;
    }

    @VisibleForTesting
    Set<String> getDiagnosticUniqueFields() {
        return diagnosticUniqueFields;
    }

    @VisibleForTesting
    long getDiagnosticFieldCompute() {
        return diagnosticFieldCompute;
    }

    @VisibleForTesting
    long getDiagnosticEmitNextMillis() {
        return diagnosticEmitNextMillis;
    }

    @VisibleForTesting
    void setDiagnosticEmitIntervalMillis(long intervalMillis) {
        this.diagnosticEmitIntervalMillis = intervalMillis;
    }

    @VisibleForTesting
    void setClock(Clock clock) {
        this.clock = clock;
    }

    private static class LruCache<K,V> extends LinkedHashMap<K,V> {
        private final int maxSize;
        private boolean limitExceeded;

        LruCache(int maxSize) {
            super((int) (maxSize / DEFAULT_LRU_LF) + 1, DEFAULT_LRU_LF, true);
            this.maxSize = maxSize;
        }

        boolean hasLimitExceeded() {
            return limitExceeded;
        }

        protected boolean removeEldestEntry(Map.Entry<K,V> eldest) {
            boolean localLimitExceeded = size() > maxSize;
            if (localLimitExceeded) {
                limitExceeded = true;
            }
            return localLimitExceeded;
        }
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
