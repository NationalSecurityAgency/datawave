package datawave.ingest.data.config;

import static java.lang.Thread.NORM_PRIORITY;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class CachedFieldConfigHelper implements FieldConfigHelper {
    private final static Logger log = LoggerFactory.getLogger(CachedFieldConfigHelper.class);

    private final static float DEFAULT_LRU_LF = 0.75f;
    private final static int DEFAULT_DEBUG_STATE_SECS = 30;

    private final FieldConfigHelper underlyingHelper;
    private final LruCache<String,CachedEntry> resultCache;
    private final boolean debugLimitsEnabled;
    private final int limit;
    private final Set<String> debugFieldUnique;
    private final ScheduledExecutorService debugStateExecutor;
    private final AtomicLong debugFieldComputes;

    enum AttributeType {
        INDEXED_FIELD, REVERSE_INDEXED_FIELD, TOKENIZED_FIELD, REVERSE_TOKENIZED_FIELD, STORED_FIELD, INDEX_ONLY_FIELD
    }

    public CachedFieldConfigHelper(FieldConfigHelper helper, int limit) {
        this(helper, limit, false);
    }

    public CachedFieldConfigHelper(FieldConfigHelper helper, int limit, boolean debugLimitEnabled) {
        if (limit < 1) {
            throw new IllegalArgumentException("Limit must be a positive integer");
        }
        this.underlyingHelper = helper;
        this.resultCache = new LruCache<>(limit);
        this.limit = limit;
        this.debugLimitsEnabled = debugLimitEnabled;
        this.debugFieldUnique = new HashSet<>();
        this.debugFieldComputes = new AtomicLong();

        if (debugLimitEnabled) {
            this.debugStateExecutor = Executors.newSingleThreadScheduledExecutor(
            // @formatter:off
                new ThreadFactoryBuilder()
                    .setPriority(NORM_PRIORITY)
                    .setDaemon(true)
                    .setNameFormat("CachedFieldConfigHelper.DebugState")
                    .build()
                // formatter:off
            );
            this.debugStateExecutor.scheduleAtFixedRate(this::debugLogState, DEFAULT_DEBUG_STATE_SECS, DEFAULT_DEBUG_STATE_SECS, SECONDS);
        } else {
            this.debugStateExecutor = null;
        }
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
        CachedEntry ce = !debugLimitsEnabled ?
            resultCache.computeIfAbsent(fieldName, CachedEntry::new) :
            resultCache.computeIfAbsent(fieldName, this::debugCachedEntryCreation);
        return ce.get(attributeType).getResultOrEvaluate(fn);
    }

    @VisibleForTesting
    boolean hasLimitExceeded() {
        return resultCache.hasLimitExceeded();
    }

    private CachedEntry debugCachedEntryCreation(String fieldName) {
        debugFieldComputes.incrementAndGet();
        debugFieldUnique.add(fieldName);
        return new CachedEntry(fieldName);
    }

    private void debugLogState() {
        if (resultCache.hasLimitExceeded()) {
            log.info("Field cache LRU limit exceeded [limit={}, debug={}, size={}, uniq={}]",
                limit, debugFieldComputes.get(), debugFieldUnique.size(), debugLimitsEnabled);
        }
    }

    private static class LruCache<K,V> extends LinkedHashMap<K,V> {
        private final int maxSize;
        private volatile boolean limitExceeded;

        LruCache(int maxSize) {
            super((int)(maxSize / DEFAULT_LRU_LF) + 1, DEFAULT_LRU_LF, true);
            this.maxSize = maxSize;
        }

        boolean hasLimitExceeded() {
            // thread-safe
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
