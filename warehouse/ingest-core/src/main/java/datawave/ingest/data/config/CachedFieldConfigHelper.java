package datawave.ingest.data.config;

import java.util.EnumMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.commons.collections4.map.LRUMap;

import com.google.common.annotations.VisibleForTesting;

public class CachedFieldConfigHelper implements FieldConfigHelper {
    private final FieldConfigHelper underlyingHelper;
    private final Map<String,ResultEntry> resultCache;

    enum AttributeType {
        INDEXED_FIELD, REVERSE_INDEXED_FIELD, TOKENIZED_FIELD, REVERSE_TOKENIZED_FIELD, STORED_FIELD, INDEXED_ONLY
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
        return getOrEvaluate(AttributeType.STORED_FIELD, fieldName, underlyingHelper::isStoredField);
    }

    @Override
    public boolean isIndexedField(String fieldName) {
        return getOrEvaluate(AttributeType.INDEXED_FIELD, fieldName, underlyingHelper::isIndexedField);
    }

    @Override
    public boolean isIndexOnlyField(String fieldName) {
        return getOrEvaluate(AttributeType.INDEXED_ONLY, fieldName, underlyingHelper::isIndexOnlyField);
    }

    @Override
    public boolean isReverseIndexedField(String fieldName) {
        return getOrEvaluate(AttributeType.REVERSE_INDEXED_FIELD, fieldName, underlyingHelper::isReverseIndexedField);
    }

    @Override
    public boolean isTokenizedField(String fieldName) {
        return getOrEvaluate(AttributeType.TOKENIZED_FIELD, fieldName, underlyingHelper::isTokenizedField);
    }

    @Override
    public boolean isReverseTokenizedField(String fieldName) {
        return getOrEvaluate(AttributeType.REVERSE_TOKENIZED_FIELD, fieldName, underlyingHelper::isReverseTokenizedField);
    }

    @VisibleForTesting
    boolean getOrEvaluate(AttributeType attributeType, String fieldName, Function<String,Boolean> evaluateFn) {
        return resultCache.computeIfAbsent(fieldName, ResultEntry::new).resolveResult(attributeType, evaluateFn);
    }

    private static class ResultEntry {
        private final String fieldName;
        private final EnumMap<AttributeType,Boolean> resultMap;

        ResultEntry(String fieldName) {
            this.fieldName = fieldName;
            this.resultMap = new EnumMap<>(AttributeType.class);
        }

        boolean resolveResult(AttributeType attributeType, Function<String,Boolean> evaluateFn) {
            return resultMap.computeIfAbsent(attributeType, (t) -> evaluateFn.apply(fieldName));
        }
    }
}
