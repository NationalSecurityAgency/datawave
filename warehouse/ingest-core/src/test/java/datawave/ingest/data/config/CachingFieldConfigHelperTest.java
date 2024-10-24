package datawave.ingest.data.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

public class CachingFieldConfigHelperTest {
    @Test
    public void testCachingBehaviorWillCallBaseMethods() {
        String fieldName = "test";
        FieldConfigHelper mockHelper = mock(FieldConfigHelper.class);
        FieldConfigHelper cachedHelper = new CachedFieldConfigHelper(mockHelper, 1);

        cachedHelper.isIndexOnlyField(fieldName);
        verify(mockHelper).isIndexOnlyField(eq(fieldName));

        cachedHelper.isIndexedField(fieldName);
        verify(mockHelper).isIndexedField(eq(fieldName));

        cachedHelper.isTokenizedField(fieldName);
        verify(mockHelper).isTokenizedField(eq(fieldName));

        cachedHelper.isStoredField(fieldName);
        verify(mockHelper).isStoredField(eq(fieldName));

        cachedHelper.isReverseIndexedField(fieldName);
        verify(mockHelper).isReverseIndexedField(eq(fieldName));

        cachedHelper.isReverseTokenizedField(fieldName);
        verify(mockHelper).isReverseTokenizedField(eq(fieldName));
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0})
    public void testConstructorWithNonPositiveLimitWillThrow(int limit) {
        assertThrows(IllegalArgumentException.class, () -> new CachedFieldConfigHelper(mock(FieldConfigHelper.class), limit));
    }

    @SuppressWarnings("ClassEscapesDefinedScope")
    @ParameterizedTest
    @EnumSource(CachedFieldConfigHelper.AttributeType.class)
    public void testAttributeTypesDoNotThrow(CachedFieldConfigHelper.AttributeType attributeType) {
        String fieldName = "test";
        FieldConfigHelper mockHelper = mock(FieldConfigHelper.class);
        CachedFieldConfigHelper cachedHelper = new CachedFieldConfigHelper(mockHelper, 1);
        cachedHelper.getFieldResult(attributeType, fieldName, (f) -> true);
    }

    @Test
    public void testCachingLimitsBetweenFieldsAndAttributeTypes() {
        AtomicLong storedCounter = new AtomicLong();
        AtomicLong indexCounter = new AtomicLong();
        FieldConfigHelper innerHelper = mock(FieldConfigHelper.class);
        CachedFieldConfigHelper helper = new CachedFieldConfigHelper(innerHelper, 2);

        when(innerHelper.isStoredField(any())).then((a) -> {
            storedCounter.incrementAndGet();
            return true;
        });

        when(innerHelper.isIndexedField(any())).then((a) -> {
            indexCounter.incrementAndGet();
            return true;
        });

        // following ensures that:
        // 1. fields are computed, where appropriate per attribute-type
        // 2. limit allows cache results to return
        // 3. limit blocks results to return if exceeded
        // 4. limit functions across attribute-types

        helper.getFieldResult(CachedFieldConfigHelper.AttributeType.STORED_FIELD, "field1", innerHelper::isStoredField);
        assertEquals(1, storedCounter.get(), "field1 should compute result (new field)");

        helper.getFieldResult(CachedFieldConfigHelper.AttributeType.STORED_FIELD, "field1", innerHelper::isStoredField);
        assertEquals(1, storedCounter.get(), "field1 repeated (existing field)");

        helper.getFieldResult(CachedFieldConfigHelper.AttributeType.STORED_FIELD, "field2", innerHelper::isStoredField);
        assertEquals(2, storedCounter.get(), "field2 should compute result (new field)");

        helper.getFieldResult(CachedFieldConfigHelper.AttributeType.STORED_FIELD, "field2", innerHelper::isStoredField);
        assertEquals(2, storedCounter.get(), "field2 repeated (existing)");

        helper.getFieldResult(CachedFieldConfigHelper.AttributeType.INDEXED_FIELD, "field1", innerHelper::isIndexedField);
        assertEquals(1, indexCounter.get(), "field1 should compute result (new attribute)");

        helper.getFieldResult(CachedFieldConfigHelper.AttributeType.STORED_FIELD, "field3", innerHelper::isStoredField);
        assertEquals(3, storedCounter.get(), "field3 exceeded limit (new field)");

        helper.getFieldResult(CachedFieldConfigHelper.AttributeType.STORED_FIELD, "field3", innerHelper::isStoredField);
        assertEquals(3, storedCounter.get(), "field3 exceeded limit (existing field)");

        // LRU map should evict field #2
        // we access field #1 above which has more accesses over field #2
        helper.getFieldResult(CachedFieldConfigHelper.AttributeType.STORED_FIELD, "field2", innerHelper::isStoredField);
        assertEquals(4, storedCounter.get(), "field1 exceeded limit (new field/eviction)");
    }
}
