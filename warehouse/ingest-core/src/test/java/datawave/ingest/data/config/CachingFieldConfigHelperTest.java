package datawave.ingest.data.config;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class CachingFieldConfigHelperTest {
    @SuppressWarnings("unchecked")
    @Test
    public void testCachingBehaviorWillCallBaseMethods() {
        // @formatter:off
        Stream.of(new Object[][] {
            new Object[] {
                (BiConsumer<FieldConfigHelper, String>) FieldConfigHelper::isIndexOnlyField,
                    (BiConsumer<FieldConfigHelper, String>) (h, f) -> verify(h).isIndexOnlyField(eq(f)),
                (BiConsumer<FieldConfigHelper, String>) FieldConfigHelper::isIndexedField,
                    (BiConsumer<FieldConfigHelper, String>) (h, f) -> verify(h).isIndexedField(eq(f)),
                (BiConsumer<FieldConfigHelper, String>) FieldConfigHelper::isTokenizedField,
                    (BiConsumer<FieldConfigHelper, String>) (h, f) -> verify(h).isTokenizedField(eq(f)),
                (BiConsumer<FieldConfigHelper, String>) FieldConfigHelper::isStoredField,
                    (BiConsumer<FieldConfigHelper, String>) (h, f) -> verify(h).isStoredField(eq(f)),
                (BiConsumer<FieldConfigHelper, String>) FieldConfigHelper::isReverseIndexedField,
                    (BiConsumer<FieldConfigHelper, String>) (h, f) -> verify(h).isReverseIndexedField(eq(f)),
                (BiConsumer<FieldConfigHelper, String>) FieldConfigHelper::isReverseTokenizedField,
                    (BiConsumer<FieldConfigHelper, String>) (h, f) -> verify(h).isReverseTokenizedField(eq(f)),
            }
        }).forEach(arg -> {
            // param[0] = helper method
            // param[1] = validation method
            String fieldName = "testField";
            BiConsumer<FieldConfigHelper, String> testAction = (BiConsumer<FieldConfigHelper, String>) arg[0];
            BiConsumer<FieldConfigHelper, String> verifyAction = (BiConsumer<FieldConfigHelper, String>) arg[1];
            FieldConfigHelper mockHelper = mock(FieldConfigHelper.class);
            FieldConfigHelper cachedHelper = new CachedFieldConfigHelper(mockHelper, 1);
            testAction.accept(cachedHelper, fieldName);
            verifyAction.accept(mockHelper, fieldName);
        });
        // @formatter:on
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0})
    public void testConstructorWithNonPositiveLimitWillThrow(int limit) {
        assertThrows(IllegalArgumentException.class, () -> new CachedFieldConfigHelper(mock(FieldConfigHelper.class), limit));
    }

    @Test
    public void testCachingLimitsBetweenFieldsAndAttributeTypes() {
        AtomicLong counter = new AtomicLong();
        CachedFieldConfigHelper helper = new CachedFieldConfigHelper(mock(FieldConfigHelper.class), 2);
        Function<String,Boolean> fn = (f) -> {
            counter.incrementAndGet();
            return true;
        };

        // following ensures that:
        // 1. fields are computed, where appropriate per attribute-type
        // 2. limit allows cache results to return
        // 3. limit blocks results to return if exceeded
        // 4. limit functions across attribute-types

        helper.getOrEvaluate(CachedFieldConfigHelper.AttributeType.STORED_FIELD, "field1", fn);
        Assertions.assertEquals(1, counter.get(), "field1 should compute result (new field)");

        helper.getOrEvaluate(CachedFieldConfigHelper.AttributeType.STORED_FIELD, "field1", fn);
        Assertions.assertEquals(1, counter.get(), "field1 repeated (existing field)");

        helper.getOrEvaluate(CachedFieldConfigHelper.AttributeType.STORED_FIELD, "field2", fn);
        Assertions.assertEquals(2, counter.get(), "field2 should compute result (new field)");

        helper.getOrEvaluate(CachedFieldConfigHelper.AttributeType.STORED_FIELD, "field2", fn);
        Assertions.assertEquals(2, counter.get(), "field2 repeated (existing)");

        helper.getOrEvaluate(CachedFieldConfigHelper.AttributeType.INDEXED_FIELD, "field1", fn);
        Assertions.assertEquals(3, counter.get(), "field1 should compute result (new attribute)");

        helper.getOrEvaluate(CachedFieldConfigHelper.AttributeType.STORED_FIELD, "field3", fn);
        Assertions.assertEquals(4, counter.get(), "field3 exceeded limit (new field)");

        helper.getOrEvaluate(CachedFieldConfigHelper.AttributeType.STORED_FIELD, "field3", fn);
        Assertions.assertEquals(4, counter.get(), "field3 exceeded limit (existing field)");

        // LRU map should evict field #2
        // we access field #1 above which has more accesses over field #2
        helper.getOrEvaluate(CachedFieldConfigHelper.AttributeType.STORED_FIELD, "field2", fn);
        Assertions.assertEquals(5, counter.get(), "field1 exceeded limit (new field/eviction)");
    }
}
