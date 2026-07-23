package datawave.query.transformer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.collect.TreeMultimap;

import datawave.data.type.LcNoDiacriticsType;
import datawave.marking.MarkingFunctions;
import datawave.query.attributes.Document;
import datawave.query.attributes.TemporalGranularity;
import datawave.query.attributes.TypeAttribute;
import datawave.query.attributes.UniqueFields;
import datawave.query.common.grouping.GroupFields;
import datawave.query.iterator.GroupingIterator;

/**
 * Demonstration/regression harness for the bugs documented in {@code grouping-unique-intermediate-results-findings.md}.
 * <p>
 * These tests are deliberately written to <em>pass against the current (buggy) code</em> so that they serve as an executable reproduction of each defect. Each
 * test's Javadoc states the behavior that <em>should</em> be observed once the corresponding fix is applied; when a fix lands, the assertions here should be
 * updated to encode the corrected behavior (the "AFTER FIX" expectations noted inline).
 */
public class GroupingUniqueIntermediateResultBugTest {

    @BeforeAll
    public static void setup() {
        MarkingFunctions.Factory.createMarkingFunctions();
    }

    private static MarkingFunctions<?> markingFunctions() {
        return MarkingFunctions.Factory.createMarkingFunctions();
    }

    private static Map.Entry<Key,Document> documentEntry(String field, String value, int uid) {
        Key key = new Key("row", "dt uid-" + uid);
        Document document = new Document(key, true);
        document.put(field, new TypeAttribute<>(new LcNoDiacriticsType(value), new Key("cf", "cq"), true), true);
        return new AbstractMap.SimpleEntry<>(key, document);
    }

    // =================================================================================================================
    // Finding 1: GroupingTransform never resets its per-page timer after emitting an intermediate result, so once the
    // timeout elapses EVERY subsequent apply() emits another intermediate result with no accumulation in between. In the
    // asynchronous results-thread path (RunningQuery.getResultsThread) this manifests as a flood of intermediate/PARTIAL
    // pages returned back to back ("multiple intermediate result pages at the same time").
    // =================================================================================================================

    @Test
    public void groupingTransform_emitsIntermediateFlood_becauseTimerIsNeverReset() {
        GroupFields groupFields = new GroupFields();
        TreeMultimap<String,TemporalGranularity> groupBy = TreeMultimap.create();
        groupBy.put("FIELD_A", TemporalGranularity.ALL);
        groupFields.setGroupByFieldMap(groupBy);

        // 1ms page timeout, and pretend the page started well in the past so the timeout is already exceeded.
        GroupingTransform transform = new GroupingTransform(groupFields, markingFunctions(), 1L);
        transform.setQueryExecutionForPageStartTime(System.currentTimeMillis() - 10_000L);

        int intermediateCount = 0;
        for (int i = 0; i < 5; i++) {
            Map.Entry<Key,Document> result = transform.apply(documentEntry("FIELD_A", "value-" + i, i));
            assertNotNull(result, "buggy transform returns an intermediate rather than accumulating");
            assertTrue(result.getValue().isIntermediateResult());
            intermediateCount++;
        }

        // CURRENT (buggy) behavior: all 5 apply() calls emit an intermediate result because the timer is never reset.
        assertEquals(5, intermediateCount);

        // AFTER FIX (reset queryExecutionForPageStartTime when emitting an intermediate): only the first apply() would
        // emit an intermediate; the remaining calls (occurring < 1ms later) would accumulate and return null. Expected:
        // intermediateCount == 1.
    }

    // =================================================================================================================
    // Finding 1 (Unique variant): UniqueTransform has the same missing reset. Once the timeout elapses, every duplicate
    // document produces another intermediate result.
    // =================================================================================================================

    @Test
    public void uniqueTransform_emitsIntermediateFlood_onDuplicatesBecauseTimerIsNeverReset() throws Exception {
        UniqueFields uniqueFields = new UniqueFields();
        uniqueFields.put("FIELD", TemporalGranularity.ALL);

        UniqueTransform transform = new UniqueTransform.Builder().withUniqueFields(uniqueFields).withQueryExecutionForPageTimeout(1L).build();
        transform.setQueryExecutionForPageStartTime(System.currentTimeMillis() - 10_000L);

        // The first occurrence of the value is unique and is returned as a real result (not an intermediate).
        Map.Entry<Key,Document> firstResult = transform.apply(documentEntry("FIELD", "same", 0));
        assertNotNull(firstResult);
        assertFalse(firstResult.getValue().isIntermediateResult());

        // Every subsequent duplicate, with the timeout already exceeded, emits an intermediate result.
        int intermediateCount = 0;
        for (int i = 1; i <= 4; i++) {
            Map.Entry<Key,Document> result = transform.apply(documentEntry("FIELD", "same", i));
            assertNotNull(result);
            assertTrue(result.getValue().isIntermediateResult());
            intermediateCount++;
        }

        // CURRENT (buggy) behavior: all 4 duplicates emit an intermediate result.
        assertEquals(4, intermediateCount);

        // AFTER FIX: only the first duplicate after the timeout would emit an intermediate; the timer would then reset
        // so the following duplicates return null until a fresh timeout window elapses. Expected: intermediateCount == 1.
    }

    // =================================================================================================================
    // Finding 2: mostRecent UniqueTransform never emits an intermediate result at all. On the map != null (mostRecent)
    // code path, apply() always returns null before reaching the timeout check, so a long-running mostRecent-unique
    // accumulation produces no keep-alive pages. (Documented by inspection; a full reproduction requires the
    // ivarator/HDFS-backed map setup used by UniqueTransformMostRecentTest.)
    // =================================================================================================================

    // =================================================================================================================
    // Finding 3: GroupingIterator.hasNext() is not idempotent. It both (a) contradicts the class Javadoc which claims
    // "there can be no saved state in this class", and (b) consumes a fresh batch of input and overwrites the
    // previously-prepared flattened document every time it is called. Calling hasNext() twice before next() therefore
    // silently discards a whole batch of grouped results.
    // =================================================================================================================

    @Test
    public void groupingIterator_doubleHasNext_silentlyDiscardsAGroup() {
        GroupFields groupFields = new GroupFields();
        TreeMultimap<String,TemporalGranularity> groupBy = TreeMultimap.create();
        groupBy.put("FIELD_A", TemporalGranularity.ALL);
        groupFields.setGroupByFieldMap(groupBy);

        List<Map.Entry<Key,Document>> input = new ArrayList<>();
        input.add(documentEntry("FIELD_A", "alpha", 0));
        input.add(documentEntry("FIELD_A", "beta", 1));
        Iterator<Map.Entry<Key,Document>> source = input.iterator();

        // Batch size of 1 => each batch produces one distinct group.
        GroupingIterator iterator = new GroupingIterator(source, markingFunctions(), groupFields, 1, null);

        // First hasNext() reads 'alpha' and prepares its flattened document.
        assertTrue(iterator.hasNext());
        // Second hasNext() (a legal, idempotent operation per the Iterator contract) reads 'beta' and OVERWRITES the
        // prepared 'alpha' document. 'alpha' is now lost.
        assertTrue(iterator.hasNext());

        Document surviving = iterator.next().getValue();
        // Only the 'beta' grouping survived the double hasNext().
        assertEquals("beta", surviving.get("FIELD_A.0").getData().toString());

        // The source is now exhausted and the 'alpha' group was never returned: two distinct groups existed but only one
        // was ever retrievable.
        assertFalse(iterator.hasNext());

        // AFTER FIX (make hasNext() idempotent / honor the stateless contract): both 'alpha' and 'beta' groups would be
        // returned across two next() calls regardless of how many times hasNext() is called between them.
    }

    /**
     * Control: with the correct one-hasNext-per-next() usage, both distinct groups are returned. This isolates the data loss above to the extra hasNext() call
     * rather than to the grouping logic itself.
     */
    @Test
    public void groupingIterator_singleHasNextPerNext_returnsAllGroups() {
        GroupFields groupFields = new GroupFields();
        TreeMultimap<String,TemporalGranularity> groupBy = TreeMultimap.create();
        groupBy.put("FIELD_A", TemporalGranularity.ALL);
        groupFields.setGroupByFieldMap(groupBy);

        List<Map.Entry<Key,Document>> input = new ArrayList<>();
        input.add(documentEntry("FIELD_A", "alpha", 0));
        input.add(documentEntry("FIELD_A", "beta", 1));

        GroupingIterator iterator = new GroupingIterator(input.iterator(), markingFunctions(), groupFields, 1, null);

        int groupsReturned = 0;
        while (iterator.hasNext()) {
            Document d = iterator.next().getValue();
            assertNotNull(d.get("FIELD_A.0"));
            groupsReturned++;
        }
        assertEquals(2, groupsReturned);
    }
}
