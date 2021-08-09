package datawave.ingest.table.aggregator;

import datawave.ingest.protobuf.Uid;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GlobalIndexUidAggregatorExpandedTest {
    
    private static final Key KEY = new Key("key");
    
    private GlobalIndexUidAggregator agg = new GlobalIndexUidAggregator();
    
    @Before
    public void setup() {
        agg.reset();
    }

    @Test
    public void exceedMaxIgnoresCountsUid() {
        agg = new GlobalIndexUidAggregator(2);
        Value value1 = UidTestBuilder.newBuilder()
                .withCountOnly(3)
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid3", "uid4")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(5)
                .build());

        testCombinations(expectation, asList(value1, value2));
    }

    @Test
    public void overMaxAddTwoDropOne() {
        agg = new GlobalIndexUidAggregator(2);
        Value value1 = UidTestBuilder.newBuilder()
                .withCountOnly(3)
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid3", "uid4")
                .build();

        Value value3 = UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid5")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(4)
                .build());

        testCombinations(expectation, asList(value1, value2, value3));
    }

    @Test
    public void seenIgnoreAddsTwoValues() {
        agg = new GlobalIndexUidAggregator(2);
        Value value1 = UidTestBuilder.newBuilder()
                .withCountOnly(3)
                .build();

        // while count-only, just add 2 to count = 5
        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2")
                .build();

        // while count-only, add one for uid4 and subtract for uid2 = 5
        Value value3 = UidTestBuilder.newBuilder()
                .withUids("uid4")
                .withRemovals("uid2") // happens to match prior value's uid2
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(5)
                .build());

        // This currently fails in reverse mode because of the inconsistency in how remove uids are treated
        // for seenIgnore = true, the removal uid results in a deduction of the count
        // for seenIgnore = false, the removal only impacts the count if the removal uid is in the uid list
        testForwardOnly(expectation, asList(value1, value2, value3));
    }

    @Test
    public void seenIgnoreAddsTwoValuesReverse() {
        // uid2 from value2 will be removed because of the removal UID from value1
        agg = new GlobalIndexUidAggregator(2);

        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid4")
                .withRemovals("uid2")
                .build();

        // after value1 and value2 are combined, uids=uid1,uid4 (count of 2), uid2 is still in the remove list
        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2")
                .build();

        // count-only encountered, so current removal count subtracted from uid count 2-1 = 1
        // add three to it, so count is 4
        Value value3 = UidTestBuilder.newBuilder()
                .withCountOnly(3)
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(4)
                .build());

        // This currently fails in reverse mode because of the inconsistency in how remove uids are treated
        // for seenIgnore = true, the removal uid results in a deduction of the count
        // for seenIgnore = false, the removal only impacts the count if the removal uid is in the uid list
        testForwardOnly(expectation, asList(value1, value2, value3));
    }

    @Test
    public void seenIgnoreAddsTwoValuesNoMatch() {
        agg = new GlobalIndexUidAggregator(2);
        Value value1 = UidTestBuilder.newBuilder()
                .withCountOnly(3)
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2")
                .build();

        Value value3 = UidTestBuilder.newBuilder()
                .withUids("uid4")
                .withRemovals("uid3") // does not match prior value's uid list
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(5)
                .build());

        testCombinations(expectation, asList(value1, value2, value3));
    }

    @Test
    public void testEvaluatesMaxWithEachAddition() { // 40 - behaves this way as an optimization.  Different between forward and reverse
        agg = new GlobalIndexUidAggregator(2);
        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2")
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid3")
                .build();

        Value value3 = UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid1")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(2)
                .build());

        testForwardOnly(expectation, asList(value1, value2, value3));
    }

    @Test
    public void testHitsMax() { // 41
        agg = new GlobalIndexUidAggregator(2);
        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2")
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid3")
                .build();

        Value value3 = UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid4")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(2)
                .build());

        testCombinations(expectation, asList(value1, value2, value3));
    }

    @Test
    public void testDeduplicates() { // 42
        agg = new GlobalIndexUidAggregator(2);
        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2")
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid1")
                .build();

        Value value3 = UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid2")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withUids("uid1")
                .withRemovals("uid2")
                .build());

        testCombinations(expectation, asList(value1, value2, value3));
    }

    @Test
    public void testDeduplicatesVariant() { // 43
        agg = new GlobalIndexUidAggregator(2);
        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2")
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid1")
                .build();

        Value value3 = UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid4")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2")
                .withRemovals("uid4")
                .build());

        testCombinations(expectation, asList(value1, value2, value3));
    }

    @Test
    public void testDuplicatesRecountedUnderSeenIgnoreReverse() { // 45 - inconsistent between forward and reverse
        agg = new GlobalIndexUidAggregator(10);
        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2")
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2")
                .build();

        Value value3 = UidTestBuilder.newBuilder()
                .withCountOnly(11)
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(13)
                .build());

        testForwardOnly(expectation, asList(value1, value2, value3));
    }

    @Test
    public void testRemovalToZero() { // 46
        agg = new GlobalIndexUidAggregator(10);
        Value value1 = UidTestBuilder.newBuilder()
                .withCountOnly(1)
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid1")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(0)
                .build());

        testCombinations(expectation, asList(value1, value2));
    }


    @Test
    public void floorOfZeroVariant() {
        agg = new KeepCountOnlyUidAggregator(10);
        Value value1 = UidTestBuilder.newBuilder()
                .withCountOnly(1)
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid1", "uid2")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(0)
                .withRemovals()
                .build());

        testCombinations(expectation, asList(value1, value2));
    }

    @Test
    public void countOnlyReduced() {
        agg = new GlobalIndexUidAggregator(10);
        Value value1 = UidTestBuilder.newBuilder()
                .withCountOnly(30)
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid1", "uid2")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(28)
                .withRemovals()
                .build());

        testCombinations(expectation, asList(value1, value2));
    }


    @Test
    public void testAddToNowDefunctNegativeCount() {
        agg = new GlobalIndexUidAggregator(10);

        // The -1 will be changed to zero as negative numbers are the legacy format.
        Value value1 = UidTestBuilder.newBuilder()
                .withCountOnly(-1)
                .build();

        // Combine zero with a named uid brings the count to one.
        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid1")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(1)
                .build());

        testForwardOnly(expectation, asList(value1, value2));
    }

    @Test
    public void testAddToNowDefunctNegativeCountReverse() {
        agg = new GlobalIndexUidAggregator(10);

        // Combine zero with a named uid brings the count to one.
        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid1")
                .build();

        // The -1 will be changed to zero as negative numbers are the legacy format.
        Value value2 = UidTestBuilder.newBuilder()
                .withCountOnly(-1)
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(0)
                .build());

        testForwardOnly(expectation, asList(value1, value2));
    }

    @Test
    public void testRemoveFromNowDefunctNegativeCount() {
        agg = new KeepCountOnlyUidAggregator(10);
        Value value1 = UidTestBuilder.newBuilder()
                .withCountOnly(-1)
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid1")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(0)
                .build());

        testCombinations(expectation, asList(value1, value2));
    }

    private void testFullCompactionsOnly(Uid.List expectation, List<Value> input) {
        // There should be nothing in the removal UID list after a Full Major Compaction

        Uid.List expectNoRemovals = Uid.List.newBuilder().mergeFrom(expectation).clearREMOVEDUID().build();

        verify("Forward, Full Major", expectNoRemovals, testAsFullMajorCompaction(input));

        // reverse the ordering of the input and try again
        Collections.reverse(input);

        verify("Reverse, Full Major", expectNoRemovals, testAsFullMajorCompaction(input));
    }

    private void testForwardOnly(Uid.List expectation, List<Value> input) {
        // There should be nothing in the removal UID list after a Full Major Compaction

        Uid.List expectNoRemovals = Uid.List.newBuilder().mergeFrom(expectation).clearREMOVEDUID().build();

        verify("Forward, Partial Major", expectation, testPartialCompaction(input));
        verify("Forward, Full Major", expectNoRemovals, testAsFullMajorCompaction(input));
    }

    private void testCombinations(Uid.List expectation, List<Value> input) {
        // There should be nothing in the removal UID list after a Full Major Compaction

        Uid.List expectNoRemovals = Uid.List.newBuilder().mergeFrom(expectation).clearREMOVEDUID().build();

        verify("Forward, Partial Major", expectation, testPartialCompaction(input));
        verify("Forward, Full Major", expectNoRemovals, testAsFullMajorCompaction(input));

        // reverse the ordering of the input and try again
        Collections.reverse(input);

        verify("Reverse, Partial Major", expectation, testPartialCompaction(input));
        verify("Reverse, Full Major", expectNoRemovals, testAsFullMajorCompaction(input));
    }

    private Uid.List testPartialCompaction(List<Value> values) {
        agg.reset();
        agg.propogate = true;
        return UidTestBuilder.valueToUidList(agg.reduce(KEY, values.iterator()));
    }

    private Uid.List testAsFullMajorCompaction(List<Value> values) {
        agg.reset();
        agg.propogate = false;
        return UidTestBuilder.valueToUidList(agg.reduce(KEY, values.iterator()));
    }

    private void verify(String label, Uid.List expectation, Uid.List result) {
        assertEquals("getIGNORE differs - " + label, expectation.getIGNORE(), result.getIGNORE());

        for (String expectedUid: expectation.getUIDList()) {
            assertTrue("UID list missing " + expectedUid + " - " + label, result.getUIDList().contains(expectedUid));
        }
        assertEquals("UID count differs - " + label + " " + result.getUIDList(), expectation.getCOUNT(), result.getCOUNT());
        assertEquals("UID list size differs - " + label, expectation.getUIDList().size(), result.getUIDList().size());
        // The count and UID List sizes should match unless seenIgnore = true
        if (!expectation.getIGNORE()) {
            assertEquals("Invalid test state: expected Uid.List size and getCount differ - " + label, expectation.getCOUNT(), expectation.getUIDList().size());
        }

        for (String expectedRemovalUid: expectation.getREMOVEDUIDList()) {
            assertTrue("Remove UID list missing " + expectedRemovalUid + " - " + label, result.getREMOVEDUIDList().contains(expectedRemovalUid));
        }
        assertEquals("Removed count differs - " + label, expectation.getREMOVEDUIDCount(), result.getREMOVEDUIDCount());
        assertEquals("Removed UID list size differs - " + label, expectation.getREMOVEDUIDList().size(), result.getREMOVEDUIDList().size());
    }
}
