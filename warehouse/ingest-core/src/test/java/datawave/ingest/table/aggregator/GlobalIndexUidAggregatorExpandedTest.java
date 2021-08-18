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
    private boolean isFullCompactionOnlyTest = false;
    private boolean isPartialCompactionOnlyTest = false;
    private boolean isForwardOnlyTest = false;

    @Before
    public void setup() {
        agg.reset();
    }

    @Test
    public void twoSingleUids() {
        // Do UID lists get combined across values?
        Value value1 = UidTestBuilder.uidList("uid1");
        Value value2 = UidTestBuilder.uidList("uid2");

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2")
                .build());

        testCombinations(expectation, asList(value1, value2));
    }

    @Test
    public void removalAcrossValue() {
        // Does a UID removal work across Values?
        // Are UID removals maintained in partial compactions?
        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2", "uid3")
                .withRemovals("uid4") // uid4 in other value's list
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid4", "uid5", "uid6")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2", "uid3", "uid5", "uid6")
                .withRemovals("uid4")
                .build());

        testCombinations(expectation, asList(value1, value2));
    }

    @Test
    public void removalAbsent() {
        // Are UID removals maintained in partial compactions, without a match?
        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2", "uid3")
                .withRemovals("uid7") // uid7 absent from both lists
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid4", "uid5", "uid6")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2", "uid3", "uid4", "uid5", "uid6")
                .withRemovals("uid7")
                .build());

        testCombinations(expectation, asList(value1, value2));
    }

    @Test
    public void removalsCrissCross() {
        // Do UID removals work across Values in both directions?
        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2", "uid3")
                .withRemovals("uid4") // uid4 in other list
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid4", "uid5", "uid6")
                .withRemovals("uid1") // uid1 in other list
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withUids("uid2", "uid3", "uid5", "uid6")
                .withRemovals("uid1", "uid4")
                .build());

        testCombinations(expectation, asList(value1, value2));
    }

    @Test
    public void removalCrissCrossAndAbsent() {
        // Does a mixture of a removal-that-hits and a removal-that-doesn't-hit work?
        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2", "uid3")
                .withRemovals("uid4") // uid4 in other list
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid4", "uid5", "uid6")
                .withRemovals("uid7") // uid7 absent
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2", "uid3", "uid5", "uid6")
                .withRemovals("uid4", "uid7")
                .build());

        testCombinations(expectation, asList(value1, value2));
    }

    @Test
    public void removeAllAcrossValue() {
        // Do removals of all UIDs in another list work?
        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2", "uid3")
                .withRemovals("uid4", "uid5", "uid6") // all uids in other list
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid4", "uid5", "uid6")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2", "uid3")
                .withRemovals("uid4", "uid5", "uid6")
                .build());

        testCombinations(expectation, asList(value1, value2));
    }

    @Test
    public void removeMultipleAbsent() {
        // Are multiple removals persisted in partial compactions, even without matching?
        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2", "uid3")
                .withRemovals("uid7", "uid8", "uid9") // all uids absent
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid4", "uid5", "uid6")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2", "uid3", "uid4", "uid5", "uid6")
                .withRemovals("uid7", "uid8", "uid9") // all uids absent
                .build());

        testCombinations(expectation, asList(value1, value2));
    }

    @Test
    public void removeAllAcrossBothValues() {
        // Do multiple removals wipe UIDs across Values and persist on partial compactions?
        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2", "uid3")
                .withRemovals("uid4", "uid5", "uid6") // all uids in other list
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid4", "uid5", "uid6")
                .withRemovals("uid1", "uid2", "uid3") // all uids in other list
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid1", "uid2", "uid3", "uid4", "uid5", "uid6")
                .build());

        testCombinations(expectation, asList(value1, value2));
    }

    @Test
    public void removeAllAcrossValuePlusAbsent() {
        // Do multiple removals-that-hit and multiple removals-that-don't-hit work like single removals?
        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2", "uid3")
                .withRemovals("uid4", "uid5", "uid6") // all UIDs in other list
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid4", "uid5", "uid6")
                .withRemovals("uid7", "uid8", "uid9") // all absent
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2", "uid3")
                .withRemovals("uid4", "uid5", "uid6", "uid7", "uid8", "uid9")
                .build());

        testCombinations(expectation, asList(value1, value2));
    }

    @Test
    public void removeMixture() {
        // Mixture of hit and non-hit removals within one Value.
        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2", "uid3")
                .withRemovals("uid4", "uid6", "uid7") // two UIDs in other list, one absent
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid4", "uid5", "uid6")
                .withRemovals("uid8", "uid9") // all absent
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2", "uid3", "uid5")
                .withRemovals("uid4", "uid6", "uid7", "uid8", "uid9")
                .build());

        testCombinations(expectation, asList(value1, value2));
    }

    @Test
    public void extraIdInRemovals() {
        // Another variation of a mixture of hit and non-hit removals within one Value.
        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2", "uid3")
                .withRemovals("uid4", "uid5", "uid6", "uid7") // all uids in other list, one absent
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid4", "uid5", "uid6")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2", "uid3")
                .withRemovals("uid4", "uid5", "uid6", "uid7")
                .build());

        testCombinations(expectation, asList(value1, value2));
    }

    @Test
    public void extraIdsInFullRemovals() {
        // Do extra non-hit-removals persist when mixed with hit-removals?
        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2", "uid3")
                .withRemovals("uid4", "uid5", "uid6", "uid7") // all uids in other list, one absent
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid4", "uid5", "uid6")
                .withRemovals("uid1", "uid2", "uid3", "uid8") // all uids in other list, one absent
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid1", "uid2", "uid3", "uid4", "uid5", "uid6", "uid7", "uid8")
                .build());

        testCombinations(expectation, asList(value1, value2));
    }

    @Test
    public void loneRemovalValue() {
        // Does a single removal work when seen in the second Value?
        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2")
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid2") // removal of one uid in other list
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withUids("uid1")
                .withRemovals("uid2")
                .build());

        testCombinations(expectation, asList(value1, value2));
    }

    @Test
    public void loneRemovalExtra() {
        // Does a non-matching removal persist?
        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2")
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid3") // removal of one UID, not matching other list
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2")
                .withRemovals("uid3")
                .build());

        testCombinations(expectation, asList(value1, value2));
    }

    @Test
    public void removalAvoidsExceedingMax() {
        // With a lowered maximum, is count-only mode avoided with a matching removal?
        agg = new GlobalIndexUidAggregator(2);
        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2")
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid3")
                .withRemovals("uid1") // removal of one UID in other list
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withUids("uid2", "uid3")
                .withRemovals("uid1")
                .build());

        testCombinations(expectation, asList(value1, value2));
    }

    @Test
    public void exceedMax() {
        // In exceeding the maximum, is count-only mode reached?
        agg = new GlobalIndexUidAggregator(2);
        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2")
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid3")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(3)
                .build());

        testCombinations(expectation, asList(value1, value2));
    }

    @Test
    public void overMaxTwice() {
        // Do counts add properly in count-only mode?
        Value value1 = UidTestBuilder.newBuilder()
                .withCountOnly(3)
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withCountOnly(3)
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(6)
                .build());

        testCombinations(expectation, asList(value1, value2));
    }

    @Test
    public void doNotGoNegativeForNamedUids() {
        // Are removals persisted in partial compaction mode?
        Value value1 = UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid4", "uid5", "uid6", "uid7")
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid1", "uid2", "uid3", "uid8")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid1", "uid2", "uid3", "uid4", "uid5", "uid6", "uid7", "uid8")
                .build());

        testCombinations(expectation, asList(value1, value2));
    }

    @Test
    public void testBadDataHandling() {
        // Does a legacy Value (no UIDs, not count-only, yet a count=1) get effectively ignored?
        Value value1 = UidTestBuilder.newBuilder()
                .withUids()
                .withCountOverride(1)
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2")
                .build();

        Value value3 = UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid1", "uid3", "uid4")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withUids("uid2")
                .withRemovals("uid1", "uid3", "uid4")
                .build());

        testCombinations(expectation, asList(value1, value2, value3));
    }

    @Test
    public void testDuplicatesRecountedUnderSeenIgnore() {
        // Does a count-only Value, when seen first, prevent de-duplication of named
        // UID lists (done for performance optimization)?
        agg = new GlobalIndexUidAggregator(10);
        Value value1 = UidTestBuilder.newBuilder()
                .withCountOnly(11)
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2")
                .build();

        Value value3 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(15)
                .build());

        // not testing the reversal because its behavior is captured
        // in testDuplicatesRecountedUnderSeenIgnoreReverse
        isForwardOnlyTest = true;
        testCombinations(expectation, asList(value1, value2, value3));
    }

    @Test
    public void testDuplicatesRecountedUnderSeenIgnoreReverse() {
        // Does a count-only Value, when seen last, NOT prevent de-duplication of named
        // UID lists (done for performance optimization)?
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

        // not testing the reversal because its behavior is captured
        // in testDuplicatesRecountedUnderSeenIgnore
        isForwardOnlyTest = true;
        testCombinations(expectation, asList(value1, value2, value3));
    }

    @Test
    public void testFloorOfZero() {
        // Does count match size of UID list even when there are more removal UIDs than UIDs?
        agg = new GlobalIndexUidAggregator(10);
        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid1")
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid1", "uid2", "uid3")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid1", "uid2", "uid3")
                .build());

        testCombinations(expectation, asList(value1, value2));
    }

    @Test
    public void preserveRemovals() {
        // Does count match size of UID list even when there are propagated removals?
        agg = new GlobalIndexUidAggregator(10);
        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2")
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid3", "uid4")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2")
                .withRemovals("uid3", "uid4")
                .build());

        testCombinations(expectation, asList(value1, value2));
    }

    @Test
    public void preserveTwoRemovalLists() {
        // Are removals propagated and the count left at zero?
        agg = new GlobalIndexUidAggregator(10);
        Value value1 = UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid1", "uid2")
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid3", "uid4")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid1", "uid2", "uid3", "uid4")
                .build());

        testCombinations(expectation, asList(value1, value2));
    }

    @Test
    public void testDeduplicates() {
        // Do we avoid temporarily exceeding the (lowered) maximum when re-adding
        // an already included UID?
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
    public void testDeduplicatesVariant() {
        // Same as testDeduplicates but with a non-matching removal UID:
        // Do we avoid temporarily exceeding the (lowered) maximum when re-adding
        // an already included UID?
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
    public void testEvaluatesMaxWithEachAddition() {
        // Does removal after exceeding max simply deduct from the count?
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

        // Reverse ordering is tested in testEvaluatesMaxWithEachAdditionReverse
        isForwardOnlyTest = true;
        testCombinations(expectation, asList(value1, value2, value3));
    }

    @Test
    public void testEvaluatesMaxWithEachAdditionReverse() {
        // Does removal before exceeding max avoid count-only mode?
        agg = new GlobalIndexUidAggregator(2);
        Value value1 = UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid1")
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid3")
                .build();

        Value value3 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withUids("uid2", "uid3")
                .withRemovals("uid1")
                .build());

        // Reverse ordering is tested in testEvaluatesMaxWithEachAddition
        isForwardOnlyTest = true;
        testCombinations(expectation, asList(value1, value2, value3));
    }

    @Test
    public void seenIgnoreAddsTwoValues() {
        // Do additions and removals after a count-only Value simply adjust the count?
        agg = new GlobalIndexUidAggregator(2);
        Value value1 = UidTestBuilder.newBuilder()
                .withCountOnly(3)
                .build();

        // Add two
        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2")
                .build();

        // Net zero
        Value value3 = UidTestBuilder.newBuilder()
                .withUids("uid4")
                .withRemovals("uid2") // happens to match prior value's uid2
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(5)
                .build());

        // The behavior for the reverse ordering is captured
        // in seenIgnoreAddsTwoValuesReverse
        isForwardOnlyTest = true;
        testCombinations(expectation, asList(value1, value2, value3));
    }

    @Test
    public void seenIgnoreAddsTwoValuesReverse() {
        // uid2 from value2 will be removed because of the removal UID from value1
        agg = new GlobalIndexUidAggregator(2);

        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid4")
                .withRemovals("uid2")
                .build();

        // After value1 and value2 are combined:
        // uids=uid1,uid4 (count of 2)
        // removals=uid2
        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2")
                .build();

        // Count-only encountered, so current removal count subtracted from uid count (uid1,uid4 - uid2 = 1)
        // Add three to it.
        // Total expected count is 4.
        Value value3 = UidTestBuilder.newBuilder()
                .withCountOnly(3)
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(4)
                .build());

        // The behavior for the reverse ordering is captured
        // in seenIgnoreAddsTwoValues
        isForwardOnlyTest = true;
        testCombinations(expectation, asList(value1, value2, value3));
    }

    @Test
    public void testAddUidToNegativeCount() {
        // Will negative count-only, combined with named uid, result in zero?
        agg = new GlobalIndexUidAggregator(10);

        Value value1 = UidTestBuilder.newBuilder()
                .withCountOnly(-1)
                .build();

        // Combine -1 (count-only) with a named uid will increment the count
        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid1")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(0)
                .build());

        testCombinations(expectation, asList(value1, value2));
    }

    @Test
    public void testRemoveFromNowDefunctNegativeCountFinal() {
        // With negative count, combined with a removal, result in a zero count for full compactions?
        agg = new GlobalIndexUidAggregator(10);
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

        isFullCompactionOnlyTest = true;
        testCombinations(expectation, asList(value1, value2));
    }

    @Test
    public void testRemoveFromNowDefunctNegativeCountPartial() {
        // With negative count, combined with a removal, result in a negative two count in
        // propagation scenarios?
        agg = new GlobalIndexUidAggregator(10);
        Value value1 = UidTestBuilder.newBuilder()
                .withCountOnly(-1)
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid1")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(-2)
                .build());

        isPartialCompactionOnlyTest = true;
        testCombinations(expectation, asList(value1, value2));
    }

    @Test
    public void testDropKeyWhenCountReachesZero() {
        // With a negative count (due to two removals) be changed to a zero
        // count for full compactions?
        agg = new GlobalIndexUidAggregator(10);
        Value value1 = UidTestBuilder.newBuilder()
                .withCountOnly(1)
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid1", "uid2")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(0)
                .build());

        isFullCompactionOnlyTest = true;
        testCombinations(expectation, asList(value1, value2));
    }

    @Test
    public void testDropKeyWhenCountReachesZeroPartial() {
        // With a negative count (due to two removals) propagate
        // as a negative count for partial compactions?
        agg = new GlobalIndexUidAggregator(10);
        Value value1 = UidTestBuilder.newBuilder()
                .withCountOnly(1)
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid1", "uid2")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(-1)
                .build());

        isPartialCompactionOnlyTest = true;
        testCombinations(expectation, asList(value1, value2));
    }

    @Test
    public void seenIgnoreAddsTwoValuesNoMatch() {
        // Will count-only take into account the available information when
        // flipping to seenIgnore?
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
    public void testHitsMax() {
        // Will count-only status continue after decrementing to no longer
        // exceed maximum?
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
    public void exceedMaxIgnoresCountsUid() {
        // Will a flip to seenIgnore take into account the available
        // information?
        // Note this tests both orderings of values.
        // i.e. count = this.uids.size() - this.uidsToRemove.size();
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
        // Will flipping to seen ignore take into account the available information?
        // Note this tests both orderings of values.
        // i.e. count = this.uids.size() - this.uidsToRemove.size();
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
    public void testRemovalToZero() {
        // Will flipping to seen ignore take into account the available information?
        // Note this tests both orderings of values.
        // i.e. count = this.uids.size() - this.uidsToRemove.size();
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
    public void floorOfZeroVariantFinal() {
        // Will flipping to seen ignore take into account the available information?
        // Will a floor of zero be honored for full compactions?
        // Note this tests both orderings of values.
        // i.e. count = this.uids.size() - this.uidsToRemove.size();
        agg = new GlobalIndexUidAggregator(10);
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

        isFullCompactionOnlyTest = true;
        testCombinations(expectation, asList(value1, value2));
    }

    @Test
    public void floorOfZeroVariantPartial() {
        // Will flipping to seen ignore take into account the available information?
        // Will a negative count propagate for partial compactions?
        // Note this tests both orderings of values.
        // i.e. count = this.uids.size() - this.uidsToRemove.size();
        agg = new GlobalIndexUidAggregator(10);
        Value value1 = UidTestBuilder.newBuilder()
                .withCountOnly(1)
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid1", "uid2")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(-1)
                .withRemovals()
                .build());

        isPartialCompactionOnlyTest = true;
        testCombinations(expectation, asList(value1, value2));
    }

    @Test
    public void countOnlyReduced() {
        // Will flipping to seen ignore take into account the available information?
        // Note this tests both orderings of values.
        // i.e. count = this.uids.size() - this.uidsToRemove.size();
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

    private void testCombinations(Uid.List expectation, List<Value> input) {
        // There should be nothing in the removal UID list after a Full Major Compaction

        Uid.List expectNoRemovals = Uid.List.newBuilder().mergeFrom(expectation).clearREMOVEDUID().build();

        if (!isFullCompactionOnlyTest) {
            verify("Forward, Partial Major", expectation, testAsPartialCompaction(input));
        }

        if (!isPartialCompactionOnlyTest) {
            verify("Forward, Full Major", expectNoRemovals, testAsFullMajorCompaction(input));
        }

        if (!isForwardOnlyTest) {
            // reverse the ordering of the input and try again
            Collections.reverse(input);

            if (!isFullCompactionOnlyTest) {
                verify("Reverse, Partial Major", expectation, testAsPartialCompaction(input));
            }

            if (!isPartialCompactionOnlyTest) {
                verify("Reverse, Full Major", expectNoRemovals, testAsFullMajorCompaction(input));
            }
        }
    }

    private Uid.List testAsPartialCompaction(List<Value> values) {
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
            assertEquals("Invalid test state: expected variable's Uid.List size and getCount differ - " + label, expectation.getCOUNT(), expectation.getUIDList().size());
        }

        for (String expectedRemovalUid: expectation.getREMOVEDUIDList()) {
            assertTrue("Remove UID list missing " + expectedRemovalUid + " - " + label, result.getREMOVEDUIDList().contains(expectedRemovalUid));
        }
        assertEquals("Removed count differs - " + label, expectation.getREMOVEDUIDCount(), result.getREMOVEDUIDCount());
        assertEquals("Removed UID list size differs - " + label, expectation.getREMOVEDUIDList().size(), result.getREMOVEDUIDList().size());
    }
}
