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
    
    // For this set of tests, Forward and Reverse refer to the aggregation order of the
    // values. This is not to be confused with the ordering of Accumulo Keys.
    // In this context, Forward means that the values will be aggregated in the same
    // order that is provided to the test. Reverse means that they will be aggregated in
    // the reverse order that is provided to the test.
    private boolean isForwardOnlyTest = false;
    
    // For minor compactions and partial major compactions, Accumulo may not have all of
    // the available Values for a given Key. Additional data may exist in other rfiles.
    // As a result, the Aggregator must preserve (AKA propagate) certain information, such
    // as removal markers, in case that information would impact data in those other rfiles.
    // For scans and full major compactions, on the other hand, Accumulo is guaranteed to
    // have all available Values for a given Key. With this complete view of the data, it
    // is possible for the Aggregator to make final decisions and to thus eliminate the
    // data that would otherwise be preserved, such as removal markers.
    private boolean isFullCompactionOnlyTest = false;
    private boolean isPartialCompactionOnlyTest = false;
    
    @Before
    public void setup() {
        agg.reset();
    }
    
    @Test
    public void twoSingleUids() {
        // Do UID lists get combined across values?
        Value value1 = UidTestBuilder.uidList("uid1");
        Value value2 = UidTestBuilder.uidList("uid2");
        
        // @formatter:off
        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2")
                .build());
        // @formatter:on
        
        testCombinations(expectation, value1, value2);
    }
    
    @Test
    public void removalAcrossValue() {
        // @formatter:off
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
        // @formatter:on
        
        testCombinations(expectation, value1, value2);
    }
    
    @Test
    public void removalAbsent() {
        // Are UID removals maintained in partial compactions, without a match?
        
        // @formatter:off
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
        // @formatter:on
        
        testCombinations(expectation, value1, value2);
    }
    
    @Test
    public void removalsCrissCross() {
        // Do UID removals work across Values in both directions?
        
        // @formatter:off
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
        // @formatter:on
        
        testCombinations(expectation, value1, value2);
    }
    
    @Test
    public void removalCrissCrossAndAbsent() {
        // Does a mixture of a removal-that-hits and a removal-that-doesn't-hit work?
        
        // @formatter:off
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
        // @formatter:on
        
        testCombinations(expectation, value1, value2);
    }
    
    @Test
    public void removeAllAcrossValue() {
        // Do removals of all UIDs in another list work?
        // @formatter:off
        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2", "uid3")
                .withRemovals("uid4", "uid5", "uid6") // all UIDs in other list
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid4", "uid5", "uid6")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2", "uid3")
                .withRemovals("uid4", "uid5", "uid6")
                .build());
        // @formatter:on
        
        testCombinations(expectation, value1, value2);
    }
    
    @Test
    public void removeMultipleAbsent() {
        // Are multiple removals persisted in partial compactions, even without matching?
        
        // @formatter:off
        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2", "uid3")
                .withRemovals("uid7", "uid8", "uid9") // all UIDs absent
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid4", "uid5", "uid6")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2", "uid3", "uid4", "uid5", "uid6")
                .withRemovals("uid7", "uid8", "uid9") // all UIDs absent
                .build());
        // @formatter:on
        
        testCombinations(expectation, value1, value2);
    }
    
    @Test
    public void removeAllAcrossBothValues() {
        // Do multiple removals wipe UIDs across Values and persist on partial compactions?
        
        // @formatter:off
        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2", "uid3")
                .withRemovals("uid4", "uid5", "uid6") // all UIDs in other list
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid4", "uid5", "uid6")
                .withRemovals("uid1", "uid2", "uid3") // all UIDs in other list
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid1", "uid2", "uid3", "uid4", "uid5", "uid6")
                .build());
        // @formatter:on
        
        testCombinations(expectation, value1, value2);
    }
    
    @Test
    public void removeSecondValueUidsPlusAbsent() {
        // Do multiple removals-that-hit and multiple removals-that-don't-hit work like single removals?
        
        // @formatter:off
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
        // @formatter:on
        
        testCombinations(expectation, value1, value2);
    }
    
    @Test
    public void removeMixture() {
        // Mixture of hit and non-hit removals within one Value.
        
        // @formatter:off
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
        // @formatter:on
        
        testCombinations(expectation, value1, value2);
    }
    
    @Test
    public void extraIdInRemovals() {
        // Another variation of a mixture of hit and non-hit removals within one Value.
        
        // @formatter:off
        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2", "uid3")
                .withRemovals("uid4", "uid5", "uid6", "uid7") // all UIDs in other list, one absent
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid4", "uid5", "uid6")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2", "uid3")
                .withRemovals("uid4", "uid5", "uid6", "uid7")
                .build());
        // @formatter:on
        
        testCombinations(expectation, value1, value2);
    }
    
    @Test
    public void matchingAndExtraRemovalIds() {
        // Do extra non-hit-removals persist when mixed with hit-removals?
        
        // @formatter:off
        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2", "uid3")
                .withRemovals("uid4", "uid5", "uid6", "uid7") // all UIDs in other list, one absent
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid4", "uid5", "uid6")
                .withRemovals("uid1", "uid2", "uid3", "uid8") // all UIDs in other list, one absent
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid1", "uid2", "uid3", "uid4", "uid5", "uid6", "uid7", "uid8")
                .build());
        // @formatter:on
        
        testCombinations(expectation, value1, value2);
    }
    
    @Test
    public void loneRemovalId() {
        // Does a single removal work when seen in the second Value?
        
        // @formatter:off
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
        // @formatter:on
        
        testCombinations(expectation, value1, value2);
    }
    
    @Test
    public void loneRemovalExtra() {
        // Does a non-matching removal persist?
        
        // @formatter:off
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
        // @formatter:on
        
        testCombinations(expectation, value1, value2);
    }
    
    @Test
    public void removalAvoidsExceedingMax() {
        // With a lowered maximum, is count-only mode avoided with a matching removal?
        
        // @formatter:off
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
        // @formatter:on
        
        testCombinations(expectation, value1, value2);
    }
    
    @Test
    public void exceedMax() {
        // In exceeding the maximum, is count-only mode reached?
        
        // @formatter:off
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
        // @formatter:on
        
        testCombinations(expectation, value1, value2);
    }
    
    @Test
    public void exceedsMaxTwice() {
        // Do counts add properly in count-only mode?
        
        // @formatter:off
        Value value1 = UidTestBuilder.newBuilder()
                .withCountOnly(3)
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withCountOnly(3)
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(6)
                .build());
        // @formatter:on
        
        testCombinations(expectation, value1, value2);
    }
    
    @Test
    public void doNotGoNegativeForNamedUids() {
        // Are removals persisted in partial compaction mode?
        
        // @formatter:off
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
        // @formatter:on
        
        testCombinations(expectation, value1, value2);
    }
    
    @Test
    public void unexpectedDataHandling() {
        // Does a legacy Value (no UIDs, not count-only, yet a count=1) get effectively ignored?
        
        // @formatter:off
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
        // @formatter:on
        
        testCombinations(expectation, value1, value2, value3);
    }
    
    @Test
    public void duplicatesRecountedUnderSeenIgnore() {
        // Does a count-only Value, when seen first, prevent de-duplication of named
        // UID lists (done for performance optimization)?
        // @formatter:off
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
        // @formatter:on
        
        // not testing the reversal because its behavior is captured
        // in testDuplicatesRecountedUnderSeenIgnoreReverse
        isForwardOnlyTest = true;
        testCombinations(expectation, value1, value2, value3);
    }
    
    @Test
    public void duplicatesRecountedUnderSeenIgnoreReverse() {
        // Does a count-only Value, when seen last, NOT prevent de-duplication of named
        // UID lists (done for performance optimization)?
        
        // @formatter:off
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
        // @formatter:on
        
        // not testing the reversal because its behavior is captured
        // in testDuplicatesRecountedUnderSeenIgnore
        isForwardOnlyTest = true;
        testCombinations(expectation, value1, value2, value3);
    }
    
    @Test
    public void floorOfZeroWhenSeenIgnoreFalse() {
        // Does count match size of UID list even when there are more removal UIDs than UIDs?
        // @formatter:off
        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid1")
                .build();

        Value value2 = UidTestBuilder.newBuilder().withUids()
                .withRemovals("uid1", "uid2", "uid3")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid1", "uid2", "uid3")
                .build());
        // @formatter:on
        
        testCombinations(expectation, value1, value2);
    }
    
    @Test
    public void preserveRemovals() {
        // Does count match size of UID list even when there are propagated removals?
        
        // @formatter:off
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
        // @formatter:on
        
        testCombinations(expectation, value1, value2);
    }
    
    @Test
    public void preserveTwoRemovalLists() {
        // Are removals propagated and the count left at zero?
        
        // @formatter:off
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
        // @formatter:on
        
        testCombinations(expectation, value1, value2);
    }
    
    @Test
    public void shouldDeduplicate() {
        // Do we avoid temporarily exceeding the (lowered) maximum when re-adding
        // an already included UID?
        
        // @formatter:off
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
        // @formatter:on
        
        testCombinations(expectation, value1, value2, value3);
    }
    
    @Test
    public void shouldDeduplicateVariant() {
        // Same as testDeduplicates but with a non-matching removal UID:
        // Do we avoid temporarily exceeding the (lowered) maximum when re-adding
        // an already included UID?
        
        // @formatter:off
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
        // @formatter:on
        
        testCombinations(expectation, value1, value2, value3);
    }
    
    @Test
    public void evaluatesMaxWithEachAddition() {
        // Does removal after exceeding max simply deduct from the count?
        
        // @formatter:off
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
        // @formatter:on
        
        // Reverse ordering is tested in testEvaluatesMaxWithEachAdditionReverse
        isForwardOnlyTest = true;
        testCombinations(expectation, value1, value2, value3);
    }
    
    @Test
    public void evaluatesMaxWithEachAdditionReverse() {
        // Does removal before exceeding max avoid count-only mode?
        
        // @formatter:off
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
        // @formatter:on
        
        // Reverse ordering is tested in testEvaluatesMaxWithEachAddition
        isForwardOnlyTest = true;
        testCombinations(expectation, value1, value2, value3);
    }
    
    @Test
    public void seenIgnoreAddsTwoValues() {
        // Do additions and removals after a count-only Value simply adjust the count?
        
        // @formatter:off
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
        // @formatter:on
        
        // The behavior for the reverse ordering is captured
        // in seenIgnoreAddsTwoValuesReverse
        isForwardOnlyTest = true;
        testCombinations(expectation, value1, value2, value3);
    }
    
    @Test
    public void seenIgnoreAddsTwoValuesReverse() {
        // uid2 from value2 will be removed because of the removal UID from value1
        
        // @formatter:off
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
        // @formatter:on
        
        // The behavior for the reverse ordering is captured
        // in seenIgnoreAddsTwoValues
        isForwardOnlyTest = true;
        testCombinations(expectation, value1, value2, value3);
    }
    
    @Test
    public void addUidToNegativeCount() {
        // Will negative count-only, combined with named uid, result in zero?
        
        // @formatter:off
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
        // @formatter:on
        
        testCombinations(expectation, value1, value2);
    }
    
    @Test
    public void negativeCountSetToZeroAfterFullCompaction() {
        // Will a negative count, combined with a removal, result in a zero count for
        // full compactions?
        
        // @formatter:off
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
        // @formatter:on
        
        isFullCompactionOnlyTest = true;
        testCombinations(expectation, value1, value2);
    }
    
    @Test
    public void negativeCountRemainsAfterPartialCompaction() {
        // Will a negative count, combined with a removal, result in a negative two count in
        // propagation scenarios?
        
        // @formatter:off
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
        // @formatter:on
        
        isPartialCompactionOnlyTest = true;
        testCombinations(expectation, value1, value2);
    }
    
    @Test
    public void anotherNegativeSetToZeroAfterFullCompaction() {
        // Will a negative count (due to two removals) be changed to a zero
        // count for full compactions?
        
        // @formatter:off
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
        // @formatter:on
        
        isFullCompactionOnlyTest = true;
        testCombinations(expectation, value1, value2);
    }
    
    @Test
    public void anotherNegativeRemainsAfterPartialCompaction() {
        // Will a negative count (due to two removals) propagate
        // as a negative count for partial compactions?
        
        // @formatter:off
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
        // @formatter:on
        
        isPartialCompactionOnlyTest = true;
        testCombinations(expectation, value1, value2);
    }
    
    @Test
    public void seenIgnoreAddsTwoValuesNoMatch() {
        // Will count-only take into account the available information when
        // flipping to seenIgnore?
        
        // @formatter:off
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
        // @formatter:on
        
        testCombinations(expectation, value1, value2, value3);
    }
    
    @Test
    public void countOnlyWhenExceedMax() {
        // Will count-only status continue after decrementing to no longer
        // exceed maximum?
        
        // @formatter:off
        agg = new GlobalIndexUidAggregator(2);
        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2")
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid3")
                .build();

        Value value3 = UidTestBuilder.newBuilder().withUids()
                .withRemovals("uid4")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(2)
                .build());
        // @formatter:on
        
        testCombinations(expectation, value1, value2, value3);
    }
    
    @Test
    public void combineCountAndNamedListDeterministic() {
        // Will a flip to seenIgnore take into account the available
        // information?
        // Note this tests both orderings of values (forward and reverse).
        // 1. either start with a count-only and add two uids
        // 2. or start with two named uids and add a count-only
        
        // @formatter:off
        Value value1 = UidTestBuilder.newBuilder()
                .withCountOnly(3)
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid3", "uid4")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(5)
                .build());
        // @formatter:on
        
        testCombinations(expectation, value1, value2);
    }
    
    @Test
    public void exceedMaxAddTwoDropOne() {
        // Will flipping to seen ignore take into account the available information?
        // Note this tests both orderings of values.
        // i.e., count = this.uids.size() - this.uidsToRemove.size();
        
        // @formatter:off
        Value value1 = UidTestBuilder.newBuilder()
                .withCountOnly(3)
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid3", "uid4")
                .build();

        Value value3 = UidTestBuilder.newBuilder()
                .withUids().withRemovals("uid5")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(4)
                .build());
        // @formatter:on
        
        testCombinations(expectation, value1, value2, value3);
    }
    
    @Test
    public void removalToZeroCount() {
        // Will flipping to seen ignore take into account the available information?
        // Note this tests both orderings of values.
        // i.e., count = this.uids.size() - this.uidsToRemove.size();
        
        // @formatter:off
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
        // @formatter:on
        
        testCombinations(expectation, value1, value2);
    }
    
    @Test
    public void countOnlyReduced() {
        // Will flipping to seen ignore take into account the available information?
        // Note this tests both orderings of values.
        // i.e., count = this.uids.size() - this.uidsToRemove.size();
        
        // @formatter:off
        Value value1 = UidTestBuilder.newBuilder()
                .withCountOnly(30)
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid1", "uid2")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(28)
                .build());
        // @formatter:on
        
        testCombinations(expectation, value1, value2);
    }
    
    @Test
    public void maxRemovalsCauseCountOnlyPartialCompact() {
        // Test with a single Uid.List with maximum removals
        agg = new GlobalIndexUidAggregator(2);
        
        // @formatter:off
        Value value1 = UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid1", "uid2") // exceeds the maximum limit
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(-2)
                .build());
        // @formatter:on
        
        isPartialCompactionOnlyTest = true;
        testCombinations(expectation, value1);
    }
    
    @Test
    public void maxRemovalsCauseCountOnlyFullCompact() {
        // Test with a single Uid.List with maximum removals
        agg = new GlobalIndexUidAggregator(2);
        
        // @formatter:off
        Value value1 = UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid1", "uid2") // exceeds the maximum limit
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(0)
                .build());
        // @formatter:on
        
        isFullCompactionOnlyTest = true;
        testCombinations(expectation, value1);
    }
    
    @Test
    public void combineMaxRemovalsWithAdditionPartialCompact() {
        agg = new GlobalIndexUidAggregator(2);
        
        // @formatter:off
        Value value1 = UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid1", "uid2", "uid3", "uid4", "uid5") // exceeds the maximum limit
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid100") // not in value1
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(-4)
                .build());
        // @formatter:on
        
        isPartialCompactionOnlyTest = true;
        testCombinations(expectation, value1, value2);
    }
    
    @Test
    public void combineMaxRemovalsWithAdditionFullCompact() {
        agg = new GlobalIndexUidAggregator(2);
        
        // @formatter:off
        Value value1 = UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid1", "uid2", "uid3", "uid4", "uid5") // exceeds the maximum limit
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid100") // not in value1
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(0)
                .build());
        // @formatter:on
        
        isFullCompactionOnlyTest = true;
        testCombinations(expectation, value1, value2);
    }
    
    @Test
    public void twoRemovalsExceedMaximum() {
        agg = new GlobalIndexUidAggregator(2);
        
        // @formatter:off
        Value value1 = UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid1")
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid2")
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(-2)
                .build());
        // @formatter:on
        
        isPartialCompactionOnlyTest = true;
        testCombinations(expectation, value1, value2);
    }
    
    @Test
    public void netNegativeOneRemains() {
        agg = new GlobalIndexUidAggregator(2);
        
        // @formatter:off
        Value value1 = UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid1", "uid2") // exceeds the maximum limit
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid1") // offsets one of the removals
                .build();

        Value value3 = UidTestBuilder.newBuilder()
                .withUids("uid1") // the same uid as value2
                .build();

        // The removal maximum causes a count-only of -2.  Any UIDs that are added after
        // the aggregator is in count-only mode offset this count, regardless of value.
        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(0)
                .build());
        // @formatter:on
        
        this.isForwardOnlyTest = true;
        testCombinations(expectation, value1, value2, value3);
    }
    
    @Test
    public void netNegativeOneRemainsReverse() {
        agg = new GlobalIndexUidAggregator(2);
        
        // @formatter:off
        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid1") // offsets one of the forthcoming removals
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid1") // the same uid as value1
                .build();

        Value value3 = UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid1", "uid2") // exceeds the maximum limit, matches an addition
                .build();

        // The additions of uid1 in both value1 and value2 are de-duplicated.
        // The two removals force the aggregator into count-only mode, resulting in the previous count
        // of 1 plus the size of the value3 uids (0) minus the size of value3's removals (-2),
        // equaling -1.
        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(-1)
                .build());
        // @formatter:on
        
        this.isForwardOnlyTest = true;
        this.isPartialCompactionOnlyTest = true;
        testCombinations(expectation, value1, value2, value3);
    }
    
    // For Forward, Partial, and Full see the comments by the boolean field declarations
    private void testCombinations(Uid.List expectation, Value... inputValues) {
        List<Value> input = asList(inputValues);
        // There should be nothing in the removal UID list after a Full Major Compaction
        
        // @formatter:off
        Uid.List expectNoRemovals = Uid.List.newBuilder()
                .mergeFrom(expectation)
                .clearREMOVEDUID()
                .build();
        // @formatter:on
        
        if (!isFullCompactionOnlyTest) {
            verify("Forward, Partial Major", expectation, testAsPartialCompaction(input));
        }
        
        if (!isPartialCompactionOnlyTest) {
            verify("Forward, Full Major", expectNoRemovals, testAsFullMajorCompaction(input));
        }
        
        // See the comment by isForwardOnlyTest declaration
        if (!isForwardOnlyTest) {
            // Reverse the ordering of the input and try again
            // See the comment by isForwardOnlyTest declaration
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
        
        for (String expectedUid : expectation.getUIDList()) {
            assertTrue("UID list missing " + expectedUid + " - " + label, result.getUIDList().contains(expectedUid));
        }
        assertEquals("UID count differs - " + label + " " + result.getUIDList(), expectation.getCOUNT(), result.getCOUNT());
        assertEquals("UID list size differs - " + label, expectation.getUIDList().size(), result.getUIDList().size());
        // The count and UID List sizes should match unless seenIgnore = true
        if (!expectation.getIGNORE()) {
            assertEquals("Invalid test state: expected variable's Uid.List size and getCount differ - " + label, expectation.getCOUNT(), expectation
                            .getUIDList().size());
        }
        
        for (String expectedRemovalUid : expectation.getREMOVEDUIDList()) {
            assertTrue("Remove UID list missing " + expectedRemovalUid + " - " + label, result.getREMOVEDUIDList().contains(expectedRemovalUid));
        }
        assertEquals("Removed count differs - " + label, expectation.getREMOVEDUIDCount(), result.getREMOVEDUIDCount());
        assertEquals("Removed UID list size differs - " + label, expectation.getREMOVEDUIDList().size(), result.getREMOVEDUIDList().size());
    }
}
