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

    // forward and reverse
    // partial and full

    @Test
    public void twoSingleUids() {
        Value value1 = UidTestBuilder.uidList("uid1");
        Value value2 = UidTestBuilder.uidList("uid2");

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2")
                .build());

        testCombinations(asList(value1, value2), expectation);
    }

    @Test
    public void removalAcrossValue() {
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

        testCombinations(asList(value1, value2), expectation);
    }

    @Test
    public void removalAbsent() {
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

        testCombinations(asList(value1, value2), expectation);
    }

    @Test
    public void removalsCrissCross() {
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

        testCombinations(asList(value1, value2), expectation);
    }

    @Test
    public void removalCrissCrossAndAbsent() {
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

        testCombinations(asList(value1, value2), expectation);
    }

    @Test
    public void removeAllAcrossValue() {
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

        testCombinations(asList(value1, value2), expectation);
    }

    @Test
    public void removeMultipleAbsent() {
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

        testCombinations(asList(value1, value2), expectation);
    }

    @Test
    public void removeAllAcrossBothValues() {
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

        testCombinations(asList(value1, value2), expectation);
    }

    @Test
    public void removeAllAcrossValuePlusAbsent() {
        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2", "uid3")
                .withRemovals("uid4", "uid5", "uid6") // all uids in other list
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid4", "uid5", "uid6")
                .withRemovals("uid7", "uid8", "uid9") // all absent
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2", "uid3")
                .withRemovals("uid4", "uid5", "uid6", "uid7", "uid8", "uid9")
                .build());

        testCombinations(asList(value1, value2), expectation);
    }

    @Test
    public void removeMixture() {
        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2", "uid3")
                .withRemovals("uid4", "uid6", "uid7") // two uids in other list, one absent
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid4", "uid5", "uid6")
                .withRemovals("uid8", "uid9") // all absent
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2", "uid3", "uid5")
                .withRemovals("uid4", "uid6", "uid7", "uid8", "uid9")
                .build());

        testCombinations(asList(value1, value2), expectation);
    }

    @Test
    public void extraIdInRemovals() {
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

        testCombinations(asList(value1, value2), expectation);
    }

    @Test
    public void extraIdsInFullRemovals() {
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

        testCombinations(asList(value1, value2), expectation);
    }

    @Test
    public void loneRemovalValue() {
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

        testCombinations(asList(value1, value2), expectation);
    }

    @Test
    public void loneRemovalExtra() {
        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2")
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids()
                .withRemovals("uid3") // removal of one uid in other list
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2")
                .withRemovals("uid3")
                .build());

        testCombinations(asList(value1, value2), expectation);
    }

    @Test
    public void removalAvoidsExceedingMax() {
        agg = new GlobalIndexUidAggregator(2);
        Value value1 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2")
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid3")
                .withRemovals("uid1") // removal of one uid in other list
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withUids("uid2", "uid3")
                .withRemovals("uid1")
                .build());

        testCombinations(asList(value1, value2), expectation);
    }

    @Test
    public void exceedMax() {
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

        testCombinations(asList(value1, value2), expectation);
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

        testCombinations(asList(value1, value2), expectation);
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

        testCombinations(asList(value1, value2, value3), expectation);
    }

    @Test
    public void overMaxTwice() {
        agg = new GlobalIndexUidAggregator(2);
        Value value1 = UidTestBuilder.newBuilder()
                .withCountOnly(3)
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withCountOnly(3)
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(6)
                .build());

        testCombinations(asList(value1, value2), expectation);
    }

    @Test
    public void seenIgnoreAddsTwoValues() {
        agg = new GlobalIndexUidAggregator(2);
        Value value1 = UidTestBuilder.newBuilder()
                .withCountOnly(3)
                .build();

        Value value2 = UidTestBuilder.newBuilder()
                .withUids("uid1", "uid2")
                .build();

        Value value3 = UidTestBuilder.newBuilder()
                .withUids("uid4")
                .withRemovals("uid2") // happens to match prior value's uid2
                .build();

        Uid.List expectation = UidTestBuilder.valueToUidList(UidTestBuilder.newBuilder()
                .withCountOnly(5)
                .build());

        testCombinations(asList(value1, value2, value3), expectation);
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

        testCombinations(asList(value1, value2, value3), expectation);
    }

    // todo - flip params
    private void testCombinations(List<Value> input, Uid.List expectation) {
        // todo - consider copying input to avoid test interference
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
