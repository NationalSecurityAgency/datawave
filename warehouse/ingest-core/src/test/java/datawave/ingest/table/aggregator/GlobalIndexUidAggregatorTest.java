package datawave.ingest.table.aggregator;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import datawave.ingest.protobuf.Uid;
import datawave.ingest.protobuf.Uid.List.Builder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

import static datawave.ingest.table.aggregator.UidTestUtils.countOnlyList;
import static datawave.ingest.table.aggregator.UidTestUtils.legacyRemoveUidList;
import static datawave.ingest.table.aggregator.UidTestUtils.removeUidList;
import static datawave.ingest.table.aggregator.UidTestUtils.uidList;
import static datawave.ingest.table.aggregator.UidTestUtils.valueToUidList;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class GlobalIndexUidAggregatorTest {
    
    PropogatingCombiner agg = new GlobalIndexUidAggregator();
    
    private Builder createNewUidList() {
        return Uid.List.newBuilder();
    }
    
    private Builder createNewUidList(String... uidsToAdd) {
        Builder b = createNewUidList();
        b.setIGNORE(false);
        b.setCOUNT(uidsToAdd.length);
        Arrays.stream(uidsToAdd).forEach(b::addUID);
        return b;
    }
    
    private Builder createNewRemoveUidList(String... uidsToRemove) {
        Builder b = createNewUidList();
        b.setIGNORE(false);
        b.setCOUNT(-uidsToRemove.length);
        Arrays.stream(uidsToRemove).forEach(b::addREMOVEDUID);
        return b;
    }
    
    private Value toValue(Builder uidListBuilder) {
        return new Value(uidListBuilder.build().toByteArray());
    }
    
    @Test
    public void testSingleUid() {
        agg.reset();
        String uuid = UUID.randomUUID().toString();
        Value val = toValue(createNewUidList(uuid));
        
        Value result = agg.reduce(new Key("key"), Iterators.singletonIterator(val));
        assertNotNull(result);
        assertNotNull(result.get());
        assertNotNull(val.get());
        assertEquals(0, val.compareTo(result.get()));
    }
    
    @Test
    public void testLessThanMax() throws Exception {
        agg.reset();
        List<String> savedUUIDs = new ArrayList<>();
        Collection<Value> values = Lists.newArrayList();
        for (int i = 0; i < GlobalIndexUidAggregator.MAX - 1; i++) {
            String uuid = UUID.randomUUID().toString();
            Value val = toValue(createNewUidList(uuid));
            values.add(val);
            savedUUIDs.add(uuid);
        }
        Value result = agg.reduce(new Key("key"), values.iterator());
        Uid.List resultList = Uid.List.parseFrom(result.get());
        assertFalse(resultList.getIGNORE());
        assertEquals(resultList.getUIDCount(), (GlobalIndexUidAggregator.MAX - 1));
        List<String> resultListUUIDs = resultList.getUIDList();
        for (String s : savedUUIDs)
            assertTrue(resultListUUIDs.contains(s));
    }
    
    @Test
    public void testEqualsMax() throws Exception {
        agg.reset();
        List<String> savedUUIDs = new ArrayList<>();
        Collection<Value> values = Lists.newArrayList();
        for (int i = 0; i < GlobalIndexUidAggregator.MAX; i++) {
            String uuid = UUID.randomUUID().toString();
            Value val = toValue(createNewUidList(uuid));
            values.add(val);
            savedUUIDs.add(uuid);
        }
        Value result = agg.reduce(new Key("key"), values.iterator());
        Uid.List resultList = Uid.List.parseFrom(result.get());
        assertNotNull(resultList);
        assertFalse(resultList.getIGNORE());
        assertEquals(resultList.getUIDCount(), (GlobalIndexUidAggregator.MAX));
        List<String> resultListUUIDs = resultList.getUIDList();
        for (String s : savedUUIDs)
            assertTrue(resultListUUIDs.contains(s));
    }
    
    @Test
    public void testMoreThanMax() throws Exception {
        agg.reset();
        Collection<Value> values = Lists.newArrayList();
        for (int i = 0; i < GlobalIndexUidAggregator.MAX + 10; i++) {
            String uuid = UUID.randomUUID().toString();
            Value val = toValue(createNewUidList(uuid));
            values.add(val);
        }
        Value result = agg.reduce(new Key("key"), values.iterator());
        Uid.List resultList = Uid.List.parseFrom(result.get());
        assertTrue(resultList.getIGNORE());
        assertEquals(0, resultList.getUIDCount());
        assertEquals(resultList.getCOUNT(), (GlobalIndexUidAggregator.MAX + 10));
    }
    
    @Test
    public void testManyRemoval() throws Exception {
        agg.reset();
        List<String> savedUUIDs = new ArrayList<>();
        Collection<Value> values = Lists.newArrayList();
        for (int i = 0; i < GlobalIndexUidAggregator.MAX / 2; i++) {
            String uuid = UUID.randomUUID().toString();
            Value val = toValue(createNewRemoveUidList(uuid));
            values.add(val);
            savedUUIDs.add(uuid);
        }
        
        for (int j = 0; j < 1; j++) {
            for (String uuid : savedUUIDs) {
                Value val = toValue(createNewUidList(uuid));
                values.add(val);
            }
        }
        
        Value result = agg.reduce(new Key("key"), values.iterator());
        Uid.List resultList = Uid.List.parseFrom(result.get());
        assertEquals(0, resultList.getUIDCount());
        assertEquals(0, resultList.getCOUNT());
        
    }
    
    @Test
    public void testSeenIgnore() throws Exception {
        Logger.getRootLogger().setLevel(Level.ALL);
        agg.reset();
        Builder b = createNewUidList();
        b.setIGNORE(true);
        b.setCOUNT(0);
        Collection<Value> values = Lists.newArrayList();
        Value val = toValue(b);
        values.add(val);
        String uuid = UUID.randomUUID().toString();
        val = toValue(createNewUidList(uuid));
        values.add(val);
        Value result = agg.reduce(new Key("key"), values.iterator());
        Uid.List resultList = Uid.List.parseFrom(result.get());
        assertTrue(resultList.getIGNORE());
        assertEquals(0, resultList.getUIDCount());
        assertEquals(1, resultList.getCOUNT());
    }
    
    @Test
    public void testInvalidValueType() throws Exception {
        Logger log = Logger.getLogger(GlobalIndexUidAggregator.class);
        Level origLevel = log.getLevel();
        log.setLevel(Level.FATAL);
        Collection<Value> values = Lists.newArrayList();
        agg.reset();
        Value val = new Value(UUID.randomUUID().toString().getBytes());
        values.add(val);
        Value result = agg.reduce(new Key("key"), values.iterator());
        Uid.List resultList = Uid.List.parseFrom(result.get());
        assertFalse(resultList.getIGNORE());
        assertEquals(0, resultList.getUIDCount());
        assertEquals(0, resultList.getCOUNT());
        
        log.setLevel(origLevel);
    }
    
    @Test
    public void testCountUnderMax() throws Exception {
        agg.reset();
        // Collect five random UIDs
        Collection<Value> values = Lists.newArrayList();
        for (int i = 0; i < 5; i++) {
            String uuid = UUID.randomUUID().toString();
            Value val = toValue(createNewUidList(uuid));
            values.add(val);
        }
        Value result = agg.reduce(new Key("key"), values.iterator());
        Uid.List resultList = Uid.List.parseFrom(result.get());
        
        assertEquals(5, resultList.getCOUNT());
        assertFalse(resultList.getIGNORE());
        assertEquals(5, resultList.getUIDCount());
        assertEquals(5, resultList.getUIDList().size());
        
    }
    
    @Test
    public void testCountWithDuplicates() throws Exception {
        agg.reset();
        String uuid = UUID.randomUUID().toString();
        // Collect the same UUID five times.
        Collection<Value> values = Lists.newArrayList();
        for (int i = 0; i < 5; i++) {
            Value val = toValue(createNewUidList(uuid));
            values.add(val);
        }
        Value result = agg.reduce(new Key("key"), values.iterator());
        Uid.List resultList = Uid.List.parseFrom(result.get());
        
        assertEquals(1, resultList.getCOUNT());
        assertFalse(resultList.getIGNORE());
        assertEquals(1, resultList.getUIDCount());
        assertEquals(1, resultList.getUIDList().size());
        assertEquals(uuid, resultList.getUID(0));
        
    }
    
    @Test
    public void testRemoveAndReAddUUID() throws Exception {
        GlobalIndexUidAggregator localAgg = new GlobalIndexUidAggregator();
        IteratorSetting is = new IteratorSetting(19, "test", GlobalIndexUidAggregator.class);
        GlobalIndexUidAggregator.setTimestampsIgnoredOpt(is, false);
        GlobalIndexUidAggregator.setCombineAllColumns(is, true);
        localAgg.validateOptions(is.getOptions());
        
        String uuid1 = UUID.randomUUID().toString();
        String uuid2 = UUID.randomUUID().toString();
        
        // Remove UUID2 and then re-add it.
        ArrayList<Value> values = Lists.newArrayList();
        values.add(toValue(createNewUidList(uuid1)));
        values.add(toValue(createNewRemoveUidList(uuid2)));
        values.add(toValue(createNewUidList(uuid2)));
        
        // Both uuid1 and uuid2 should be in the UID list. Uuid1 should be there because we
        // added and never touched it, and uuid2 should be there because the last thing we did
        // with it was an add (even though there was an older remove for it).
        Collections.reverse(values);
        Value result = localAgg.reduce(new Key("key"), values.iterator());
        Uid.List resultList = Uid.List.parseFrom(result.get());
        assertEquals(2, resultList.getCOUNT());
        assertEquals(2, resultList.getUIDCount());
        assertEquals(2, resultList.getUIDList().size());
        assertEquals(0, resultList.getREMOVEDUIDList().size());
        assertTrue(resultList.getUIDList().contains(uuid1));
        assertTrue(resultList.getUIDList().contains(uuid2));
    }
    
    @Test
    public void testRemoveAndReAddUUIDWithTimestampsIgnored() throws Exception {
        GlobalIndexUidAggregator localAgg = new GlobalIndexUidAggregator();
        
        String uuid1 = UUID.randomUUID().toString();
        String uuid2 = UUID.randomUUID().toString();
        
        // Remove UUID2 and then re-add it.
        ArrayList<Value> values = Lists.newArrayList();
        values.add(toValue(createNewUidList(uuid1)));
        values.add(toValue(createNewRemoveUidList(uuid2)));
        values.add(toValue(createNewUidList(uuid2)));
        
        // uuid1 should be in the UID list and uuid2 should be in the REMOVEDUID list. Uuid1
        // should be there because we added and never touched it. uuid2 should be in the
        // REMOVEDUID list even though the most recent action was an add because when timestamps
        // are ignored, a remove takes precedence over any add.
        Collections.reverse(values);
        Value result = localAgg.reduce(new Key("key"), values.iterator());
        Uid.List resultList = Uid.List.parseFrom(result.get());
        assertEquals(1, resultList.getCOUNT());
        assertEquals(1, resultList.getUIDCount());
        assertEquals(1, resultList.getUIDList().size());
        assertEquals(1, resultList.getREMOVEDUIDList().size());
        assertTrue(resultList.getUIDList().contains(uuid1));
        assertTrue(resultList.getREMOVEDUIDList().contains(uuid2));
    }
    
    @Test
    public void testNegativeCountWithPartialMajorCompaction() throws Exception {
        GlobalIndexUidAggregator localAgg = new GlobalIndexUidAggregator();
        IteratorSetting is = new IteratorSetting(19, "test", GlobalIndexUidAggregator.class);
        GlobalIndexUidAggregator.setTimestampsIgnoredOpt(is, false);
        GlobalIndexUidAggregator.setCombineAllColumns(is, true);
        localAgg.validateOptions(is.getOptions());
        
        localAgg.reset();
        
        String uuid1 = UUID.randomUUID().toString();
        String uuid2 = UUID.randomUUID().toString();
        String uuid3 = UUID.randomUUID().toString();
        String uuid4 = UUID.randomUUID().toString();
        
        // Collect the addition of uuid1 and the removal of uuid 2, 3, and 4 together.
        ArrayList<Value> values = Lists.newArrayList();
        values.add(toValue(createNewUidList(uuid1)));
        values.add(toValue(createNewRemoveUidList(uuid2)));
        values.add(toValue(createNewRemoveUidList(uuid3)));
        values.add(toValue(createNewRemoveUidList(uuid4)));
        
        Collections.reverse(values);
        Value result = localAgg.reduce(new Key("key"), values.iterator());
        Uid.List resultList = Uid.List.parseFrom(result.get());
        assertEquals(1, resultList.getCOUNT());
        assertEquals(1, resultList.getUIDCount());
        assertEquals(1, resultList.getUIDList().size());
        assertEquals(3, resultList.getREMOVEDUIDCount());
        assertEquals(3, resultList.getREMOVEDUIDList().size());
        assertTrue(resultList.getUIDList().contains(uuid1));
        assertTrue(resultList.getREMOVEDUIDList().contains(uuid2));
        assertTrue(resultList.getREMOVEDUIDList().contains(uuid3));
        assertTrue(resultList.getREMOVEDUIDList().contains(uuid4));
        
        localAgg.reset();
        values.clear();
        values.add(result);
        
        // Simulate a partial major compaction by taking the previous combined result (the partial major compaction
        // output) and include the original adds for uuid2-uuid4.
        values.add(toValue(createNewUidList(uuid2)));
        values.add(toValue(createNewUidList(uuid3)));
        values.add(toValue(createNewUidList(uuid4)));
        
        // Don't reverse the collection since we want the adds for uuid2-4 to appear before
        // the partially compacted protocol buffer. When we're considering timestamps, this
        // order matters.
        // Collections.reverse(values);
        result = localAgg.reduce(new Key("key"), values.iterator());
        resultList = Uid.List.parseFrom(result.get());
        assertEquals(1, resultList.getCOUNT());
        assertEquals(1, resultList.getUIDCount());
        assertEquals(1, resultList.getUIDList().size());
        assertEquals(3, resultList.getREMOVEDUIDList().size());
        assertEquals(uuid1, resultList.getUID(0));
        assertTrue(resultList.getREMOVEDUIDList().contains(uuid2));
        assertTrue(resultList.getREMOVEDUIDList().contains(uuid3));
        assertTrue(resultList.getREMOVEDUIDList().contains(uuid4));
    }
    
    @Test
    public void testNegativeCountWithPartialMajorCompactionAndTimestampsIgnored() throws Exception {
        GlobalIndexUidAggregator localAgg = new GlobalIndexUidAggregator();
        
        localAgg.reset();
        
        String uuid1 = UUID.randomUUID().toString();
        String uuid2 = UUID.randomUUID().toString();
        String uuid3 = UUID.randomUUID().toString();
        String uuid4 = UUID.randomUUID().toString();
        
        // Collect the addition of uuid1 and the removal of uuid 2, 3, and 4 together.
        ArrayList<Value> values = Lists.newArrayList();
        values.add(toValue(createNewUidList(uuid1)));
        values.add(toValue(createNewRemoveUidList(uuid2)));
        values.add(toValue(createNewRemoveUidList(uuid3)));
        values.add(toValue(createNewRemoveUidList(uuid4)));
        
        Collections.reverse(values);
        Value result = localAgg.reduce(new Key("key"), values.iterator());
        Uid.List resultList = Uid.List.parseFrom(result.get());
        assertEquals(1, resultList.getCOUNT());
        assertEquals(1, resultList.getUIDCount());
        assertEquals(1, resultList.getUIDList().size());
        assertEquals(3, resultList.getREMOVEDUIDCount());
        assertEquals(3, resultList.getREMOVEDUIDList().size());
        assertTrue(resultList.getUIDList().contains(uuid1));
        assertTrue(resultList.getREMOVEDUIDList().contains(uuid2));
        assertTrue(resultList.getREMOVEDUIDList().contains(uuid3));
        assertTrue(resultList.getREMOVEDUIDList().contains(uuid4));
        
        localAgg.reset();
        values.clear();
        values.add(result);
        
        // Simulate a partial major compaction by taking the previous combined result (the partial major compaction
        // output) and include the original adds for uuid2-4
        values.add(toValue(createNewUidList(uuid2)));
        values.add(toValue(createNewUidList(uuid3)));
        values.add(toValue(createNewUidList(uuid4)));
        
        // Don't reverse the collection so that the adds for uuid2-4 appear after
        // the removals. Even though the adds are last, when we're ignoring timestamps
        // removals take precedence.
        // Collections.reverse(values);
        result = localAgg.reduce(new Key("key"), values.iterator());
        resultList = Uid.List.parseFrom(result.get());
        assertEquals(1, resultList.getCOUNT());
        assertEquals(1, resultList.getUIDCount());
        assertEquals(1, resultList.getUIDList().size());
        assertEquals(3, resultList.getREMOVEDUIDList().size());
        assertEquals(uuid1, resultList.getUID(0));
    }
    
    @Test
    public void testRemoveAndReAddUUIDWithPartialMajorCompaction() throws Exception {
        GlobalIndexUidAggregator localAgg = new GlobalIndexUidAggregator();
        IteratorSetting is = new IteratorSetting(19, "test", GlobalIndexUidAggregator.class);
        GlobalIndexUidAggregator.setTimestampsIgnoredOpt(is, false);
        GlobalIndexUidAggregator.setCombineAllColumns(is, true);
        localAgg.validateOptions(is.getOptions());
        
        localAgg.reset();
        
        String uuid1 = UUID.randomUUID().toString();
        String uuid2 = UUID.randomUUID().toString();
        
        // Collect uuids 1 and 2 together in the UID list...
        ArrayList<Value> values = Lists.newArrayList();
        values.add(toValue(createNewUidList(uuid1)));
        values.add(toValue(createNewUidList(uuid2)));
        
        Collections.reverse(values);
        Value result = localAgg.reduce(new Key("key"), values.iterator());
        Uid.List resultList = Uid.List.parseFrom(result.get());
        assertEquals(2, resultList.getCOUNT());
        assertEquals(2, resultList.getUIDCount());
        assertEquals(2, resultList.getUIDList().size());
        assertEquals(0, resultList.getREMOVEDUIDCount());
        assertEquals(0, resultList.getREMOVEDUIDList().size());
        assertTrue(resultList.getUIDList().contains(uuid1));
        assertTrue(resultList.getUIDList().contains(uuid2));
        
        localAgg.reset();
        values.clear();
        values.add(result);
        
        // Simulate a partial major compaction by taking the previous combined result (the partial major compaction
        // output) and add in a removal for uuid 2.
        values.add(toValue(createNewRemoveUidList(uuid2)));
        
        Collections.reverse(values);
        result = localAgg.reduce(new Key("key"), values.iterator());
        resultList = Uid.List.parseFrom(result.get());
        assertEquals(1, resultList.getCOUNT());
        assertEquals(1, resultList.getUIDCount());
        assertEquals(1, resultList.getUIDList().size());
        assertEquals(1, resultList.getREMOVEDUIDList().size());
        assertEquals(uuid1, resultList.getUID(0));
        assertEquals(uuid2, resultList.getREMOVEDUID(0));
        
        localAgg.reset();
        values.clear();
        values.add(result);
        
        // Take the previous combined result (which should have uuid1 in the uid list and uuid2 in the remove list)
        // and add uuid2 back in. The combined result should have uuid1 in the uid list and uuid2 in the remove list.
        values.add(toValue(createNewUidList(uuid2)));
        
        Collections.reverse(values);
        result = localAgg.reduce(new Key("key"), values.iterator());
        resultList = Uid.List.parseFrom(result.get());
        
        assertEquals(2, resultList.getCOUNT());
        assertEquals(2, resultList.getUIDCount());
        assertEquals(2, resultList.getUIDList().size());
        assertTrue(resultList.getUIDList().contains(uuid1));
        assertTrue(resultList.getUIDList().contains(uuid2));
        assertEquals(0, resultList.getREMOVEDUIDList().size());
    }
    
    @Test
    public void testRemoveAndReAddUUIDWithTimestampsIgnoredAndPartialMajorCompaction() throws Exception {
        agg.reset();
        
        String uuid1 = UUID.randomUUID().toString();
        String uuid2 = UUID.randomUUID().toString();
        
        // Collect uuids 1 and 2 together in the UID list...
        ArrayList<Value> values = Lists.newArrayList();
        values.add(toValue(createNewUidList(uuid1)));
        values.add(toValue(createNewUidList(uuid2)));
        
        Collections.reverse(values);
        Value result = agg.reduce(new Key("key"), values.iterator());
        Uid.List resultList = Uid.List.parseFrom(result.get());
        assertEquals(2, resultList.getCOUNT());
        assertEquals(2, resultList.getUIDCount());
        assertEquals(2, resultList.getUIDList().size());
        assertEquals(0, resultList.getREMOVEDUIDCount());
        assertEquals(0, resultList.getREMOVEDUIDList().size());
        assertTrue(resultList.getUIDList().contains(uuid1));
        assertTrue(resultList.getUIDList().contains(uuid2));
        
        agg.reset();
        values.clear();
        values.add(result);
        
        // Simulate a partial major compaction by taking the previous combined result (the partial major compaction
        // output) and add in a removal for uuid 2.
        values.add(toValue(createNewRemoveUidList(uuid2)));
        
        Collections.reverse(values);
        result = agg.reduce(new Key("key"), values.iterator());
        resultList = Uid.List.parseFrom(result.get());
        assertEquals(1, resultList.getCOUNT());
        assertEquals(1, resultList.getUIDCount());
        assertEquals(1, resultList.getUIDList().size());
        assertEquals(1, resultList.getREMOVEDUIDList().size());
        assertEquals(uuid1, resultList.getUID(0));
        assertEquals(uuid2, resultList.getREMOVEDUID(0));
        
        agg.reset();
        values.clear();
        values.add(result);
        
        // Take the previous combined result (which should have uuid1 in the uid list and uuid2 in the remove list)
        // and add uuid2 back in. The combined result should have uuid1 in the uid list and uuid2 in the remove list.
        values.add(toValue(createNewUidList(uuid2)));
        
        Collections.reverse(values);
        result = agg.reduce(new Key("key"), values.iterator());
        resultList = Uid.List.parseFrom(result.get());
        
        assertEquals(1, resultList.getCOUNT());
        assertEquals(1, resultList.getUIDCount());
        assertEquals(1, resultList.getUIDList().size());
        assertTrue(resultList.getUIDList().contains(uuid1));
        assertEquals(1, resultList.getREMOVEDUIDList().size());
        assertTrue(resultList.getREMOVEDUIDList().contains(uuid2));
    }
    
    @Test
    public void testAggregateWithZeroCountAndUUIDs() throws Exception {
        agg.reset();
        
        String uuid1 = UUID.randomUUID().toString();
        String uuid2 = UUID.randomUUID().toString();
        
        // Add uuid1 and remove uuid2 (which hasn't been added yet) to produce a zero count.
        ArrayList<Value> values = Lists.newArrayList();
        values.add(toValue(createNewUidList(uuid1)));
        values.add(toValue(createNewRemoveUidList(uuid2)));
        
        Collections.reverse(values);
        Value result = agg.reduce(new Key("key"), values.iterator());
        Uid.List resultList = Uid.List.parseFrom(result.get());
        assertEquals(1, resultList.getCOUNT());
        assertEquals(1, resultList.getUIDCount());
        assertEquals(1, resultList.getUIDList().size());
        assertEquals(1, resultList.getREMOVEDUIDList().size());
        assertTrue(resultList.getUIDList().contains(uuid1));
        assertTrue(resultList.getREMOVEDUIDList().contains(uuid2));
        
        agg.reset();
        values.clear();
        values.add(result);
        
        // Take the previous result (simulating a partial major compaction) and add uuid2 first (before the removal).
        // We should end up with only uuid1 in the uuid list, and uuid2 in the remove list (if we did a full major
        // compaction, then uuid1 would be gone from the remove list too).
        values.add(toValue(createNewUidList(uuid2)));
        
        Collections.reverse(values);
        result = agg.reduce(new Key("key"), values.iterator());
        resultList = Uid.List.parseFrom(result.get());
        assertEquals(1, resultList.getUIDCount());
        assertEquals(1, resultList.getUIDList().size());
        assertEquals(1, resultList.getREMOVEDUIDList().size());
        assertEquals(uuid1, resultList.getUID(0));
        assertEquals(uuid2, resultList.getREMOVEDUID(0));
    }
    
    @Test
    public void testAggregateWithPositiveCountAndUUIDs() throws Exception {
        agg.reset();
        
        String uuid1 = UUID.randomUUID().toString();
        String uuid2 = UUID.randomUUID().toString();
        String uuid3 = UUID.randomUUID().toString();
        
        // Add uuid2 and uuid3 as well as removing uuid1 (which hasn't been added yet).
        ArrayList<Value> values = Lists.newArrayList();
        values.add(toValue(createNewUidList(uuid2)));
        values.add(toValue(createNewUidList(uuid3)));
        values.add(toValue(createNewRemoveUidList(uuid1)));
        
        Collections.reverse(values);
        Value result = agg.reduce(new Key("key"), values.iterator());
        Uid.List resultList = Uid.List.parseFrom(result.get());
        assertEquals(2, resultList.getCOUNT());
        assertEquals(2, resultList.getUIDCount());
        assertEquals(2, resultList.getUIDList().size());
        assertEquals(1, resultList.getREMOVEDUIDList().size());
        assertTrue(resultList.getUIDList().contains(uuid2));
        assertTrue(resultList.getUIDList().contains(uuid3));
        assertTrue(resultList.getREMOVEDUIDList().contains(uuid1));
        
        agg.reset();
        values.clear();
        values.add(result);
        
        // Take the previous result (simulating a partial major compaction) and add uuid1 first (before the removal).
        // We should end up with only uuid2 and uuid3 in the uuid list, and uuid1 in the remove list (if we did a full
        // major compaction, then uuid1 would be gone from the remove list too).
        values.add(toValue(createNewRemoveUidList(uuid1)));
        
        Collections.reverse(values);
        result = agg.reduce(new Key("key"), values.iterator());
        resultList = Uid.List.parseFrom(result.get());
        assertEquals(2, resultList.getUIDCount());
        assertEquals(2, resultList.getUIDList().size());
        assertEquals(1, resultList.getREMOVEDUIDList().size());
        assertTrue(resultList.getUIDList().contains(uuid2));
        assertTrue(resultList.getUIDList().contains(uuid3));
        assertTrue(resultList.getREMOVEDUIDList().contains(uuid1));
    }
    
    @Test
    public void testAddUIDTwice() throws Exception {
        agg.reset();
        
        String uuid1 = UUID.randomUUID().toString();
        ArrayList<Value> values = Lists.newArrayList();
        
        values.add(toValue(createNewUidList(uuid1)));
        values.add(toValue(createNewUidList(uuid1)));
        
        Value result = agg.reduce(new Key("key"), values.iterator());
        Uid.List resultList = Uid.List.parseFrom(result.get());
        assertEquals(1, resultList.getUIDCount());
        assertTrue(resultList.getUIDList().contains(uuid1));
        assertEquals(1, resultList.getUIDList().size());
        assertEquals(1, resultList.getCOUNT());
    }
    
    @Test
    public void testAddUIDThrice() throws Exception {
        // lowered max to show problem more easily
        GlobalIndexUidAggregator localAgg = new GlobalIndexUidAggregator(2);
        
        String uuid1 = UUID.randomUUID().toString();
        ArrayList<Value> values = Lists.newArrayList();
        values.add(toValue(createNewUidList(uuid1)));
        values.add(toValue(createNewUidList(uuid1)));
        values.add(toValue(createNewUidList(uuid1)));
        
        Value result = localAgg.reduce(new Key("key"), values.iterator());
        Uid.List resultList = Uid.List.parseFrom(result.get());
        assertEquals(1, resultList.getUIDCount());
        assertTrue(resultList.getUIDList().contains(uuid1));
        assertEquals(1, resultList.getUIDList().size());
        assertEquals(1, resultList.getCOUNT());
    }
    
    @Test
    public void testAddDuplicateUUIDWithSeenIgnore() throws Exception {
        // lowered max to show problem more easily
        GlobalIndexUidAggregator localAgg = new GlobalIndexUidAggregator(2);
        
        String uuid1 = UUID.randomUUID().toString();
        ArrayList<Value> values = Lists.newArrayList();
        
        // Add 3 uuids, which will put the protocol buffer over the max and
        // cause the ignore flag to be set. Then add a duplicate uuid a couple
        // more times. Once we've exceeded the limit, we don't have a list of
        // UIDs anymore to check for duplicates so we simply count incoming
        // UIDs.
        values.add(toValue(createNewUidList(uuid1)));
        values.add(toValue(createNewUidList(UUID.randomUUID().toString())));
        values.add(toValue(createNewUidList(UUID.randomUUID().toString())));
        values.add(toValue(createNewUidList(uuid1)));
        values.add(toValue(createNewUidList(uuid1)));
        
        Value result = localAgg.reduce(new Key("key"), values.iterator());
        Uid.List resultList = Uid.List.parseFrom(result.get());
        assertEquals(0, resultList.getUIDCount());
        assertEquals(0, resultList.getUIDList().size());
        assertEquals(5, resultList.getCOUNT());
    }
    
    @Test
    public void testAddDuplicateUUIDWithSeenIgnoreAndCompaction() throws Exception {
        // lowered max to show problem more easily
        GlobalIndexUidAggregator localAgg = new GlobalIndexUidAggregator(2);
        
        String uuid1 = UUID.randomUUID().toString();
        ArrayList<Value> values = Lists.newArrayList();
        
        // Add 3 uuids, which will put the protocol buffer over the max and
        // cause the ignore flag to be set.
        values.add(toValue(createNewUidList(uuid1)));
        values.add(toValue(createNewUidList(UUID.randomUUID().toString())));
        values.add(toValue(createNewUidList(UUID.randomUUID().toString())));
        
        Value result = localAgg.reduce(new Key("key"), values.iterator());
        Uid.List resultList = Uid.List.parseFrom(result.get());
        assertEquals(0, resultList.getUIDCount());
        assertEquals(0, resultList.getUIDList().size());
        assertEquals(3, resultList.getCOUNT());
        
        // Simulate a compaction
        localAgg.reset();
        values.clear();
        values.add(result);
        
        // Now add uuid1 in a new protocol buffer. Even though it's a duplicate,
        // since we're merging it in to a protocol buffer with the ignore flag
        // set, we'll simply trust the count (since we don't have any individual
        // UIDs to check for duplicates).
        values.add(toValue(createNewUidList(uuid1)));
        
        result = localAgg.reduce(new Key("key"), values.iterator());
        resultList = Uid.List.parseFrom(result.get());
        assertEquals(0, resultList.getUIDCount());
        assertEquals(0, resultList.getUIDList().size());
        assertEquals(4, resultList.getCOUNT());
    }
    
    @Test
    public void testRemoveAndReAdd() throws Exception {
        GlobalIndexUidAggregator localAgg = new GlobalIndexUidAggregator();
        IteratorSetting is = new IteratorSetting(19, "test", GlobalIndexUidAggregator.class);
        GlobalIndexUidAggregator.setTimestampsIgnoredOpt(is, false);
        GlobalIndexUidAggregator.setCombineAllColumns(is, true);
        localAgg.validateOptions(is.getOptions());
        
        localAgg.reset();
        
        String uuid1 = UUID.randomUUID().toString();
        ArrayList<Value> values = Lists.newArrayList();
        
        // When we're considering timestamps, an add of a UID, followed by a removal
        // of that UID, and then a re-add should result in the UID ending up in the
        // UID list.
        values.add(toValue(createNewUidList(uuid1)));
        values.add(toValue(createNewRemoveUidList(uuid1)));
        values.add(toValue(createNewUidList(uuid1)));
        
        Value result = localAgg.reduce(new Key("key"), values.iterator());
        Uid.List resultList = Uid.List.parseFrom(result.get());
        assertEquals(1, resultList.getUIDCount());
        assertEquals(1, resultList.getUIDList().size());
        assertEquals(0, resultList.getREMOVEDUIDCount());
        assertEquals(0, resultList.getREMOVEDUIDList().size());
        assertEquals(1, resultList.getCOUNT());
    }
    
    @Test
    public void testRemoveAndReAddWithTimestampsIgnored() throws Exception {
        agg.reset();
        
        String uuid1 = UUID.randomUUID().toString();
        ArrayList<Value> values = Lists.newArrayList();
        
        // The default behavior of the aggregator is that timestamps are ignored. In that case,
        // a remove takes priority over an add no matter what the order. Therefore, if we
        // add a UID, then remove it, then re-add it, the expected result is that the UID will
        // be removed.
        values.add(toValue(createNewUidList(uuid1)));
        values.add(toValue(createNewRemoveUidList(uuid1)));
        values.add(toValue(createNewUidList(uuid1)));
        
        Value result = agg.reduce(new Key("key"), values.iterator());
        Uid.List resultList = Uid.List.parseFrom(result.get());
        assertEquals(0, resultList.getUIDCount());
        assertEquals(0, resultList.getUIDList().size());
        assertEquals(1, resultList.getREMOVEDUIDCount());
        assertEquals(1, resultList.getREMOVEDUIDList().size());
        assertEquals(0, resultList.getCOUNT());
    }
    
    // Legacy remove UID list is not supported.
    @Test
    public void testLegacyRemoval() {
        List<Value> values = asList(uidList("uid1", "uid2"), legacyRemoveUidList("uid1"));
        Uid.List result = valueToUidList(agg(values));
        
        assertEquals(2, result.getCOUNT());
        assertEquals(2, result.getUIDList().size());
        assertTrue(result.getUIDList().contains("uid1"));
        assertTrue(result.getUIDList().contains("uid2"));
        assertEquals(0, result.getREMOVEDUIDCount());
    }
    
    // Legacy remove UID list is not supported.
    @Test
    public void testCombineLegacyAndNewRemovals() {
        List<Value> values = asList(removeUidList("uid1", "uid2"), legacyRemoveUidList("uid3"));
        Uid.List result = valueToUidList(agg(values));
        
        assertEquals(2, result.getREMOVEDUIDCount());
        assertTrue(result.getREMOVEDUIDList().contains("uid1"));
        assertTrue(result.getREMOVEDUIDList().contains("uid2"));
        assertEquals(1, result.getUIDList().size());
        assertEquals(1, result.getCOUNT());
        assertTrue(result.getUIDList().contains("uid3"));
    }
    
    @Test
    public void testCombineCountAndUidListAndRemoval() {
        List<Value> values = asList(countOnlyList(100), uidList("uid1", "uid2"), removeUidList("uid3", "uid4", "uid5"));
        Uid.List result = valueToUidList(agg(values));
        
        assertEquals(99, result.getCOUNT());
        assertTrue(result.getIGNORE());
        assertTrue(result.getREMOVEDUIDList().isEmpty());
        assertTrue(result.getUIDList().isEmpty());
    }
    
    @Test
    public void testDropKeyWhenCountReachesZero() {
        List<Value> values = asList(countOnlyList(2), removeUidList("uid1", "uid2"));
        agg.setPropogate(false);
        Uid.List result = valueToUidList(agg(values));
        
        assertEquals(0, result.getCOUNT());
        assertTrue(result.getIGNORE());
        assertFalse(agg.propogateKey());
    }
    
    @Test
    public void testKeepKeyWhenCountReachesZeroWhilePropagating() {
        List<Value> values = asList(countOnlyList(2), removeUidList("uid1", "uid2"));
        agg.setPropogate(true);
        Uid.List result = valueToUidList(agg(values));
        
        assertEquals(0, result.getCOUNT());
        assertTrue(result.getIGNORE());
        assertTrue(agg.propogateKey());
    }
    
    @Test
    public void testDropKeyWhenCountReachesZeroWithCount() {
        List<Value> values = asList(countOnlyList(100), countOnlyList(-100));
        agg.setPropogate(false);
        Uid.List result = valueToUidList(agg(values));
        
        assertEquals(0, result.getCOUNT());
        assertFalse(agg.propogateKey());
        assertTrue(result.getIGNORE());
    }
    
    @Test
    public void testKeepKeyWhenCountReachesZeroWithCountWhilePropagating() {
        List<Value> values = asList(countOnlyList(100), countOnlyList(-100));
        agg.setPropogate(true);
        Uid.List result = valueToUidList(agg(values));
        
        assertEquals(0, result.getCOUNT());
        assertTrue(agg.propogateKey());
        assertTrue(result.getIGNORE());
    }
    
    @Test
    public void testPrepareToDropKeyWhenCountGoesNegative() {
        List<Value> values = asList(countOnlyList(1), removeUidList("uid1", "uid2"));
        agg.setPropogate(false);
        Uid.List result = valueToUidList(agg(values));
        
        assertEquals(0, result.getCOUNT());
        assertTrue(result.getIGNORE());
        assertFalse(agg.propogateKey());
    }
    
    @Test
    public void testKeepKeyWhenCountGoesNegativeWhilePropagating() {
        List<Value> values = asList(countOnlyList(1), removeUidList("uid1", "uid2"));
        agg.setPropogate(true);
        Uid.List result = valueToUidList(agg(values));
        
        assertEquals(-1, result.getCOUNT());
        assertTrue(result.getIGNORE());
        assertTrue(agg.propogateKey());
    }
    
    @Test
    public void testWeKeepUidListDuringDoubleRemovals() {
        List<Value> values = asList(uidList("uid1"), removeUidList("uid2"), removeUidList("uid2"));
        agg.setPropogate(false);
        Uid.List result = valueToUidList(agg(values));
        
        assertEquals(1, result.getUIDList().size());
        assertTrue(agg.propogateKey());
    }
    
    private Value agg(List<Value> values) {
        agg.reset();
        return agg.reduce(new Key("row"), values.iterator());
    }
}
