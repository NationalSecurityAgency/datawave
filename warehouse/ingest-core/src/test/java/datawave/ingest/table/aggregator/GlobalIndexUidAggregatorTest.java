package datawave.ingest.table.aggregator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import datawave.ingest.protobuf.Uid;
import datawave.ingest.protobuf.Uid.List.Builder;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

public class GlobalIndexUidAggregatorTest {
    
    PropogatingCombiner agg = new GlobalIndexUidAggregator();
    
    private Uid.List.Builder createNewUidList() {
        return Uid.List.newBuilder();
    }
    
    @Test
    public void testSingleUid() {
        agg.reset();
        Builder b = createNewUidList();
        b.setCOUNT(1);
        b.setIGNORE(false);
        b.addUID(UUID.randomUUID().toString());
        Uid.List uidList = b.build();
        Value val = new Value(uidList.toByteArray());
        
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
            Builder b = createNewUidList();
            b.setIGNORE(false);
            String uuid = UUID.randomUUID().toString();
            savedUUIDs.add(uuid);
            b.setCOUNT(1);
            b.addUID(uuid);
            Uid.List uidList = b.build();
            Value val = new Value(uidList.toByteArray());
            values.add(val);
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
            Builder b = createNewUidList();
            b.setIGNORE(false);
            String uuid = UUID.randomUUID().toString();
            savedUUIDs.add(uuid);
            b.setCOUNT(1);
            b.addUID(uuid);
            Uid.List uidList = b.build();
            Value val = new Value(uidList.toByteArray());
            values.add(val);
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
            Builder b = createNewUidList();
            b.setIGNORE(false);
            String uuid = UUID.randomUUID().toString();
            b.setCOUNT(1);
            b.addUID(uuid);
            Uid.List uidList = b.build();
            Value val = new Value(uidList.toByteArray());
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
            Builder b = createNewUidList();
            b.setIGNORE(false);
            String uuid = UUID.randomUUID().toString();
            savedUUIDs.add(uuid);
            
            b.setCOUNT(-1);
            b.addREMOVEDUID(uuid);
            Uid.List uidList = b.build();
            Value val = new Value(uidList.toByteArray());
            values.add(val);
        }
        int i = 0;
        
        for (int j = 0; j < 1; j++) {
            for (String uuid : savedUUIDs) {
                Builder b = createNewUidList();
                b.setIGNORE(false);
                if ((i % 2) == 0)
                    b.setCOUNT(1);
                else
                    b.setCOUNT(1);
                b.addUID(uuid);
                Uid.List uidList = b.build();
                Value val = new Value(uidList.toByteArray());
                values.add(val);
                i++;
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
        Uid.List uidList = b.build();
        Collection<Value> values = Lists.newArrayList();
        Value val = new Value(uidList.toByteArray());
        values.add(val);
        b = createNewUidList();
        b.setIGNORE(false);
        b.setCOUNT(1);
        b.addUID(UUID.randomUUID().toString());
        uidList = b.build();
        val = new Value(uidList.toByteArray());
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
    public void testCount() throws Exception {
        agg.reset();
        UUID uuid = UUID.randomUUID();
        // Collect the same UUID five times.
        Collection<Value> values = Lists.newArrayList();
        for (int i = 0; i < 5; i++) {
            Builder b = createNewUidList();
            b.setCOUNT(1);
            b.setIGNORE(false);
            b.addUID(uuid.toString());
            Uid.List uidList = b.build();
            Value val = new Value(uidList.toByteArray());
            values.add(val);
        }
        Value result = agg.reduce(new Key("key"), values.iterator());
        Uid.List resultList = Uid.List.parseFrom(result.get());
        
        assertEquals(5, resultList.getCOUNT());
        assertFalse(resultList.getIGNORE());
        assertEquals(1, resultList.getUIDCount());
        
    }
    
    @Test
    public void testRemoveAndReAddUUID() throws Exception {
        GlobalIndexUidAggregator localAgg = new GlobalIndexUidAggregator();
        IteratorSetting is = new IteratorSetting(19, "test", GlobalIndexUidAggregator.class);
        GlobalIndexUidAggregator.setTimestampsIgnoredOpt(is, false);
        GlobalIndexUidAggregator.setCombineAllColumns(is, true);
        localAgg.validateOptions(is.getOptions());
        
        UUID uuid1 = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();
        
        // Remove UUID2 and then re-add it.
        ArrayList<Value> values = Lists.newArrayList();
        Builder b = createNewUidList();
        b.setIGNORE(false);
        b.setCOUNT(1);
        b.addUID(uuid1.toString());
        values.add(new Value(b.build().toByteArray()));
        
        b = createNewUidList();
        b.setIGNORE(false);
        b.setCOUNT(-1);
        b.addREMOVEDUID(uuid2.toString());
        values.add(new Value(b.build().toByteArray()));
        
        b = createNewUidList();
        b.setIGNORE(false);
        b.setCOUNT(1);
        b.addUID(uuid2.toString());
        values.add(new Value(b.build().toByteArray()));
        
        // Both uuid1 and uuid2 should be in the UID list. Uuid1 should be there because we
        // added and never touched it, and uuid2 should be there because the last thing we did
        // with it was an add (even though there was an older remove for it).
        Collections.reverse(values);
        Value result = localAgg.reduce(new Key("key"), values.iterator());
        Uid.List resultList = Uid.List.parseFrom(result.get());
        assertEquals(1, resultList.getCOUNT());
        assertEquals(2, resultList.getUIDCount());
        assertEquals(2, resultList.getUIDList().size());
        assertEquals(0, resultList.getREMOVEDUIDList().size());
        assertTrue(resultList.getUIDList().contains(uuid1.toString()));
        assertTrue(resultList.getUIDList().contains(uuid2.toString()));
    }
    
    @Test
    public void testRemoveAndReAddUUIDWithPartialMajorCompaction() throws Exception {
        GlobalIndexUidAggregator localAgg = new GlobalIndexUidAggregator();
        IteratorSetting is = new IteratorSetting(19, "test", GlobalIndexUidAggregator.class);
        GlobalIndexUidAggregator.setCombineAllColumns(is, true);
        localAgg.validateOptions(is.getOptions());
        
        localAgg.reset();
        
        UUID uuid1 = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();
        
        // Collect uuids 1 and 2 together in the UID list...
        ArrayList<Value> values = Lists.newArrayList();
        Builder b = createNewUidList();
        b.setIGNORE(false);
        b.setCOUNT(1);
        b.addUID(uuid1.toString());
        values.add(new Value(b.build().toByteArray()));
        
        b = createNewUidList();
        b.setIGNORE(false);
        b.setCOUNT(1);
        b.addUID(uuid2.toString());
        values.add(new Value(b.build().toByteArray()));
        
        Collections.reverse(values);
        Value result = localAgg.reduce(new Key("key"), values.iterator());
        Uid.List resultList = Uid.List.parseFrom(result.get());
        assertEquals(2, resultList.getCOUNT());
        assertEquals(2, resultList.getUIDCount());
        assertEquals(2, resultList.getUIDList().size());
        assertEquals(0, resultList.getREMOVEDUIDCount());
        assertEquals(0, resultList.getREMOVEDUIDList().size());
        assertTrue(resultList.getUIDList().contains(uuid1.toString()));
        assertTrue(resultList.getUIDList().contains(uuid2.toString()));
        
        localAgg.reset();
        values.clear();
        values.add(result);
        
        // Simulate a partial major compaction by taking the previous combined result (the partial major compaction
        // output) and add in a removal for uuid 2.
        b = createNewUidList();
        b.setIGNORE(false);
        b.setCOUNT(-1);
        b.addREMOVEDUID(uuid2.toString());
        values.add(new Value(b.build().toByteArray()));
        
        Collections.reverse(values);
        result = localAgg.reduce(new Key("key"), values.iterator());
        resultList = Uid.List.parseFrom(result.get());
        assertEquals(1, resultList.getCOUNT());
        assertEquals(1, resultList.getUIDCount());
        assertEquals(1, resultList.getUIDList().size());
        assertEquals(1, resultList.getREMOVEDUIDList().size());
        assertEquals(uuid1.toString(), resultList.getUID(0));
        assertEquals(uuid2.toString(), resultList.getREMOVEDUID(0));
        
        localAgg.reset();
        values.clear();
        values.add(result);
        
        // Take the previous combined result (which should have uuid1 in the uid list and uuid2 in the remove list)
        // and add uuid2 back in. The combined result should have uuid1 in the uid list and uuid2 in the remove list.
        b = createNewUidList();
        b.setIGNORE(false);
        b.setCOUNT(1);
        b.addUID(uuid2.toString());
        values.add(new Value(b.build().toByteArray()));
        
        Collections.reverse(values);
        result = localAgg.reduce(new Key("key"), values.iterator());
        resultList = Uid.List.parseFrom(result.get());
        
        assertEquals(2, resultList.getCOUNT());
        assertEquals(2, resultList.getUIDCount());
        assertEquals(2, resultList.getUIDList().size());
        assertTrue(resultList.getUIDList().contains(uuid1.toString()));
        assertTrue(resultList.getUIDList().contains(uuid2.toString()));
        assertEquals(0, resultList.getREMOVEDUIDList().size());
    }
    
    @Test
    public void testAggregateWithZeroCountAndUUIDs() throws Exception {
        GlobalIndexUidAggregator localAgg = new GlobalIndexUidAggregator();
        IteratorSetting is = new IteratorSetting(19, "test", GlobalIndexUidAggregator.class);
        GlobalIndexUidAggregator.setTimestampsIgnoredOpt(is, false);
        GlobalIndexUidAggregator.setCombineAllColumns(is, true);
        localAgg.validateOptions(is.getOptions());
        
        localAgg.reset();
        
        UUID uuid1 = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();
        
        // Add uuid1 and remove uuid2 (which hasn't been added yet) to produce a zero count.
        ArrayList<Value> values = Lists.newArrayList();
        Builder b = createNewUidList();
        b.setIGNORE(false);
        b.setCOUNT(1);
        b.addUID(uuid1.toString());
        values.add(new Value(b.build().toByteArray()));
        
        b = createNewUidList();
        b.setIGNORE(false);
        b.setCOUNT(-1);
        b.addREMOVEDUID(uuid2.toString());
        values.add(new Value(b.build().toByteArray()));
        
        Collections.reverse(values);
        Value result = localAgg.reduce(new Key("key"), values.iterator());
        Uid.List resultList = Uid.List.parseFrom(result.get());
        assertEquals(0, resultList.getCOUNT());
        assertEquals(1, resultList.getUIDCount());
        assertEquals(1, resultList.getUIDList().size());
        assertEquals(1, resultList.getREMOVEDUIDList().size());
        assertTrue(resultList.getUIDList().contains(uuid1.toString()));
        assertTrue(resultList.getREMOVEDUIDList().contains(uuid2.toString()));
        
        localAgg.reset();
        values.clear();
        values.add(result);
        
        // Take the previous result (simulating a partial major compaction) and add uuid2 first (before the removal).
        // We should end up with only uuid1 in the uuid list, and uuid2 in the remove list (if we did a full major
        // compaction, then uuid1 would be gone from the remove list too).
        b = createNewUidList();
        b.setIGNORE(false);
        b.setCOUNT(1);
        b.addREMOVEDUID(uuid2.toString());
        values.add(new Value(b.build().toByteArray()));
        
        Collections.reverse(values);
        result = localAgg.reduce(new Key("key"), values.iterator());
        resultList = Uid.List.parseFrom(result.get());
        assertEquals(1, resultList.getUIDCount());
        assertEquals(1, resultList.getUIDList().size());
        assertEquals(1, resultList.getREMOVEDUIDList().size());
        assertEquals(uuid1.toString(), resultList.getUID(0));
        assertEquals(uuid2.toString(), resultList.getREMOVEDUID(0));
    }
    
    @Test
    public void testAggregateWithPositiveCountAndUUIDs() throws Exception {
        GlobalIndexUidAggregator localAgg = new GlobalIndexUidAggregator();
        IteratorSetting is = new IteratorSetting(19, "test", GlobalIndexUidAggregator.class);
        GlobalIndexUidAggregator.setTimestampsIgnoredOpt(is, false);
        GlobalIndexUidAggregator.setCombineAllColumns(is, true);
        localAgg.validateOptions(is.getOptions());
        
        localAgg.reset();
        
        UUID uuid1 = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();
        UUID uuid3 = UUID.randomUUID();
        
        // Add uuid2 and uuid3 as well as removing uuid1 (which hasn't been added yet).
        ArrayList<Value> values = Lists.newArrayList();
        Builder b = createNewUidList();
        b.setIGNORE(false);
        b.setCOUNT(1);
        b.addUID(uuid2.toString());
        values.add(new Value(b.build().toByteArray()));
        
        b = createNewUidList();
        b.setIGNORE(false);
        b.setCOUNT(1);
        b.addUID(uuid3.toString());
        values.add(new Value(b.build().toByteArray()));
        
        b = createNewUidList();
        b.setIGNORE(false);
        b.setCOUNT(-1);
        b.addREMOVEDUID(uuid1.toString());
        values.add(new Value(b.build().toByteArray()));
        
        Collections.reverse(values);
        Value result = localAgg.reduce(new Key("key"), values.iterator());
        Uid.List resultList = Uid.List.parseFrom(result.get());
        assertEquals(1, resultList.getCOUNT());
        assertEquals(2, resultList.getUIDCount());
        assertEquals(2, resultList.getUIDList().size());
        assertEquals(1, resultList.getREMOVEDUIDList().size());
        assertTrue(resultList.getUIDList().contains(uuid2.toString()));
        assertTrue(resultList.getUIDList().contains(uuid3.toString()));
        assertTrue(resultList.getREMOVEDUIDList().contains(uuid1.toString()));
        
        localAgg.reset();
        values.clear();
        values.add(result);
        
        // Take the previous result (simulating a partial major compaction) and add uuid1 first (before the removal).
        // We should end up with only uuid2 and uuid3 in the uuid list, and uuid1 in the remove list (if we did a full
        // major compaction, then uuid1 would be gone from the remove list too).
        b = createNewUidList();
        b.setIGNORE(false);
        b.setCOUNT(1);
        b.addREMOVEDUID(uuid1.toString());
        values.add(new Value(b.build().toByteArray()));
        
        Collections.reverse(values);
        result = localAgg.reduce(new Key("key"), values.iterator());
        resultList = Uid.List.parseFrom(result.get());
        assertEquals(2, resultList.getUIDCount());
        assertEquals(2, resultList.getUIDList().size());
        assertEquals(1, resultList.getREMOVEDUIDList().size());
        assertTrue(resultList.getUIDList().contains(uuid2.toString()));
        assertTrue(resultList.getUIDList().contains(uuid3.toString()));
        assertTrue(resultList.getREMOVEDUIDList().contains(uuid1.toString()));
    }
}
