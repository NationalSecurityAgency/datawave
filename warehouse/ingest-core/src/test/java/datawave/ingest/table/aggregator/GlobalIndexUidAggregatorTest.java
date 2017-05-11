package datawave.ingest.table.aggregator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import datawave.ingest.protobuf.Uid;
import datawave.ingest.protobuf.Uid.List.Builder;

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
        assertTrue(val.compareTo(result.get()) == 0);
    }
    
    @Test
    public void testLessThanMax() throws Exception {
        agg.reset();
        List<String> savedUUIDs = new ArrayList<String>();
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
        assertTrue(resultList.getIGNORE() == false);
        assertTrue(resultList.getUIDCount() == (GlobalIndexUidAggregator.MAX - 1));
        List<String> resultListUUIDs = resultList.getUIDList();
        for (String s : savedUUIDs)
            assertTrue(resultListUUIDs.contains(s));
    }
    
    @Test
    public void testEqualsMax() throws Exception {
        agg.reset();
        List<String> savedUUIDs = new ArrayList<String>();
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
        assertFalse(resultList == null);
        assertTrue(resultList.getIGNORE() == false);
        assertTrue(resultList.getUIDCount() == (GlobalIndexUidAggregator.MAX));
        List<String> resultListUUIDs = resultList.getUIDList();
        for (String s : savedUUIDs)
            assertTrue(resultListUUIDs.contains(s));
    }
    
    @Test
    public void testMoreThanMax() throws Exception {
        agg.reset();
        List<String> savedUUIDs = new ArrayList<String>();
        Collection<Value> values = Lists.newArrayList();
        for (int i = 0; i < GlobalIndexUidAggregator.MAX + 10; i++) {
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
        assertTrue(resultList.getIGNORE() == true);
        assertTrue(resultList.getUIDCount() == 0);
        assertTrue(resultList.getCOUNT() == (GlobalIndexUidAggregator.MAX + 10));
    }
    
    @Test
    public void testManyRemoval() throws Exception {
        agg.reset();
        List<String> savedUUIDs = new ArrayList<String>();
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
        assertEquals(resultList.getUIDCount(), 0);
        assertEquals(resultList.getCOUNT(), 0);
        
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
        assertTrue(resultList.getIGNORE() == true);
        assertTrue(resultList.getUIDCount() == 0);
        assertTrue(resultList.getCOUNT() == 1);
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
        assertTrue(resultList.getIGNORE() == false);
        assertTrue(resultList.getUIDCount() == 0);
        assertTrue(resultList.getCOUNT() == 0);
        
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
        
        assertTrue(resultList.getCOUNT() == 5);
        assertTrue(resultList.getIGNORE() == false);
        assertTrue(resultList.getUIDCount() == 1);
        
    }
}
