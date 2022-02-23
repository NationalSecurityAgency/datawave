package datawave.query.discovery;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.ingest.protobuf.Uid;
import datawave.query.iterator.SourceManagerTest;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DiscoveryIteratorTest {
    static final Logger log = Logger.getLogger(DiscoveryIteratorTest.class);
    
    @Test
    public void testHappyPath() throws Throwable {
        Connector con = new InMemoryInstance("DiscoveryIteratorTest").getConnector("root", new PasswordToken(""));
        con.tableOperations().create("index");
        writeSample(con.createBatchWriter("index", new BatchWriterConfig().setMaxLatency(0, TimeUnit.SECONDS).setMaxMemory(0).setMaxWriteThreads(1)));
        Scanner s = con.createScanner("index", new Authorizations("FOO"));
        s.addScanIterator(new IteratorSetting(50, DiscoveryIterator.class));
        s.setRange(new Range());
        
        Iterator<Map.Entry<Key,Value>> itr = s.iterator();
        assertTrue(itr.hasNext());
        Map.Entry<Key,Value> e = itr.next();
        assertFalse(itr.hasNext());
        
        Key key = e.getKey();
        assertEquals("term", key.getRow().toString());
        assertEquals("field", key.getColumnFamily().toString());
        // see DiscoveryIterator for why this has a max unsigned char tacked on the end
        assertEquals("20130101\uffff", key.getColumnQualifier().toString());
        
        Value value = e.getValue();
        assertTrue(value.getSize() > 0);
        
        DataInputBuffer in = new DataInputBuffer();
        in.reset(value.get(), value.getSize());
        ArrayWritable valWrapper = new ArrayWritable(DiscoveredThing.class);
        valWrapper.readFields(in);
        Writable[] values = valWrapper.get();
        assertEquals(3, values.length);
        Set<String> types = Sets.newHashSet("t1", "t2", "t3");
        for (int i = 0; i < 3; ++i) {
            DiscoveredThing thing = (DiscoveredThing) values[i];
            assertEquals("term", thing.getTerm());
            assertEquals("field", thing.getField());
            assertTrue(types.remove(thing.getType()));
            assertEquals("20130101", thing.getDate());
            assertEquals("FOO", thing.getColumnVisibility());
            assertEquals(240L, thing.getCount());
        }
    }
    
    @Test
    public void testReseek() throws Throwable {
        DiscoveryIterator disc = new DiscoveryIterator();
        
        Map<String,String> map = Maps.newHashMap();
        
        TreeMap<Key,Value> ohMap = buildMap("term", "20130101");
        for (int i = 0; i < 25; i++) {
            ohMap.putAll(buildMap("term" + i, "20130101"));
        }
        disc.init(new SortedMapIterator(ohMap), map, null);
        
        disc.seek(new Range(), Collections.emptyList(), false);
        
        assertTrue(disc.hasTop());
        
        Key key = disc.getTopKey();
        assertEquals("term", key.getRow().toString());
        assertEquals("field", key.getColumnFamily().toString());
        // see DiscoveryIterator for why this has a max unsigned char tacked on the end
        assertEquals("20130101\uffff", key.getColumnQualifier().toString());
        
        SortedSet<String> set = new TreeSet<>();
        for (int i = 0; i < 25; i++) {
            set.add("term" + i);
        }
        
        for (String term : set) {
            // reseek on top key
            disc.seek(new Range(key, false, new Key("term99"), false), Collections.emptyList(), false);
            key = disc.getTopKey();
            
            assertEquals(term, key.getRow().toString());
            assertEquals("field", key.getColumnFamily().toString());
            // see DiscoveryIterator for why this has a max unsigned char tacked on the end
            assertEquals("20130101\uffff", key.getColumnQualifier().toString());
        }
    }
    
    @Test
    public void testCountProper() throws Throwable {
        DiscoveryIterator disc = new DiscoveryIterator();
        
        Map<String,String> map = Maps.newHashMap();
        
        TreeMap<Key,Value> ohMap = Maps.newTreeMap();
        
        for (int i = 0; i < 25; i++) {
            Value value = new Value(makeUidList((i % 2) == 0 ? 1 : -1).toByteArray());
            ohMap.put(new Key("term", "field", "20130101_0" + "\u0000t1", "FOO"), value);
        }
        disc.init(new SortedMapIterator(ohMap), map, null);
        
        disc.seek(new Range(), Collections.emptyList(), false);
        
        assertTrue(disc.hasTop());
        
        Key key = disc.getTopKey();
        Value value = disc.getTopValue();
        assertEquals("term", key.getRow().toString());
        assertEquals("field", key.getColumnFamily().toString());
        // see DiscoveryIterator for why this has a max unsigned char tacked on the end
        assertEquals("20130101\uffff", key.getColumnQualifier().toString());
        
        DataInputBuffer in = new DataInputBuffer();
        in.reset(value.get(), value.getSize());
        ArrayWritable valWrapper = new ArrayWritable(DiscoveredThing.class);
        valWrapper.readFields(in);
        Writable[] values = valWrapper.get();
        assertEquals(1, values.length);
        Set<String> types = Sets.newHashSet("t1", "t2", "t3");
        for (int i = 0; i < 1; ++i) {
            DiscoveredThing thing = (DiscoveredThing) values[i];
            assertEquals("term", thing.getTerm());
            assertEquals("field", thing.getField());
            assertTrue(types.remove(thing.getType()));
            assertEquals("20130101", thing.getDate());
            assertEquals("FOO", thing.getColumnVisibility());
            assertEquals(1L, thing.getCount());
        }
    }
    
    @Test
    public void testZeroAndNegative() throws Throwable {
        DiscoveryIterator disc = new DiscoveryIterator();
        
        Map<String,String> map = Maps.newHashMap();
        
        TreeMap<Key,Value> ohMap = Maps.newTreeMap();
        
        for (int i = 0; i < 24; i++) {
            Value value = new Value(makeUidList((i % 2) == 0 ? 1 : -1).toByteArray());
            ohMap.put(new Key("term", "field", "20130101_0" + "\u0000t1", "FOO"), value);
        }
        disc.init(new SortedMapIterator(ohMap), map, null);
        
        disc.seek(new Range(), Collections.emptyList(), false);
        
        assertFalse(disc.hasTop());
        
        disc = new DiscoveryIterator();
        
        map = Maps.newHashMap();
        
        ohMap = Maps.newTreeMap();
        
        for (int i = 0; i < 24; i++) {
            Value value = new Value(makeUidList(-1).toByteArray());
            ohMap.put(new Key("term", "field", "20130101_0" + "\u0000t1", "FOO"), value);
        }
        disc.init(new SortedMapIterator(ohMap), map, null);
        
        disc.seek(new Range(), Collections.emptyList(), false);
        
        assertFalse(disc.hasTop());
    }
    
    @Test
    public void testReverseIndex() throws Throwable {
        Connector con = new InMemoryInstance("DiscoveryIteratorTest").getConnector("root", new PasswordToken(""));
        con.tableOperations().create("reverseIndex");
        writeSample(con.createBatchWriter("reverseIndex", new BatchWriterConfig().setMaxLatency(0, TimeUnit.SECONDS).setMaxMemory(0).setMaxWriteThreads(1)),
                        true);
        Scanner s = con.createScanner("reverseIndex", new Authorizations("FOO"));
        IteratorSetting setting = new IteratorSetting(50, DiscoveryIterator.class);
        setting.addOption(DiscoveryLogic.REVERSE_INDEX, "true");
        s.addScanIterator(setting);
        s.setRange(new Range());
        
        Iterator<Map.Entry<Key,Value>> itr = s.iterator();
        assertTrue(itr.hasNext());
        Map.Entry<Key,Value> e = itr.next();
        assertFalse(itr.hasNext());
        
        Key key = e.getKey();
        assertEquals("mret", key.getRow().toString());
        assertEquals("field", key.getColumnFamily().toString());
        // see DiscoveryIterator for why this has a max unsigned char tacked on the end
        assertEquals("20130101\uffff", key.getColumnQualifier().toString());
        
        Value value = e.getValue();
        assertTrue(value.getSize() > 0);
        
        DataInputBuffer in = new DataInputBuffer();
        in.reset(value.get(), value.getSize());
        ArrayWritable valWrapper = new ArrayWritable(DiscoveredThing.class);
        valWrapper.readFields(in);
        Writable[] values = valWrapper.get();
        assertEquals(3, values.length);
        Set<String> types = Sets.newHashSet("t1", "t2", "t3");
        for (int i = 0; i < 3; ++i) {
            DiscoveredThing thing = (DiscoveredThing) values[i];
            assertEquals("term", thing.getTerm());
            assertEquals("field", thing.getField());
            assertTrue(types.remove(thing.getType()));
            assertEquals("20130101", thing.getDate());
            assertEquals("FOO", thing.getColumnVisibility());
            assertEquals(240L, thing.getCount());
        }
        
    }
    
    void writeSample(BatchWriter writer) throws MutationsRejectedException {
        writeSample(writer, false);
    }
    
    void writeSample(BatchWriter writer, boolean reverse) throws MutationsRejectedException {
        writeSample(writer, (reverse ? "mret" : "term"));
    }
    
    void writeSample(BatchWriter writer, String term) throws MutationsRejectedException {
        int nShards = 10;
        String date = "20130101";
        String[] types = new String[] {"t1", "t2", "t3"};
        Value value = new Value(makeUidList(24).toByteArray());
        Mutation m = new Mutation(term);
        for (String type : types) {
            for (int i = 0; i < nShards; i++) {
                m.put("field", date + "_" + i + "\u0000" + type, new ColumnVisibility("FOO"), value);
            }
        }
        writer.addMutation(m);
    }
    
    TreeMap<Key,Value> buildMap(String term, String date) {
        int nShards = 10;
        TreeMap<Key,Value> map = Maps.newTreeMap();
        String[] types = new String[] {"t1", "t2", "t3"};
        Value value = new Value(makeUidList(24).toByteArray());
        for (String type : types) {
            for (int i = 0; i < nShards; i++) {
                map.put(new Key(term, "field", date + "_" + i + "\u0000" + type, "FOO"), value);
            }
        }
        return map;
    }
    
    Uid.List makeUidList(int count) {
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.setIGNORE(true);
        builder.setCOUNT(count);
        return builder.build();
    }
}
