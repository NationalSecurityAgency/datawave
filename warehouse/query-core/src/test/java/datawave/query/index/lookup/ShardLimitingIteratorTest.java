package datawave.query.index.lookup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

import datawave.ingest.protobuf.Uid;
import datawave.query.util.SortedKeyValueIteratorToIterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class ShardLimitingIteratorTest {
    
    @Test
    public void testExceedMaxSingleDay() throws IOException {
        
        TreeMap<Key,Value> data = Maps.newTreeMap();
        List<String> docIds = Arrays.asList("d1", "d2", "d3", "d4");
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.addAllUID(docIds);
        builder.setCOUNT(docIds.size());
        builder.setIGNORE(false);
        Value hasDocs = new Value(builder.build().toByteArray());
        
        List<String> expectedDocs = Lists.newLinkedList();
        
        for (int i = 1; i < 50; i++) {
            expectedDocs.add("date_" + i);
            data.put(new Key("t", "v", "20130325_" + i + "\u0000A"), hasDocs);
        }
        data.put(new Key("t", "v", "20130325\u0000B"), hasDocs);
        
        CreateUidsIterator itr = new CreateUidsIterator();
        itr.init(new SortedMapIterator(data), null, null);
        Range range = new Range(new Key("t", "v", "20130325"), true, new Key("t", "v", "20130325_\uffff"), false);
        itr.seek(range, Collections.emptySet(), false);
        assertTrue(itr.hasTop());
        
        ShardLimitingIterator iter = new ShardLimitingIterator(new SortedKeyValueIteratorToIterator(itr), 25);
        assertTrue(iter.hasNext());
        
        Key topKey = iter.next().getKey();
        
        assertEquals(new Key("t", "v", "20130325"), topKey);
        
        assertFalse(iter.hasNext());
        
    }
    
    @Test
    public void testExceedMaxMultipleDays() throws IOException {
        
        TreeMap<Key,Value> data = Maps.newTreeMap();
        List<String> docIds = Arrays.asList("d1", "d2", "d3", "d4");
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.addAllUID(docIds);
        builder.setCOUNT(docIds.size());
        builder.setIGNORE(false);
        Value hasDocs = new Value(builder.build().toByteArray());
        
        List<String> expectedDocs = Lists.newLinkedList();
        
        // day one has less than the max
        for (int i = 1; i < 5; i++) {
            expectedDocs.add("date_" + i);
            data.put(new Key("t", "v", "20130325_" + i + "\u0000A"), hasDocs);
        }
        // day two exceeds the max
        for (int i = 1; i < 50; i++) {
            expectedDocs.add("date_" + i);
            data.put(new Key("t", "v", "20130326_" + i + "\u0000A"), hasDocs);
        }
        // different term, okay since we're not crossing row boundaries in tserver...only using
        // sorted set
        for (int i = 1; i < 5; i++) {
            expectedDocs.add("date_" + i);
            data.put(new Key("t", "v", "20130327_" + i + "\u0000A"), hasDocs);
        }
        
        CreateUidsIterator itr = new CreateUidsIterator();
        itr.init(new SortedMapIterator(data), null, null);
        Range range = new Range(new Key("t", "v", "20130320"), true, new Key("t", "v", "20130330_\uffff"), false);
        itr.seek(range, Collections.emptySet(), false);
        
        ShardLimitingIterator iter = new ShardLimitingIterator(new SortedKeyValueIteratorToIterator(itr), 25);
        assertTrue(iter.hasNext());
        
        Key topKey = iter.next().getKey();
        
        assertEquals(new Key("t", "v", "20130325_1"), topKey);
        
        for (int i = 2; i < 5; i++) {
            assertTrue(iter.hasNext());
            
            topKey = iter.next().getKey();
            
            assertEquals(new Key("t", "v", "20130325_" + i), topKey);
            
        }
        
        assertTrue(iter.hasNext());
        
        topKey = iter.next().getKey();
        
        assertEquals(new Key("t", "v", "20130326"), topKey);
        
        assertTrue(iter.hasNext());
        
        topKey = iter.next().getKey();
        
        assertEquals(new Key("t", "v", "20130327_1"), topKey);
        
        for (int i = 2; i < 5; i++) {
            assertTrue(iter.hasNext());
            
            topKey = iter.next().getKey();
            
            assertEquals(new Key("t", "v", "20130327_" + i), topKey);
            
        }
        
        assertFalse(iter.hasNext());
        
    }
}
