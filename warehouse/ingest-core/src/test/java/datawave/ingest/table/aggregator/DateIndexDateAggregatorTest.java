package datawave.ingest.table.aggregator;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Test;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class DateIndexDateAggregatorTest {
    
    PropogatingCombiner agg = new DateIndexDateAggregator();
    
    @Test
    public void testSingleShard() {
        agg.reset();
        BitSet bitSet = new BitSet(20);
        bitSet.set(20);
        Value val = new Value(bitSet.toByteArray());
        
        Value result = agg.reduce(new Key("key"), Iterators.singletonIterator(val));
        assertNotNull(result);
        assertNotNull(result.get());
        assertNotNull(val.get());
        assertEquals(0, val.compareTo(result.get()));
    }
    
    @Test
    public void testMerge() throws Exception {
        agg.reset();
        Set<Integer> shards = new HashSet<>();
        for (int i = 0; i <= 20; i++) {
            shards.add(i);
            shards.add(i * 3);
            shards.add(i * 5);
        }
        Collection<Value> values = Lists.newArrayList();
        for (Integer shard : shards) {
            BitSet bitSet = new BitSet(shard);
            bitSet.set(shard);
            Value val = new Value(bitSet.toByteArray());
            values.add(val);
        }
        Value result = agg.reduce(new Key("key"), values.iterator());
        
        BitSet bitSet = BitSet.valueOf(result.get());
        
        for (int i = 0; i <= (20 * 6); i++) {
            if (shards.contains(i)) {
                assertTrue(bitSet.get(i));
            } else {
                assertFalse(bitSet.get(i));
            }
        }
    }
    
    @Test
    public void testMerge2() throws Exception {
        agg.reset();
        Set<Integer> shards = new HashSet<>();
        for (int i = 0; i <= 20; i++) {
            shards.add(i);
            shards.add(i * 5);
            shards.add(i * 7);
        }
        Collection<Value> values = Lists.newArrayList();
        for (Integer shard : shards) {
            BitSet bitSet = new BitSet(shard);
            bitSet.set(shard);
            Value val = new Value(bitSet.toByteArray());
            values.add(val);
        }
        Value result = agg.reduce(new Key("key"), values.iterator());
        
        BitSet bitSet = BitSet.valueOf(result.get());
        
        for (int i = 0; i <= (20 * 8); i++) {
            if (shards.contains(i)) {
                assertTrue(bitSet.get(i));
            } else {
                assertFalse(bitSet.get(i));
            }
        }
    }
    
    @Test
    public void testIncrementalMerge() throws Exception {
        Set<Integer> shards = new HashSet<>();
        for (int i = 0; i <= 20; i++) {
            shards.add(i);
            shards.add(i * 3);
            shards.add(i * 5);
        }
        List<Value> values = Lists.newArrayList();
        for (Integer shard : shards) {
            BitSet bitSet = new BitSet(shard);
            bitSet.set(shard);
            Value val = new Value(bitSet.toByteArray());
            values.add(val);
        }
        
        while (values.size() > 1) {
            List<Value> subValues = new ArrayList<>();
            int len = Math.min(3, values.size());
            for (int i = 0; i < len; i++) {
                subValues.add(values.remove(values.size() - 1));
            }
            agg.reset();
            Value result = agg.reduce(new Key("key"), subValues.iterator());
            values.add(0, result);
        }
        
        BitSet bitSet = BitSet.valueOf(values.get(0).get());
        
        for (int i = 0; i <= (20 * 6); i++) {
            if (shards.contains(i)) {
                assertTrue(bitSet.get(i));
            } else {
                assertFalse(bitSet.get(i));
            }
        }
    }
    
    @Test
    public void testIncrementalMerge2() throws Exception {
        Set<Integer> shards = new HashSet<>();
        for (int i = 0; i <= 20; i++) {
            shards.add(i);
            shards.add(i * 5);
            shards.add(i * 7);
        }
        List<Value> values = Lists.newArrayList();
        for (Integer shard : shards) {
            BitSet bitSet = new BitSet(shard);
            bitSet.set(shard);
            Value val = new Value(bitSet.toByteArray());
            values.add(val);
        }
        
        while (values.size() > 1) {
            List<Value> subValues = new ArrayList<>();
            int len = Math.min(3, values.size());
            for (int i = 0; i < len; i++) {
                subValues.add(values.remove(values.size() - 1));
            }
            agg.reset();
            Value result = agg.reduce(new Key("key"), subValues.iterator());
            values.add(0, result);
        }
        
        BitSet bitSet = BitSet.valueOf(values.get(0).get());
        
        for (int i = 0; i <= (20 * 8); i++) {
            if (shards.contains(i)) {
                assertTrue(bitSet.get(i));
            } else {
                assertFalse(bitSet.get(i));
            }
        }
    }
}
