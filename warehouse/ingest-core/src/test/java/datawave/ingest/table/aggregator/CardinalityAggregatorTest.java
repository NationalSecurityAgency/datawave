package datawave.ingest.table.aggregator;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.google.common.collect.Iterators;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CardinalityAggregatorTest {
    
    final PropogatingCombiner agg = new CardinalityAggregator();
    
    @Test
    public void testResetPropagate() {
        agg.reset();
        assertTrue(agg.propogateKey());
        agg.setPropogate(false);
        assertFalse(agg.propogateKey());
        agg.reset();
        assertTrue(agg.propogateKey());
    }
    
    @Test
    public void testEmptyCardinality() throws IOException {
        agg.reset();
        assertTrue(agg.propogateKey());
        Value result = agg.reduce(new Key("key"), Iterators.emptyIterator());
        assertFalse(agg.propogateKey());
        assertEquals(CardinalityAggregator.EMPTY_VALUE, result);
    }
    
    @Test
    public void testSingleCardinality() throws IOException {
        agg.reset();
        assertTrue(agg.propogateKey());
        
        HyperLogLogPlus cardinality = new HyperLogLogPlus(10);
        cardinality.offer("A");
        cardinality.offer("B");
        cardinality.offer("C");
        Value val = new Value(cardinality.getBytes());
        Value result = agg.reduce(new Key("key"), Iterators.singletonIterator(val));
        assertTrue(agg.propogateKey());
        assertNotNull(result);
        assertNotNull(result.get());
        HyperLogLogPlus resultcard = HyperLogLogPlus.Builder.build(result.get());
        assertEquals(3, resultcard.cardinality());
    }
    
    @Test
    public void testMultipleCardinality() throws IOException {
        agg.reset();
        
        HyperLogLogPlus acard = new HyperLogLogPlus(10);
        acard.offer("A");
        acard.offer("B");
        acard.offer("C");
        Value aval = new Value(acard.getBytes());
        
        HyperLogLogPlus bcard = new HyperLogLogPlus(10);
        bcard.offer("C");
        bcard.offer("D");
        bcard.offer("E");
        Value bval = new Value(bcard.getBytes());
        
        List<Value> list = new ArrayList<Value>();
        list.add(aval);
        list.add(bval);
        
        Value result = agg.reduce(new Key("key"), list.iterator());
        assertTrue(agg.propogateKey());
        
        assertNotNull(result);
        assertNotNull(result.get());
        
        HyperLogLogPlus resultcard = HyperLogLogPlus.Builder.build(result.get());
        assertEquals(5, resultcard.cardinality());
    }
    
}
