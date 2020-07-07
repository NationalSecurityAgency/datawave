package datawave.ingest.mapreduce.handler.facet;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.clearspring.analytics.stream.frequency.CountMinSketch;
import org.apache.log4j.Logger;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class ApproximateAlgorithmsTest {
    
    private static final Logger log = Logger.getLogger(ApproximateAlgorithmsTest.class);
    
    @Test
    public void simplestHLLMergeTest() throws Exception {
        HyperLogLogPlus hllA = new HyperLogLogPlus.Builder(18, 25).build();
        for (int i = 0; i < 100; i++) {
            hllA.offer("A");
        }
        
        HyperLogLogPlus hllB = new HyperLogLogPlus.Builder(18, 25).build();
        for (int i = 0; i < 100; i++) {
            hllB.offer("A");
        }
        
        HyperLogLogPlus hllAB = new HyperLogLogPlus.Builder(18, 25).build();
        ICardinality hllABp = (HyperLogLogPlus) hllAB.merge(hllA, hllB);
        
        log.debug("A card: " + hllA.cardinality());
        log.debug("B card: " + hllB.cardinality());
        log.debug("AB card: " + hllABp.cardinality());
        assertEquals("Cardinality for a set of a single entry should be 1", 1, hllA.cardinality());
        assertEquals("Cardinality for a set of a single entry should be 1", 1, hllB.cardinality());
        assertEquals("Cardinality for a merged set single entries should be 1", 1, hllABp.cardinality());
        
    }
    
    @Test
    public void simpleHLLMergeTest() throws Exception {
        // Demonstrates that the HLL merge function presents a union of the objects
        // presented to the individual algorithms.
        HyperLogLogPlus hllA = new HyperLogLogPlus.Builder(18, 25).build();
        hllA.offer("1");
        hllA.offer("2");
        hllA.offer("3");
        hllA.offer("4");
        hllA.offer("5");
        
        HyperLogLogPlus hllB = new HyperLogLogPlus.Builder(18, 25).build();
        hllB.offer("2");
        hllB.offer("3");
        hllB.offer("4");
        hllB.offer("5");
        hllB.offer("6");
        
        HyperLogLogPlus hllAB = new HyperLogLogPlus.Builder(18, 25).build();
        ICardinality hllABp = (HyperLogLogPlus) hllAB.merge(hllA, hllB);
        
        log.debug("A card: " + hllA.cardinality());
        log.debug("B card: " + hllB.cardinality());
        log.debug("AB card: " + hllABp.cardinality());
        assertEquals("Cardinality for a set A (1,2,3,4,5) should be 5", 5, hllA.cardinality());
        assertEquals("Cardinality for a set B (2,3,4,5,6) should be 5", 5, hllB.cardinality());
        assertEquals("Cardinality for a merged set (1,2,3,4,5,6) should be 6", 6, hllABp.cardinality());
    }
    
    @Test
    public void simplestCMSMergeTest() throws Exception {
        double confidence = 0.999;
        double epsilon = 0.0001;
        int seed = 1;
        
        CountMinSketch sketchA = new CountMinSketch(epsilon, confidence, seed);
        for (int i = 0; i < 100; i++) {
            sketchA.add("A", 1);
        }
        CountMinSketch sketchB = new CountMinSketch(epsilon, confidence, seed);
        for (int i = 0; i < 100; i++) {
            sketchB.add("B", 1);
        }
        
        CountMinSketch sketchABp = CountMinSketch.merge(sketchA, sketchB);
        
        log.debug("A sketch: " + sketchA.size());
        log.debug("B sketch: " + sketchB.size());
        log.debug("AB sketch: " + sketchABp.size());
        assertEquals("Size of A sketch should be 100", 100, sketchA.size());
        assertEquals("Size of B sketch should be 100", 100, sketchB.size());
        assertEquals("Size of merged sketch should be 200", 200, sketchABp.size());
    }
    
    @Test
    public void simpleCMSMergeTest() throws Exception {
        double confidence = 0.999;
        double epsilon = 0.0001;
        int seed = 1;
        
        CountMinSketch sketchA = new CountMinSketch(epsilon, confidence, seed);
        sketchA.add("1", 1);
        sketchA.add("2", 1);
        sketchA.add("3", 1);
        sketchA.add("4", 1);
        sketchA.add("5", 2);
        
        CountMinSketch sketchB = new CountMinSketch(epsilon, confidence, seed);
        sketchB.add("2", 1);
        sketchB.add("3", 1);
        sketchB.add("4", 1);
        sketchB.add("5", 1);
        sketchB.add("6", 2);
        
        CountMinSketch sketchABp = CountMinSketch.merge(sketchA, sketchB);
        
        log.debug("A sketch: " + sketchA.size());
        log.debug("B sketch: " + sketchB.size());
        log.debug("AB sketch: " + sketchABp.size());
        
        for (int i = 0; i < 7; i++) {
            String v = String.valueOf(i);
            log.debug("v = " + v + " -> " + sketchABp.estimateCount(v));
        }
    }
}
