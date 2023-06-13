package datawave.query.index.lookup;

import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.util.Tuple2;
import org.apache.commons.jexl3.parser.JexlNode;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import static datawave.query.index.lookup.IndexStream.StreamContext;
import static datawave.query.index.lookup.IndexStream.StreamContext.ABSENT;
import static datawave.query.index.lookup.IndexStream.StreamContext.DELAYED_FIELD;
import static datawave.query.index.lookup.IndexStream.StreamContext.EXCEEDED_TERM_THRESHOLD;
import static datawave.query.index.lookup.IndexStream.StreamContext.EXCEEDED_VALUE_THRESHOLD;
import static datawave.query.index.lookup.IndexStream.StreamContext.IGNORED;
import static datawave.query.index.lookup.IndexStream.StreamContext.PRESENT;
import static datawave.query.index.lookup.IndexStream.StreamContext.UNINDEXED;
import static datawave.query.index.lookup.IndexStream.StreamContext.UNKNOWN_FIELD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IndexStreamComparatorTest {
    
    private final String key = "shard";
    private final TreeSet<String> shards = Sets.newTreeSet(Arrays.asList("20220101_1", "20220101_2", "20220101_3", "20220101_4"));
    
    private final IndexStreamComparator comparator = new IndexStreamComparator();
    
    @Test
    public void testIntersectionOfScannerStreams() {
        test(s1(), s2(), s3()); // all present
        test(s1(), s2(), ignored()); // present and ignored
        test(s1(), s2(), unindexed()); // present and unindexed
        test(absent(), s1(), s2()); // present and absent -- shouldn't happen but record expected output
        test(s1(), s2(), delayed()); // present and delayed
        test(s1(), s2(), unknown()); // present and unknown field
        test(s1(), s2(), exceededTerm()); // present and exceeded term threshold
        test(s1(), s2(), exceededValue()); // present and exceeded value threshold
    }
    
    // terms with PRESENT should sort before unions
    @Test
    public void testIntersectionWithNestedUnions() {
        
        // present and union(PRESENT)
        test(s1(), union(s1(), s2()));
        
        // present and union(VARIABLE)
        test(s1(), union(s1(), ignored()));
        test(s1(), union(s1(), unindexed()));
        test(s1(), union(s1(), absent()));
        test(s1(), union(s1(), delayed()));
        test(s1(), union(s1(), unknown()));
        test(s1(), union(s1(), exceededTerm()));
        test(s1(), union(s1(), exceededValue()));
        
        // present and union(DELAYED)
        test(s1(), union(ignored(), ignored()));
        test(s1(), union(unindexed(), unindexed()));
        test(union(absent(), absent()), s1());
        test(s1(), union(delayed(), delayed()));
        test(s1(), union(unknown(), unknown()));
        test(s1(), union(exceededTerm(), exceededTerm()));
        test(s1(), union(exceededValue(), exceededValue()));
        
        // present and union(VARIABLE) and union(DELAYED)
        test(s1(), union(s1(), ignored()), union(ignored(), ignored()));
        test(s1(), union(s1(), unindexed()), union(unindexed(), unindexed()));
        test(union(absent(), absent()), s1(), union(s1(), absent()));
        test(s1(), union(s1(), delayed()), union(delayed(), delayed()));
        test(s1(), union(s1(), unknown()), union(unknown(), unknown()));
        test(s1(), union(s1(), exceededTerm()), union(exceededTerm(), exceededTerm()));
        test(s1(), union(s1(), exceededValue()), union(exceededValue(), exceededValue()));
    }
    
    // unions with a context of PRESENT should sort before VARIABLE
    @Test
    public void testIntersectionOfAllUnions() {
        // union(VARIABLE) and union(DELAYED)
        test(union(s1(), ignored()), union(ignored(), ignored()));
        test(union(s1(), unindexed()), union(unindexed(), unindexed()));
        test(union(absent(), absent()), union(s1(), absent()));
        test(union(s1(), delayed()), union(delayed(), delayed()));
        test(union(s1(), unknown()), union(unknown(), unknown()));
        test(union(s1(), exceededTerm()), union(exceededTerm(), exceededTerm()));
        test(union(s1(), exceededValue()), union(exceededValue(), exceededValue()));
    }
    
    @Test
    public void testIntersectionsWithNestedUnionsWithNestedIntersections() {
        // A && (B || (C && D))
        test(s1(), union(s2(), intersection(s1(), s2())));
        
        // A && (B || (C && D)) && delayed(C)
        test(s1(), union(s2(), intersection(s1(), s2())), delayed());
        
        // A && (B || (C && D)) && (B || (delayed(C) && D))
        test(s1(), union(s2(), intersection(s1(), s2())), union(s2(), intersection(s1(), delayed())));
        
        // A && (B || C) && (C || (D && delayed))
        test(s1(), union(s2(), s3()), union(s1(), intersection(s2(), delayed())));
    }
    
    /**
     * Given a collection of IndexStreams assert that proper order is maintained through a variety of iterations
     * <p>
     * The order of input index streams is the expected order
     *
     * @param streams
     *            a collection of IndexStreams, in expected order
     */
    private void test(BaseIndexStream... streams) {
        TreeMultimap<String,IndexStream> map = TreeMultimap.create(Ordering.natural(), comparator);
        List<BaseIndexStream> inputs = new ArrayList<>(Arrays.asList(streams));
        
        for (int i = 0; i < 10; i++) {
            Collections.shuffle(inputs);
            map.clear();
            map.putAll(key, inputs);
            
            Iterator<IndexStream> iter = map.get(key).iterator();
            
            for (BaseIndexStream stream : streams) {
                assertTrue("input streams had next, but intersection did not", iter.hasNext());
                IndexStream is = iter.next();
                assertEquals("IndexStream class did not match", is.getClass(), stream.getClass());
                assertEquals("IndexStream context did not match", is.context(), stream.context());
            }
            
            assertFalse(iter.hasNext());
        }
    }
    
    private ScannerStream s1() {
        return buildScannerStream("F1", "a", PRESENT);
    }
    
    private ScannerStream s2() {
        return buildScannerStream("F2", "b", PRESENT);
    }
    
    private ScannerStream s3() {
        return buildScannerStream("F3", "c", PRESENT);
    }
    
    private ScannerStream ignored() {
        return buildScannerStream("F3", "c", IGNORED);
    }
    
    private ScannerStream unindexed() {
        return buildScannerStream("F4", "d", UNINDEXED);
    }
    
    private ScannerStream absent() {
        return buildScannerStream("F5", "e", ABSENT);
    }
    
    private ScannerStream delayed() {
        return buildScannerStream("F6", "f", DELAYED_FIELD);
    }
    
    private ScannerStream unknown() {
        return buildScannerStream("F7", "g", UNKNOWN_FIELD);
    }
    
    private ScannerStream exceededTerm() {
        return buildScannerStream("F8", "h", EXCEEDED_TERM_THRESHOLD);
    }
    
    private ScannerStream exceededValue() {
        return buildScannerStream("F9", "i", EXCEEDED_VALUE_THRESHOLD);
    }
    
    private ScannerStream buildScannerStream(String field, String value, StreamContext context) {
        JexlNode node = JexlNodeFactory.buildEQNode(field, value);
        
        List<Tuple2<String,IndexInfo>> elements = new ArrayList<>();
        for (String shard : shards) {
            IndexInfo info = new IndexInfo(-1);
            info.applyNode(node);
            elements.add(new Tuple2<>(shard, info));
        }
        
        // build correct scanner stream type based on context
        switch (context) {
            case PRESENT:
                return ScannerStream.withData(elements.iterator(), node);
            case IGNORED:
                return ScannerStream.ignored(node);
            case UNINDEXED:
                return ScannerStream.unindexed(node);
            case ABSENT:
                return ScannerStream.noData(node);
            case DELAYED_FIELD:
                return ScannerStream.delayedExpression(node);
            case UNKNOWN_FIELD:
                return ScannerStream.unknownField(node);
            case EXCEEDED_TERM_THRESHOLD:
                return ScannerStream.exceededTermThreshold(node);
            case EXCEEDED_VALUE_THRESHOLD:
                return ScannerStream.exceededValueThreshold(elements.iterator(), node);
            default:
                throw new IllegalStateException("unknown context: " + context);
        }
        
    }
    
    private Intersection intersection(BaseIndexStream... streams) {
        return new Intersection(Arrays.asList(streams), new IndexInfo());
    }
    
    private Union union(BaseIndexStream... streams) {
        return new Union(Arrays.asList(streams));
    }
    
}
