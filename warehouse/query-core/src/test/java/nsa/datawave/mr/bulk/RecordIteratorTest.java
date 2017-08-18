package nsa.datawave.mr.bulk;

import nsa.datawave.mr.bulk.split.FileRangeSplit;
import nsa.datawave.mr.bulk.split.TabletSplitSplit;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.junit.Assert.*;

public class RecordIteratorTest {
    
    @Test
    public void testProgressDiffRows() throws IOException, InterruptedException {
        TabletSplitSplit splits = new TabletSplitSplit(3);
        
        FileRangeSplit r1 = new FileRangeSplit(new Range(new Key("A123", "cf3123", "cq3"), new Key("A1239999", "cf3123", "cq3")), null, 0, 0, null);
        FileRangeSplit r2 = new FileRangeSplit(new Range(new Key("B12", "cf212", "cq2"), new Key("B129999", "cf212", "cq2")), null, 0, 0, null);
        FileRangeSplit r3 = new FileRangeSplit(new Range(new Key("C1", "cf11", "cq1"), new Key("C19999", "cf11", "cq1")), null, 0, 0, null);
        
        splits.add(r1);
        splits.add(r2);
        splits.add(r3);
        
        final SortedMap<Key,Value> keys = new TreeMap<>();
        Value EMPTY_VALUE = new Value();
        for (int s = 0; s < 3; s++) {
            Range range = ((FileRangeSplit) (splits.get(s))).getRange();
            keys.put(range.getStartKey(), EMPTY_VALUE);
            keys.put(range.getStartKey(), EMPTY_VALUE);
            // now add a set of keys in between
            Key startKey = range.getStartKey();
            for (int i = 0; i < 9999; i++) {
                Key key = new Key(startKey.getRow().toString() + i, startKey.getColumnFamily().toString(), startKey.getColumnQualifier().toString(),
                                startKey.getColumnVisibilityParsed(), startKey.getTimestamp());
                keys.put(key, EMPTY_VALUE);
            }
        }
        
        RecordIterator iterator = new RecordIterator(splits, new Configuration()) {
            @Override
            protected SortedKeyValueIterator<Key,Value> buildTopIterators(SortedKeyValueIterator<Key,Value> topIter, Configuration conf)
                            throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {
                return new SortedMapIterator(keys);
            }
        };
        
        testIterator(iterator);
    }
    
    @Test
    public void testProgressDiffCfs() throws IOException, InterruptedException {
        TabletSplitSplit splits = new TabletSplitSplit(3);
        
        FileRangeSplit r1 = new FileRangeSplit(new Range(new Key("A", "cf3123", "cq3"), new Key("A", "cf3123", "cq3")), null, 0, 0, null);
        FileRangeSplit r2 = new FileRangeSplit(new Range(new Key("A", "cf212", "cq2"), new Key("A", "cf212", "cq2")), null, 0, 0, null);
        FileRangeSplit r3 = new FileRangeSplit(new Range(new Key("A", "cf11", "cq1"), new Key("A", "cf11", "cq1")), null, 0, 0, null);
        
        splits.add(r1);
        splits.add(r2);
        splits.add(r3);
        
        final SortedMap<Key,Value> keys = new TreeMap<>();
        Value EMPTY_VALUE = new Value();
        for (int s = 0; s < 3; s++) {
            Range range = ((FileRangeSplit) (splits.get(s))).getRange();
            keys.put(range.getStartKey(), EMPTY_VALUE);
            keys.put(range.getStartKey(), EMPTY_VALUE);
            // now add a set of keys in between
            Key startKey = range.getStartKey();
            for (int i = 0; i < 9999; i++) {
                Key key = new Key(startKey.getRow().toString(), startKey.getColumnFamily().toString(), startKey.getColumnQualifier().toString() + i,
                                startKey.getColumnVisibilityParsed(), startKey.getTimestamp());
                keys.put(key, EMPTY_VALUE);
            }
        }
        
        RecordIterator iterator = new RecordIterator(splits, new Configuration()) {
            @Override
            protected SortedKeyValueIterator<Key,Value> buildTopIterators(SortedKeyValueIterator<Key,Value> topIter, Configuration conf)
                            throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {
                return new SortedMapIterator(keys);
            }
        };
        
        testIterator(iterator);
    }
    
    @Test
    public void testProgressDiffCqs() throws IOException, InterruptedException {
        TabletSplitSplit splits = new TabletSplitSplit(3);
        
        FileRangeSplit r1 = new FileRangeSplit(new Range(new Key("A", "cf3", "\0\0cq3123"), new Key("A", "cf3", "\0cq3123\0\0\0")), null, 0, 0, null);
        FileRangeSplit r2 = new FileRangeSplit(new Range(new Key("A", "cf3", "\0\0\0\0cq212\0\1"), new Key("A", "cf3",
                        "\0\0\0cq212\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\1")), null, 0, 0, null);
        FileRangeSplit r3 = new FileRangeSplit(new Range(new Key("A", "cf3", "\0\0\0cq11"), new Key("A", "cf3",
                        "\uefff\0\0cq11\0\0\0\0\0\0\uefff\uefff\uefff\uefff\uefff\uefff\uefff\uefff\uefff\uefff\uefff")), null, 0, 0, null);
        
        splits.add(r1);
        splits.add(r2);
        splits.add(r3);
        
        final SortedMap<Key,Value> keys = new TreeMap<>();
        Value EMPTY_VALUE = new Value();
        for (int s = 0; s < 3; s++) {
            Range range = ((FileRangeSplit) (splits.get(s))).getRange();
            keys.put(range.getStartKey(), EMPTY_VALUE);
            keys.put(range.getStartKey(), EMPTY_VALUE);
        }
        
        RecordIterator iterator = new RecordIterator(splits, new Configuration()) {
            @Override
            protected SortedKeyValueIterator<Key,Value> buildTopIterators(SortedKeyValueIterator<Key,Value> topIter, Configuration conf)
                            throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {
                return new SortedMapIterator(keys);
            }
        };
        
        testIterator(iterator);
    }
    
    private void testIterator(RecordIterator iterator) throws IOException {
        long time = 0L;
        float f = 0.0f;
        Key lastKey = null;
        while (iterator.hasTop()) {
            Key key = iterator.getTopKey();
            long start = System.currentTimeMillis();
            float nf = iterator.getProgress();
            time += (System.currentTimeMillis() - start);
            assertTrue(String.valueOf(f) + " -> " + lastKey + " vs " + String.valueOf(nf) + " -> " + key, nf >= f);
            f = nf;
            lastKey = key;
            iterator.next();
        }
        // System.out.println("getProgress: " + time + " for " + keys.size() + " calls";
    }
}
