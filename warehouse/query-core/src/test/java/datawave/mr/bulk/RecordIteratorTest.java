package datawave.mr.bulk;

import datawave.mr.bulk.split.FileRangeSplit;
import datawave.mr.bulk.split.TabletSplitSplit;
import datawave.security.iterator.ConfigurableVisibilityFilter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.iterators.system.DeletingIterator;
import org.apache.accumulo.core.iterators.system.MultiIterator;
import org.apache.accumulo.core.iterators.system.VisibilityFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.powermock.reflect.Whitebox;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RecordIteratorTest {
    
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    
    @Before
    public void setup() throws Exception {
        File tempDir = temporaryFolder.newFolder();
        System.setProperty("hadoop.home.dir", tempDir.getCanonicalPath());
    }
    
    @Test
    public void testIteratorStackSetup() throws Exception {
        TabletSplitSplit splits = new TabletSplitSplit(3);
        
        FileRangeSplit r1 = new FileRangeSplit(new Range(new Key("A123", "cf3123", "cq3"), new Key("A1239999", "cf3123", "cq3")), null, 0, 0, null);
        FileRangeSplit r2 = new FileRangeSplit(new Range(new Key("B12", "cf212", "cq2"), new Key("B129999", "cf212", "cq2")), null, 0, 0, null);
        FileRangeSplit r3 = new FileRangeSplit(new Range(new Key("C1", "cf11", "cq1"), new Key("C19999", "cf11", "cq1")), null, 0, 0, null);
        
        splits.add(r1);
        splits.add(r2);
        splits.add(r3);
        
        AccumuloConfiguration acuConf = DefaultConfiguration.getInstance();
        Configuration conf = new Configuration();
        conf.set("recorditer.auth.string", "A1,A2,A3");
        
        IteratorSetting cfg1 = new IteratorSetting(1, ConfigurableVisibilityFilter.class);
        cfg1.setName("visibilityFilter1");
        cfg1.addOption(ConfigurableVisibilityFilter.AUTHORIZATIONS_OPT, "B1,B2,B3");
        BulkInputFormat.addIterator(conf, cfg1);
        
        IteratorSetting cfg2 = new IteratorSetting(2, ConfigurableVisibilityFilter.class);
        cfg2.setName("visibilityFilter2");
        cfg2.addOption(ConfigurableVisibilityFilter.AUTHORIZATIONS_OPT, "C1,C2,C3");
        BulkInputFormat.addIterator(conf, cfg2);
        
        RecordIterator recordIterator = new RecordIterator(splits, acuConf, conf);
        
        // Pull out the iterator we configured internally, and make sure the stack is:
        // ConfigurableVisibilityFilter:
        // VisibilityFilter: C1,C2,C3
        // ConfigurableVisibilityFilter:
        // VisibilityFilter: B1,B2,B3
        // VisibilityFilter: A1,A2,A3
        // DeletingIterator
        // MultiIterator
        
        Object source;
        VisibilityFilter vf;
        ConfigurableVisibilityFilter cvf;
        TreeSet<String> actualAuths;
        SortedKeyValueIterator internalIter = Whitebox.getInternalState(recordIterator, SortedKeyValueIterator.class);
        
        // Top of the stack is a ConfigurableVisibilityFilter (which creates a VisibilityFilter as its source) with auths C1,C2,C3
        assertEquals(ConfigurableVisibilityFilter.class, internalIter.getClass());
        cvf = ConfigurableVisibilityFilter.class.cast(internalIter);
        source = Whitebox.getInternalState(cvf, "source");
        assertEquals(VisibilityFilter.class, source.getClass());
        vf = VisibilityFilter.class.cast(source);
        actualAuths = new TreeSet<>(Arrays.asList(Whitebox.getInternalState(vf, Authorizations.class).toString().split(",")));
        assertEquals(new TreeSet<>(Arrays.asList("C1", "C2", "C3")), actualAuths);
        source = Whitebox.getInternalState(vf, "source");
        
        // Next on the stack is a ConfigurableVisibilityFilter (which creates a VisibilityFilter as its source) with auths B1,B2,B3
        assertEquals(ConfigurableVisibilityFilter.class, source.getClass());
        cvf = ConfigurableVisibilityFilter.class.cast(source);
        source = Whitebox.getInternalState(cvf, "source");
        assertEquals(VisibilityFilter.class, source.getClass());
        vf = VisibilityFilter.class.cast(source);
        actualAuths = new TreeSet<>(Arrays.asList(Whitebox.getInternalState(vf, Authorizations.class).toString().split(",")));
        assertEquals(new TreeSet<>(Arrays.asList("B1", "B2", "B3")), actualAuths);
        source = Whitebox.getInternalState(vf, "source");
        
        // The next table iterator should be the standard VisibilityFilter which will have auths A1,A2,A3
        assertEquals(VisibilityFilter.class, source.getClass());
        vf = VisibilityFilter.class.cast(source);
        actualAuths = new TreeSet<>(Arrays.asList(Whitebox.getInternalState(vf, Authorizations.class).toString().split(",")));
        assertEquals(new TreeSet<>(Arrays.asList("A1", "A2", "A3")), actualAuths);
        source = Whitebox.getInternalState(vf, "source");
        
        // The next table iterator should be a DeletingIterator
        assertEquals(DeletingIterator.class, source.getClass());
        DeletingIterator di = DeletingIterator.class.cast(source);
        source = Whitebox.getInternalState(di, "source");
        
        // And finally, at the bottom of the stack is a MultiIterator (which has no source field)
        assertEquals(MultiIterator.class, source.getClass());
        
    }
    
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
