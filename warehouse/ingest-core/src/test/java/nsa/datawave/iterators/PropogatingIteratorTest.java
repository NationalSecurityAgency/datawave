package nsa.datawave.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.conf.ColumnToClassMapping;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Assert;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.TreeMultimap;
import com.google.protobuf.InvalidProtocolBufferException;

import nsa.datawave.ingest.protobuf.Uid;
import nsa.datawave.ingest.table.aggregator.GlobalIndexUidAggregator;
import nsa.datawave.ingest.table.aggregator.PropogatingCombiner;

/**
 * 
 */
@SuppressWarnings("deprecation")
public class PropogatingIteratorTest {
    
    protected static final boolean[] defaultHasTopResults = new boolean[] {false};
    protected static final Key[] defaultTopKeyResults = new Key[] {null};
    protected static final Value[] defaultTopValueResults = new Value[] {null};
    
    protected static boolean[] propogateKeyResults = PropogatingIteratorTest.defaultHasTopResults;
    
    protected boolean[] hasTopResults = PropogatingIteratorTest.defaultHasTopResults;
    protected Key[] topKeyResults = PropogatingIteratorTest.defaultTopKeyResults;
    protected Value[] topValueResults = PropogatingIteratorTest.defaultTopValueResults;
    protected IteratorScope getIteratorScopeResults = IteratorScope.majc;
    protected boolean isFullMajorCompactionResults = false;
    
    protected static byte[] getExpectedValue(String... uids) {
        Uid.List.Builder b = Uid.List.newBuilder();
        b.setCOUNT(uids.length);
        b.setIGNORE(false);
        for (String uid : uids) {
            b.addUID(uid);
        }
        return b.build().toByteArray();
    }
    
    public static class WrappedPropogatingAggregator extends GlobalIndexUidAggregator {
        
        protected int callCount = 0;
        
        public WrappedPropogatingAggregator() {}
        
        @Override
        public boolean propogateKey() {
            // TODO Auto-generated method stub
            return propogateKeyResults[callCount++];
        }
        
    }
    
    private static class PrivateWrappedAggregator extends PropogatingCombiner {
        
        @Override
        public void reset() {
            // TODO Auto-generated method stub
            
        }
        
        @Override
        public void collect(Value value) {
            // TODO Auto-generated method stub
            
        }
        
        @Override
        public Value aggregate() {
            // TODO Auto-generated method stub
            return null;
        }
        
    }
    
    public static class WrappedPropogatingIterator extends PropogatingIterator {
        
        public WrappedPropogatingIterator() {
            super();
        }
        
        public WrappedPropogatingIterator(PropogatingIterator clone) {
            
            this.env = clone.env;
            this.defaultAgg = clone.defaultAgg;
        }
        
        public PropogatingCombiner getPropAgg() {
            
            return defaultAgg;
        }
        
        public void setPropAgg(PropogatingCombiner pa) {
            
            defaultAgg = pa;
        }
        
        public IteratorEnvironment getIteratorEnvironment() {
            
            return env;
        }
        
    }
    
    /**
     * @param topValue
     * @param string
     * @param string2
     * @param string3
     * @throws InvalidProtocolBufferException
     */
    private void validateUids(Value topValue, String... uids) throws InvalidProtocolBufferException {
        Uid.List v = Uid.List.parseFrom(topValue.get());
        
        Assert.assertEquals(uids.length, v.getCOUNT());
        for (String uid : uids) {
            v.getUIDList().contains(uid);
        }
        
    }
    
    /**
     * @param topValue
     * @param string
     * @param string2
     * @param string3
     * @throws InvalidProtocolBufferException
     */
    private void validateRemoval(Value topValue, String... uids) throws InvalidProtocolBufferException {
        Uid.List v = Uid.List.parseFrom(topValue.get());
        
        Assert.assertEquals(-uids.length, v.getCOUNT());
        for (String uid : uids) {
            v.getREMOVEDUIDList().contains(uid);
        }
        
    }
    
    public class MockIteratorEnvironment implements IteratorEnvironment {
        
        AccumuloConfiguration conf;
        private boolean major;
        
        public MockIteratorEnvironment(AccumuloConfiguration conf) {
            this.conf = conf;
        }
        
        public MockIteratorEnvironment(boolean major) {
            this.conf = AccumuloConfiguration.getDefaultConfiguration();
            this.major = major;
        }
        
        @Override
        public AccumuloConfiguration getConfig() {
            return conf;
        }
        
        @Override
        public IteratorScope getIteratorScope() {
            if (major) {
                return IteratorScope.majc;
            } else
                return IteratorScope.scan;
        }
        
        @Override
        public boolean isFullMajorCompaction() {
            return major;
        }
        
        @Override
        public void registerSideChannel(SortedKeyValueIterator<Key,Value> iter) {
            throw new UnsupportedOperationException();
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see org.apache.accumulo.core.iterators.IteratorEnvironment# reserveMapFileReader(java.lang.String)
         */
        @Override
        public SortedKeyValueIterator<Key,Value> reserveMapFileReader(String arg0) throws IOException {
            // TODO Auto-generated method stub
            return null;
        }
        
    }
    
    protected IteratorEnvironment createMockIteratorEnvironment(boolean activateIteratorScope, boolean activateIsFullCompaction) {
        
        return createMockIteratorEnvironment(activateIteratorScope, (activateIteratorScope ? 1 : 0), activateIsFullCompaction, (activateIsFullCompaction ? 1
                        : 0));
    }
    
    protected IteratorEnvironment createMockIteratorEnvironment(boolean activateIteratorScope, int iteratorScopeCallCount, boolean activateIsFullCompaction,
                    int compactionCallCount) {
        IteratorEnvironment mock = PowerMock.createMock(IteratorEnvironment.class);
        EasyMock.expect(mock.getConfig()).andReturn(null);
        if (activateIteratorScope) {
            
            mock.getIteratorScope();
            EasyMock.expectLastCall().andAnswer(new IAnswer<Object>() {
                
                @Override
                public Object answer() throws Throwable {
                    
                    return getIteratorScopeResults;
                }
            }).times(iteratorScopeCallCount);
        }
        
        if (activateIsFullCompaction) {
            
            mock.isFullMajorCompaction();
            EasyMock.expectLastCall().andAnswer(new IAnswer<Object>() {
                
                @Override
                public Object answer() throws Throwable {
                    
                    return isFullMajorCompactionResults;
                }
            }).times(compactionCallCount);
        }
        
        PowerMock.replay(mock);
        
        return mock;
    }
    
    protected SortedKeyValueIterator<Key,Value> createMockSortedKeyValueIterator() {
        
        return createMockSortedKeyValueIterator(PropogatingIteratorTest.defaultHasTopResults, PropogatingIteratorTest.defaultTopKeyResults,
                        PropogatingIteratorTest.defaultTopValueResults, new AtomicInteger(0));
    }
    
    protected SortedKeyValueIterator<Key,Value> createMockSortedKeyValueIterator(boolean[] htr, Key[] gtk, Value[] gtv, final AtomicInteger callCounter) {
        
        callCounter.set(0);
        hasTopResults = new boolean[htr.length];
        System.arraycopy(htr, 0, hasTopResults, 0, htr.length);
        topKeyResults = new Key[gtk.length];
        System.arraycopy(gtk, 0, topKeyResults, 0, gtk.length);
        topValueResults = new Value[gtv.length];
        System.arraycopy(gtv, 0, topValueResults, 0, gtv.length);
        
        SortedKeyValueIterator<Key,Value> mock = new SortedKeyValueIterator<Key,Value>() {
            
            @Override
            public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
                // TODO Auto-generated method stub
                
            }
            
            @Override
            public boolean hasTop() {
                
                return hasTopResults[callCounter.get()];
            }
            
            @Override
            public void next() throws IOException {
                
                callCounter.incrementAndGet();
            }
            
            @Override
            public Key getTopKey() {
                
                return topKeyResults[callCounter.get()];
            }
            
            @Override
            public Value getTopValue() {
                
                return topValueResults[callCounter.get()];
            }
            
            @Override
            public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
                // TODO Auto-generated method stub
                return this;
            }
            
            @Override
            public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
                // TODO Auto-generated method stub
                
            }
        };
        
        return mock;
    }
    
    long ts = 1349541830;
    
    private Uid.List.Builder createNewUidList(String uid) {
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.setIGNORE(false);
        builder.setCOUNT(1);
        builder.addUID(uid);
        
        return builder;
    }
    
    private Uid.List.Builder createremove(String uid) {
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.setIGNORE(false);
        builder.setCOUNT(-1);
        builder.addREMOVEDUID(uid);
        
        return builder;
    }
    
    protected Key newKey(String row, String field, String uid, boolean delete) {
        Key key = new Key(uid, field, "dataType\0" + row, new ColumnVisibility("PUBLIC"), ts);
        key.setDeleted(delete);
        return key;
        
    }
    
    protected Key newKey(String row, String field, String uid) {
        return newKey(row, field, uid, false);
    }
    
    @Test
    public void testAggregateThree() throws IOException {
        TreeMultimap<Key,Value> map = TreeMultimap.create();
        
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.1").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.2").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.3").build().toByteArray()));
        
        map.put(newKey("20121002_1", "UUID", "abd"), new Value(createNewUidList("abc.3").build().toByteArray()));
        
        SortedMultiMapIterator data = new SortedMultiMapIterator(map);
        
        PropogatingIterator iter = new PropogatingIterator();
        Map<String,String> options = Maps.newHashMap();
        
        options.put(PropogatingIterator.AGGREGATOR_DEFAULT, GlobalIndexUidAggregator.class.getCanonicalName());
        
        IteratorEnvironment env = new MockIteratorEnvironment(false);
        
        iter.init(data, options, env);
        
        iter.seek(new Range(), Collections.<ByteSequence> emptyList(), false);
        
        Assert.assertTrue(iter.hasTop());
        
        Key topKey = iter.getTopKey();
        
        Assert.assertEquals(newKey("20121002_1", "UUID", "abc"), topKey);
        validateUids(iter.getTopValue(), "abc.1", "abc.2", "abc.3");
        iter.next();
        topKey = iter.getTopKey();
        Assert.assertEquals(newKey("20121002_1", "UUID", "abd"), topKey);
        
    }
    
    @Test
    public void testAggregateFour() throws IOException {
        TreeMultimap<Key,Value> map = TreeMultimap.create();
        
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.1").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.2").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.3").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.4").build().toByteArray()));
        
        map.put(newKey("20121002_1", "UUID", "abd"), new Value(createNewUidList("abc.3").build().toByteArray()));
        
        SortedMultiMapIterator data = new SortedMultiMapIterator(map);
        
        PropogatingIterator iter = new PropogatingIterator();
        Map<String,String> options = Maps.newHashMap();
        
        options.put(PropogatingIterator.AGGREGATOR_DEFAULT, GlobalIndexUidAggregator.class.getCanonicalName());
        
        IteratorEnvironment env = new MockIteratorEnvironment(false);
        
        iter.init(data, options, env);
        
        iter.seek(new Range(), Collections.<ByteSequence> emptyList(), false);
        
        Assert.assertTrue(iter.hasTop());
        
        Key topKey = iter.getTopKey();
        
        Assert.assertEquals(newKey("20121002_1", "UUID", "abc"), topKey);
        validateUids(iter.getTopValue(), "abc.1", "abc.2", "abc.3", "abc.4");
        iter.next();
        topKey = iter.getTopKey();
        Assert.assertEquals(newKey("20121002_1", "UUID", "abd"), topKey);
        validateUids(iter.getTopValue(), "abc.3");
        
    }
    
    @Test(expected = NullPointerException.class)
    public void testNullOptions() throws IOException {
        
        TreeMultimap<Key,Value> map = TreeMultimap.create();
        
        map.put(newKey("20121002_1", "UUID", "abc", true), new Value(createNewUidList("abc.0").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.1").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.2").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.3").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.4").build().toByteArray()));
        
        map.put(newKey("20121002_1", "UUID", "abd"), new Value(createNewUidList("abc.3").build().toByteArray()));
        
        SortedMultiMapIterator data = new SortedMultiMapIterator(map);
        
        PropogatingIterator iter = new PropogatingIterator();
        Map<String,String> options = Maps.newHashMap();
        
        options.put(PropogatingIterator.AGGREGATOR_DEFAULT, GlobalIndexUidAggregator.class.getCanonicalName());
        
        IteratorEnvironment env = new MockIteratorEnvironment(false);
        
        iter.init(data, null, env);
        
        iter.seek(new Range(), Collections.<ByteSequence> emptyList(), false);
    }
    
    @Test(expected = NullPointerException.class)
    public void testNullEnvironment() throws IOException {
        
        TreeMultimap<Key,Value> map = TreeMultimap.create();
        
        map.put(newKey("20121002_1", "UUID", "abc", true), new Value(createNewUidList("abc.0").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.1").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.2").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.3").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.4").build().toByteArray()));
        
        map.put(newKey("20121002_1", "UUID", "abd"), new Value(createNewUidList("abc.3").build().toByteArray()));
        
        SortedMultiMapIterator data = new SortedMultiMapIterator(map);
        
        PropogatingIterator iter = new PropogatingIterator();
        Map<String,String> options = Maps.newHashMap();
        
        options.put(PropogatingIterator.AGGREGATOR_DEFAULT, GlobalIndexUidAggregator.class.getCanonicalName());
        
        IteratorEnvironment env = new MockIteratorEnvironment(false);
        
        iter.init(data, options, null);
        
        iter.seek(new Range(), Collections.<ByteSequence> emptyList(), false);
    }
    
    @Test
    public void testForceNoPropogate() throws IOException {
        TreeMultimap<Key,Value> map = TreeMultimap.create();
        
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.0").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createremove("abc.0").build().toByteArray()));
        
        map.put(newKey("20121002_1", "UUID", "abd"), new Value(createNewUidList("abc.3").build().toByteArray()));
        
        SortedMultiMapIterator data = new SortedMultiMapIterator(map);
        
        PropogatingIterator iter = new PropogatingIterator();
        Map<String,String> options = Maps.newHashMap();
        
        options.put(PropogatingIterator.AGGREGATOR_DEFAULT, GlobalIndexUidAggregator.class.getCanonicalName());
        
        IteratorEnvironment env = new MockIteratorEnvironment(true);
        
        iter.init(data, options, env);
        
        iter.seek(new Range(), Collections.<ByteSequence> emptyList(), false);
        
        Assert.assertTrue(iter.hasTop());
        
        Key topKey = iter.getTopKey();
        
        topKey = iter.getTopKey();
        Assert.assertEquals(newKey("20121002_1", "UUID", "abd"), topKey);
        
    }
    
    @Test
    public void testNoAggregator() throws IOException {
        TreeMultimap<Key,Value> map = TreeMultimap.create();
        
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.0").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createremove("abc.0").build().toByteArray()));
        
        map.put(newKey("20121002_1", "UUID", "abd"), new Value(createNewUidList("abc.3").build().toByteArray()));
        
        SortedMultiMapIterator data = new SortedMultiMapIterator(map);
        
        PropogatingIterator iter = new PropogatingIterator();
        Map<String,String> options = Maps.newHashMap();
        
        IteratorEnvironment env = new MockIteratorEnvironment(false);
        
        iter.init(data, options, env);
        
        iter.seek(new Range(), Collections.<ByteSequence> emptyList(), false);
        
        Assert.assertTrue(iter.hasTop());
        
        Key topKey = iter.getTopKey();
        
        Assert.assertEquals(newKey("20121002_1", "UUID", "abc"), topKey);
        validateUids(iter.getTopValue(), "abc.0");
        iter.next();
        Assert.assertEquals(newKey("20121002_1", "UUID", "abc"), topKey);
        validateRemoval(iter.getTopValue(), "abc.0");
        iter.next();
        topKey = iter.getTopKey();
        Assert.assertEquals(newKey("20121002_1", "UUID", "abd"), topKey);
        
    }
    
    @Test
    public void testDeleteAggregateFour() throws IOException {
        TreeMultimap<Key,Value> map = TreeMultimap.create();
        
        map.put(newKey("20121002_1", "UUID", "abc", true), new Value(createNewUidList("abc.0").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.1").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.2").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.3").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.4").build().toByteArray()));
        
        map.put(newKey("20121002_1", "UUID", "abd"), new Value(createNewUidList("abc.3").build().toByteArray()));
        
        SortedMultiMapIterator data = new SortedMultiMapIterator(map);
        
        PropogatingIterator iter = new PropogatingIterator();
        Map<String,String> options = Maps.newHashMap();
        
        options.put(PropogatingIterator.AGGREGATOR_DEFAULT, GlobalIndexUidAggregator.class.getCanonicalName());
        
        IteratorEnvironment env = new MockIteratorEnvironment(false);
        
        iter.init(data, options, env);
        
        iter.seek(new Range(), Collections.<ByteSequence> emptyList(), false);
        
        Assert.assertTrue(iter.hasTop());
        
        Key topKey = iter.getTopKey();
        
        Assert.assertEquals(newKey("20121002_1", "UUID", "abc", true), topKey);
        validateUids(iter.getTopValue(), "abc.0", "abc.1", "abc.2", "abc.3", "abc.4");
        iter.next();
        topKey = iter.getTopKey();
        Assert.assertEquals(newKey("20121002_1", "UUID", "abd"), topKey);
        
    }
    
    @Test
    public void testDeleteFullMajc() throws IOException {
        TreeMultimap<Key,Value> map = TreeMultimap.create();
        
        map.put(newKey("20121002_1", "UUID", "abc", true), new Value(createNewUidList("abc.0").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.1").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.2").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.3").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.4").build().toByteArray()));
        
        map.put(newKey("20121002_1", "UUID", "abd"), new Value(createNewUidList("abc.3").build().toByteArray()));
        
        SortedMultiMapIterator data = new SortedMultiMapIterator(map);
        
        PropogatingIterator iter = new PropogatingIterator();
        Map<String,String> options = Maps.newHashMap();
        
        options.put(PropogatingIterator.AGGREGATOR_DEFAULT, GlobalIndexUidAggregator.class.getCanonicalName());
        
        IteratorEnvironment env = new MockIteratorEnvironment(true);
        
        iter.init(data, options, env);
        
        iter.seek(new Range(), Collections.<ByteSequence> emptyList(), false);
        
        Assert.assertTrue(iter.hasTop());
        
        Key topKey = iter.getTopKey();
        
        Assert.assertEquals(newKey("20121002_1", "UUID", "abc", true), topKey);
        validateUids(iter.getTopValue(), "abc.0", "abc.1", "abc.2", "abc.3", "abc.4");
        iter.next();
        topKey = iter.getTopKey();
        Assert.assertEquals(newKey("20121002_1", "UUID", "abd"), topKey);
        
    }
    
    @Test(expected = NullPointerException.class)
    public void testDeepCopyCannotCopyUninitialized() {
        
        PropogatingIterator uut = new PropogatingIterator();
        IteratorEnvironment env = new PropogatingIteratorTest.MockIteratorEnvironment(false);
        
        uut.deepCopy(env);
    }
    
    @Test
    public void testCtor() throws IOException {
        
        PropogatingIterator uut = new PropogatingIterator();
        
        Assert.assertNotNull("ProgpatingIterator constructor failed to create a valid instance.", uut);
        
        TreeMultimap<Key,Value> map = TreeMultimap.create();
        
        map.put(newKey("20121002_1", "UUID", "abc", true), new Value(createNewUidList("abc.0").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.1").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.2").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.3").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.4").build().toByteArray()));
        
        map.put(newKey("20121002_1", "UUID", "abd"), new Value(createNewUidList("abc.3").build().toByteArray()));
        
        SortedMultiMapIterator data = new SortedMultiMapIterator(map);
        
        ColumnToClassMapping<Combiner> Aggregators = null;
        
        uut = new PropogatingIterator(data, Aggregators);
        
        Assert.assertNotNull("ProgpatingIterator constructor failed to create a valid instance.", uut);
        
    }
    
    @Test
    public void test2ArgCtor() throws IOException {
        
        TreeMultimap<Key,Value> map = TreeMultimap.create();
        
        map.put(newKey("20121002_1", "UUID", "abc", true), new Value(createNewUidList("abc.0").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.1").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.2").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.3").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.4").build().toByteArray()));
        
        map.put(newKey("20121002_1", "UUID", "abd"), new Value(createNewUidList("abc.3").build().toByteArray()));
        
        SortedMultiMapIterator data = new SortedMultiMapIterator(map);
        
        ColumnToClassMapping<Combiner> Aggregators = null;
        
        PropogatingIterator uut = new PropogatingIterator(data, Aggregators);
        
        Assert.assertNotNull("ProgpatingIterator constructor failed to create a valid instance.", uut);
        
    }
    
    @Test
    public void testAggregateThreeWithDeepCopy() throws IOException {
        TreeMultimap<Key,Value> map = TreeMultimap.create();
        
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.1").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.2").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.3").build().toByteArray()));
        
        map.put(newKey("20121002_1", "UUID", "abd"), new Value(createNewUidList("abc.3").build().toByteArray()));
        
        SortedMultiMapIterator data = new SortedMultiMapIterator(map);
        
        PropogatingIterator iter = new PropogatingIterator();
        Map<String,String> options = Maps.newHashMap();
        
        options.put(PropogatingIterator.AGGREGATOR_DEFAULT, GlobalIndexUidAggregator.class.getCanonicalName());
        
        IteratorEnvironment env = new MockIteratorEnvironment(false);
        
        iter.init(data, options, env);
        iter = iter.deepCopy(env);
        
        iter.seek(new Range(), Collections.<ByteSequence> emptyList(), false);
        
        Assert.assertTrue(iter.hasTop());
        
        Key topKey = iter.getTopKey();
        
        Assert.assertEquals(newKey("20121002_1", "UUID", "abc"), topKey);
        validateUids(iter.getTopValue(), "abc.1", "abc.2", "abc.3");
        iter.next();
        topKey = iter.getTopKey();
        Assert.assertEquals(newKey("20121002_1", "UUID", "abd"), topKey);
        
    }
    
    @Test
    public void testAggregateFourWithDeepCopy() throws IOException {
        TreeMultimap<Key,Value> map = TreeMultimap.create();
        
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.1").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.2").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.3").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.4").build().toByteArray()));
        
        map.put(newKey("20121002_1", "UUID", "abd"), new Value(createNewUidList("abc.3").build().toByteArray()));
        
        SortedMultiMapIterator data = new SortedMultiMapIterator(map);
        
        PropogatingIterator iter = new PropogatingIterator();
        Map<String,String> options = Maps.newHashMap();
        
        options.put(PropogatingIterator.AGGREGATOR_DEFAULT, GlobalIndexUidAggregator.class.getCanonicalName());
        
        IteratorEnvironment env = new MockIteratorEnvironment(false);
        
        iter.init(data, options, env);
        iter = iter.deepCopy(env);
        
        iter.seek(new Range(), Collections.<ByteSequence> emptyList(), false);
        
        Assert.assertTrue(iter.hasTop());
        
        Key topKey = iter.getTopKey();
        
        Assert.assertEquals(newKey("20121002_1", "UUID", "abc"), topKey);
        validateUids(iter.getTopValue(), "abc.1", "abc.2", "abc.3", "abc.4");
        iter.next();
        topKey = iter.getTopKey();
        Assert.assertEquals(newKey("20121002_1", "UUID", "abd"), topKey);
        validateUids(iter.getTopValue(), "abc.3");
        
    }
    
    @Test(expected = NullPointerException.class)
    public void testNullOptionsWithDeepCopy() throws IOException {
        
        TreeMultimap<Key,Value> map = TreeMultimap.create();
        
        map.put(newKey("20121002_1", "UUID", "abc", true), new Value(createNewUidList("abc.0").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.1").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.2").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.3").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.4").build().toByteArray()));
        
        map.put(newKey("20121002_1", "UUID", "abd"), new Value(createNewUidList("abc.3").build().toByteArray()));
        
        SortedMultiMapIterator data = new SortedMultiMapIterator(map);
        
        PropogatingIterator iter = new PropogatingIterator();
        Map<String,String> options = Maps.newHashMap();
        
        options.put(PropogatingIterator.AGGREGATOR_DEFAULT, GlobalIndexUidAggregator.class.getCanonicalName());
        
        IteratorEnvironment env = new MockIteratorEnvironment(false);
        
        iter.init(data, null, env);
        
        iter.seek(new Range(), Collections.<ByteSequence> emptyList(), false);
    }
    
    @Test(expected = NullPointerException.class)
    public void testNullEnvironmentWithDeepCopy() throws IOException {
        
        TreeMultimap<Key,Value> map = TreeMultimap.create();
        
        map.put(newKey("20121002_1", "UUID", "abc", true), new Value(createNewUidList("abc.0").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.1").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.2").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.3").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.4").build().toByteArray()));
        
        map.put(newKey("20121002_1", "UUID", "abd"), new Value(createNewUidList("abc.3").build().toByteArray()));
        
        SortedMultiMapIterator data = new SortedMultiMapIterator(map);
        
        PropogatingIterator iter = new PropogatingIterator();
        Map<String,String> options = Maps.newHashMap();
        
        options.put(PropogatingIterator.AGGREGATOR_DEFAULT, GlobalIndexUidAggregator.class.getCanonicalName());
        
        IteratorEnvironment env = new MockIteratorEnvironment(false);
        
        iter.init(data, options, null);
        
        iter.seek(new Range(), Collections.<ByteSequence> emptyList(), false);
    }
    
    @Test
    public void testForceNoPropogateWithDeepCopy() throws IOException {
        TreeMultimap<Key,Value> map = TreeMultimap.create();
        
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.0").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createremove("abc.0").build().toByteArray()));
        
        map.put(newKey("20121002_1", "UUID", "abd"), new Value(createNewUidList("abc.3").build().toByteArray()));
        
        SortedMultiMapIterator data = new SortedMultiMapIterator(map);
        
        PropogatingIterator iter = new PropogatingIterator();
        Map<String,String> options = Maps.newHashMap();
        
        options.put(PropogatingIterator.AGGREGATOR_DEFAULT, GlobalIndexUidAggregator.class.getCanonicalName());
        
        IteratorEnvironment env = new MockIteratorEnvironment(true);
        
        iter.init(data, options, env);
        iter = iter.deepCopy(env);
        
        iter.seek(new Range(), Collections.<ByteSequence> emptyList(), false);
        
        Assert.assertTrue(iter.hasTop());
        
        Key topKey = iter.getTopKey();
        
        topKey = iter.getTopKey();
        Assert.assertEquals(newKey("20121002_1", "UUID", "abd"), topKey);
        
    }
    
    @Test
    public void testNoAggregatorWithDeepCopy() throws IOException {
        TreeMultimap<Key,Value> map = TreeMultimap.create();
        
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.0").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createremove("abc.0").build().toByteArray()));
        
        map.put(newKey("20121002_1", "UUID", "abd"), new Value(createNewUidList("abc.3").build().toByteArray()));
        
        SortedMultiMapIterator data = new SortedMultiMapIterator(map);
        
        PropogatingIterator iter = new PropogatingIterator();
        Map<String,String> options = Maps.newHashMap();
        
        IteratorEnvironment env = new MockIteratorEnvironment(false);
        
        iter.init(data, options, env);
        iter = iter.deepCopy(env);
        
        iter.seek(new Range(), Collections.<ByteSequence> emptyList(), false);
        
        Assert.assertTrue(iter.hasTop());
        
        Key topKey = iter.getTopKey();
        
        Assert.assertEquals(newKey("20121002_1", "UUID", "abc"), topKey);
        validateUids(iter.getTopValue(), "abc.0");
        iter.next();
        Assert.assertEquals(newKey("20121002_1", "UUID", "abc"), topKey);
        validateRemoval(iter.getTopValue(), "abc.0");
        iter.next();
        topKey = iter.getTopKey();
        Assert.assertEquals(newKey("20121002_1", "UUID", "abd"), topKey);
        
    }
    
    @Test
    public void testDeleteAggregateFourWithDeepCopy() throws IOException {
        TreeMultimap<Key,Value> map = TreeMultimap.create();
        
        map.put(newKey("20121002_1", "UUID", "abc", true), new Value(createNewUidList("abc.0").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.1").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.2").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.3").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.4").build().toByteArray()));
        
        map.put(newKey("20121002_1", "UUID", "abd"), new Value(createNewUidList("abc.3").build().toByteArray()));
        
        SortedMultiMapIterator data = new SortedMultiMapIterator(map);
        
        PropogatingIterator iter = new PropogatingIterator();
        Map<String,String> options = Maps.newHashMap();
        
        options.put(PropogatingIterator.AGGREGATOR_DEFAULT, GlobalIndexUidAggregator.class.getCanonicalName());
        
        IteratorEnvironment env = new MockIteratorEnvironment(false);
        
        iter.init(data, options, env);
        iter = iter.deepCopy(env);
        
        iter.seek(new Range(), Collections.<ByteSequence> emptyList(), false);
        
        Assert.assertTrue(iter.hasTop());
        
        Key topKey = iter.getTopKey();
        
        Assert.assertEquals(newKey("20121002_1", "UUID", "abc", true), topKey);
        validateUids(iter.getTopValue(), "abc.0", "abc.1", "abc.2", "abc.3", "abc.4");
        iter.next();
        topKey = iter.getTopKey();
        Assert.assertEquals(newKey("20121002_1", "UUID", "abd"), topKey);
        
    }
    
    @Test
    public void testDeleteFullMajcWithDeepCopy() throws IOException {
        TreeMultimap<Key,Value> map = TreeMultimap.create();
        
        map.put(newKey("20121002_1", "UUID", "abc", true), new Value(createNewUidList("abc.0").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.1").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.2").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.3").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.4").build().toByteArray()));
        
        map.put(newKey("20121002_1", "UUID", "abd"), new Value(createNewUidList("abc.3").build().toByteArray()));
        
        SortedMultiMapIterator data = new SortedMultiMapIterator(map);
        
        PropogatingIterator iter = new PropogatingIterator();
        Map<String,String> options = Maps.newHashMap();
        
        options.put(PropogatingIterator.AGGREGATOR_DEFAULT, GlobalIndexUidAggregator.class.getCanonicalName());
        
        IteratorEnvironment env = new MockIteratorEnvironment(true);
        
        iter.init(data, options, env);
        iter = iter.deepCopy(env);
        
        iter.seek(new Range(), Collections.<ByteSequence> emptyList(), false);
        
        Assert.assertTrue(iter.hasTop());
        
        Key topKey = iter.getTopKey();
        
        Assert.assertEquals(newKey("20121002_1", "UUID", "abc", true), topKey);
        validateUids(iter.getTopValue(), "abc.0", "abc.1", "abc.2", "abc.3", "abc.4");
        iter.next();
        topKey = iter.getTopKey();
        Assert.assertEquals(newKey("20121002_1", "UUID", "abd"), topKey);
        
    }
    
    @Test
    public void testDeleteFullMajcWithDeepCopyThreaded() throws IOException, InterruptedException, ExecutionException {
        TreeMultimap<Key,Value> map = TreeMultimap.create();
        
        map.put(newKey("20121002_1", "UUID", "abc", true), new Value(createNewUidList("abc.0").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.1").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.2").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.3").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.4").build().toByteArray()));
        
        map.put(newKey("20121002_1", "UUID", "abd"), new Value(createNewUidList("abc.3").build().toByteArray()));
        
        SortedMultiMapIterator data = new SortedMultiMapIterator(map);
        
        final PropogatingIterator iter = new PropogatingIterator();
        Map<String,String> options = Maps.newHashMap();
        
        options.put(PropogatingIterator.AGGREGATOR_DEFAULT, GlobalIndexUidAggregator.class.getCanonicalName());
        
        final IteratorEnvironment env = new MockIteratorEnvironment(true);
        
        iter.init(data, options, env);
        ExecutorService exec = Executors.newFixedThreadPool(10);
        
        List<Future<List<Entry<Key,Value>>>> futures = Lists.newArrayList();
        for (int i = 0; i < 25; i++) {
            futures.add(exec.submit(new Callable<List<Entry<Key,Value>>>() {
                
                @Override
                public List<Entry<Key,Value>> call() throws Exception {
                    SortedKeyValueIterator<Key,Value> myitr = iter.deepCopy(env);
                    Random rand = new Random();
                    LockSupport.parkNanos(rand.nextInt(10));
                    
                    myitr.seek(new Range(), Collections.<ByteSequence> emptyList(), false);
                    
                    List<Entry<Key,Value>> resultList = Lists.newArrayList();
                    
                    Assert.assertTrue(myitr.hasTop());
                    
                    Key topKey = myitr.getTopKey();
                    resultList.add(Maps.immutableEntry(topKey, myitr.getTopValue()));
                    myitr.next();
                    resultList.add(Maps.immutableEntry(myitr.getTopKey(), myitr.getTopValue()));
                    
                    return resultList;
                }
                
            }));
        }
        
        for (Future<List<Entry<Key,Value>>> future : futures) {
            List<Entry<Key,Value>> kvList = future.get();
            
            Assert.assertEquals(2, kvList.size());
            
            Entry<Key,Value> kv1 = kvList.get(0);
            Entry<Key,Value> kv2 = kvList.get(1);
            
            Assert.assertEquals(newKey("20121002_1", "UUID", "abc", true), kv1.getKey());
            validateUids(kv1.getValue(), "abc.0", "abc.1", "abc.2", "abc.3", "abc.4");
            Assert.assertEquals(newKey("20121002_1", "UUID", "abd"), kv2.getKey());
        }
        exec.shutdownNow();
        
    }
    
    @Test
    public void testCtorWithDeepCopy() throws IOException {
        
        PropogatingIterator uut = new PropogatingIterator();
        
        Assert.assertNotNull("ProgpatingIterator constructor failed to create a valid instance.", uut);
        
        TreeMultimap<Key,Value> map = TreeMultimap.create();
        
        map.put(newKey("20121002_1", "UUID", "abc", true), new Value(createNewUidList("abc.0").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.1").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.2").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.3").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.4").build().toByteArray()));
        
        map.put(newKey("20121002_1", "UUID", "abd"), new Value(createNewUidList("abc.3").build().toByteArray()));
        
        SortedMultiMapIterator data = new SortedMultiMapIterator(map);
        
        ColumnToClassMapping<Combiner> Aggregators = null;
        
        uut = new PropogatingIterator(data, Aggregators);
        
        Assert.assertNotNull("ProgpatingIterator constructor failed to create a valid instance.", uut);
        
    }
    
    @Test
    public void test2ArgCtorWithDeepCopy() throws IOException {
        
        TreeMultimap<Key,Value> map = TreeMultimap.create();
        
        map.put(newKey("20121002_1", "UUID", "abc", true), new Value(createNewUidList("abc.0").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.1").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.2").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.3").build().toByteArray()));
        map.put(newKey("20121002_1", "UUID", "abc"), new Value(createNewUidList("abc.4").build().toByteArray()));
        
        map.put(newKey("20121002_1", "UUID", "abd"), new Value(createNewUidList("abc.3").build().toByteArray()));
        
        SortedMultiMapIterator data = new SortedMultiMapIterator(map);
        
        ColumnToClassMapping<Combiner> Aggregators = null;
        
        PropogatingIterator uut = new PropogatingIterator(data, Aggregators);
        
        Assert.assertNotNull("ProgpatingIterator constructor failed to create a valid instance.", uut);
        
    }
    
}
