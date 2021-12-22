package datawave.iterators;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.LockSupport;

import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.TreeMultimap;
import com.google.protobuf.InvalidProtocolBufferException;

import datawave.ingest.protobuf.Uid;
import datawave.ingest.table.aggregator.GlobalIndexUidAggregator;

import static org.junit.Assert.assertTrue;

public class PropogatingIteratorTest {
    private static final String SHARD = "20121002_1";
    private static final String FIELD_TO_AGGREGATE = "UUID";
    private static final long TIMESTAMP = 1349541830;
    
    private void validateUids(Value topValue, String... uids) throws InvalidProtocolBufferException {
        Uid.List v = Uid.List.parseFrom(topValue.get());
        
        Assert.assertEquals(uids.length, v.getCOUNT());
        for (String uid : uids) {
            assertTrue(v.getUIDList().contains(uid));
        }
    }
    
    private void validateRemoval(Value topValue, String... uids) throws InvalidProtocolBufferException {
        Uid.List v = Uid.List.parseFrom(topValue.get());
        
        Assert.assertEquals(-uids.length, v.getCOUNT());
        for (String uid : uids) {
            assertTrue(v.getREMOVEDUIDList().contains(uid));
        }
    }
    
    private Uid.List.Builder createValueWithUid(String uid) {
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.setIGNORE(false);
        builder.setCOUNT(1);
        builder.addUID(uid);
        
        return builder;
    }
    
    private Uid.List.Builder createValueWithRemoveUid(String uid) {
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.setIGNORE(false);
        builder.setCOUNT(-1);
        builder.addREMOVEDUID(uid);
        
        return builder;
    }
    
    public static class MockIteratorEnvironment implements IteratorEnvironment {
        AccumuloConfiguration conf;
        private final boolean major;
        
        public MockIteratorEnvironment(boolean major) {
            this.conf = DefaultConfiguration.getInstance();
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
        
        @Override
        public Authorizations getAuthorizations() {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public IteratorEnvironment cloneWithSamplingEnabled() {
            throw new SampleNotPresentException();
        }
        
        @Override
        public boolean isSamplingEnabled() {
            return false;
        }
        
        @Override
        public SamplerConfiguration getSamplerConfiguration() {
            return null;
        }
        
        @Override
        public SortedKeyValueIterator<Key,Value> reserveMapFileReader(String arg0) {
            return null;
        }
    }
    
    protected Key newKey(String row, String field, String uid, boolean delete) {
        Key key = new Key(uid, field, "dataType\0" + row, new ColumnVisibility("PUBLIC"), TIMESTAMP);
        key.setDeleted(delete);
        return key;
    }
    
    protected Key newKey(String row, String field, String uid) {
        return newKey(row, field, uid, false);
    }
    
    @Test
    public void testAggregateThree() throws IOException {
        TreeMultimap<Key,Value> map = TreeMultimap.create();
        
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abc"), new Value(createValueWithUid("abc.1").build().toByteArray()));
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abc"), new Value(createValueWithUid("abc.2").build().toByteArray()));
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abc"), new Value(createValueWithUid("abc.3").build().toByteArray()));
        
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abd"), new Value(createValueWithUid("abc.3").build().toByteArray()));
        
        SortedMultiMapIterator data = new SortedMultiMapIterator(map);
        
        PropogatingIterator iter = new PropogatingIterator();
        Map<String,String> options = Maps.newHashMap();
        
        options.put(PropogatingIterator.AGGREGATOR_DEFAULT, GlobalIndexUidAggregator.class.getCanonicalName());
        
        IteratorEnvironment env = new MockIteratorEnvironment(false);
        
        iter.init(data, options, env);
        
        iter.seek(new Range(), Collections.emptyList(), false);
        
        assertTrue(iter.hasTop());
        
        Key topKey = iter.getTopKey();
        
        Assert.assertEquals(newKey(SHARD, FIELD_TO_AGGREGATE, "abc"), topKey);
        validateUids(iter.getTopValue(), "abc.1", "abc.2", "abc.3");
        iter.next();
        topKey = iter.getTopKey();
        Assert.assertEquals(newKey(SHARD, FIELD_TO_AGGREGATE, "abd"), topKey);
        
    }
    
    @Test
    public void testAggregateFour() throws IOException {
        TreeMultimap<Key,Value> map = TreeMultimap.create();
        
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abc"), new Value(createValueWithUid("abc.1").build().toByteArray()));
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abc"), new Value(createValueWithUid("abc.2").build().toByteArray()));
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abc"), new Value(createValueWithUid("abc.3").build().toByteArray()));
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abc"), new Value(createValueWithUid("abc.4").build().toByteArray()));
        
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abd"), new Value(createValueWithUid("abc.3").build().toByteArray()));
        
        SortedMultiMapIterator data = new SortedMultiMapIterator(map);
        
        PropogatingIterator iter = new PropogatingIterator();
        Map<String,String> options = Maps.newHashMap();
        
        options.put(PropogatingIterator.AGGREGATOR_DEFAULT, GlobalIndexUidAggregator.class.getCanonicalName());
        
        IteratorEnvironment env = new MockIteratorEnvironment(false);
        
        iter.init(data, options, env);
        
        iter.seek(new Range(), Collections.emptyList(), false);
        
        assertTrue(iter.hasTop());
        
        Key topKey = iter.getTopKey();
        
        Assert.assertEquals(newKey(SHARD, FIELD_TO_AGGREGATE, "abc"), topKey);
        validateUids(iter.getTopValue(), "abc.1", "abc.2", "abc.3", "abc.4");
        iter.next();
        topKey = iter.getTopKey();
        Assert.assertEquals(newKey(SHARD, FIELD_TO_AGGREGATE, "abd"), topKey);
        validateUids(iter.getTopValue(), "abc.3");
        
    }
    
    @Test(expected = NullPointerException.class)
    public void testNullOptions() throws IOException {
        
        PropogatingIterator iter = new PropogatingIterator();
        
        IteratorEnvironment env = new MockIteratorEnvironment(false);
        
        iter.init(createSourceWithTestData(), null, env);
        
        iter.seek(new Range(), Collections.emptyList(), false);
    }
    
    private SortedMultiMapIterator createSourceWithTestData() {
        TreeMultimap<Key,Value> map = TreeMultimap.create();
        
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abc", true), new Value(createValueWithUid("abc.0").build().toByteArray()));
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abc"), new Value(createValueWithUid("abc.1").build().toByteArray()));
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abc"), new Value(createValueWithUid("abc.2").build().toByteArray()));
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abc"), new Value(createValueWithUid("abc.3").build().toByteArray()));
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abc"), new Value(createValueWithUid("abc.4").build().toByteArray()));
        
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abd"), new Value(createValueWithUid("abc.3").build().toByteArray()));
        
        return new SortedMultiMapIterator(map);
    }
    
    @Test(expected = NullPointerException.class)
    public void testNullEnvironment() throws IOException {
        PropogatingIterator iter = new PropogatingIterator();
        Map<String,String> options = Maps.newHashMap();
        
        options.put(PropogatingIterator.AGGREGATOR_DEFAULT, GlobalIndexUidAggregator.class.getCanonicalName());
        
        iter.init(createSourceWithTestData(), options, null);
        
        iter.seek(new Range(), Collections.emptyList(), false);
    }
    
    @Test
    public void testForceNoPropogate() throws IOException {
        TreeMultimap<Key,Value> map = TreeMultimap.create();
        
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abc"), new Value(createValueWithUid("abc.0").build().toByteArray()));
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abc"), new Value(createValueWithRemoveUid("abc.0").build().toByteArray()));
        
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abd"), new Value(createValueWithUid("abc.3").build().toByteArray()));
        
        SortedMultiMapIterator data = new SortedMultiMapIterator(map);
        
        PropogatingIterator iter = new PropogatingIterator();
        Map<String,String> options = Maps.newHashMap();
        
        options.put(PropogatingIterator.AGGREGATOR_DEFAULT, GlobalIndexUidAggregator.class.getCanonicalName());
        
        IteratorEnvironment env = new MockIteratorEnvironment(true);
        
        iter.init(data, options, env);
        
        iter.seek(new Range(), Collections.emptyList(), false);
        
        assertTrue(iter.hasTop());
        
        Key topKey = iter.getTopKey();
        Assert.assertEquals(newKey(SHARD, FIELD_TO_AGGREGATE, "abd"), topKey);
        
    }
    
    @Test
    public void testNoAggregator() throws IOException {
        TreeMultimap<Key,Value> map = TreeMultimap.create();
        
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abc"), new Value(createValueWithUid("abc.0").build().toByteArray()));
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abc"), new Value(createValueWithRemoveUid("abc.0").build().toByteArray()));
        
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abd"), new Value(createValueWithUid("abc.3").build().toByteArray()));
        
        SortedMultiMapIterator data = new SortedMultiMapIterator(map);
        
        PropogatingIterator iter = new PropogatingIterator();
        Map<String,String> options = Maps.newHashMap();
        
        IteratorEnvironment env = new MockIteratorEnvironment(false);
        
        iter.init(data, options, env);
        
        iter.seek(new Range(), Collections.emptyList(), false);
        
        assertTrue(iter.hasTop());
        
        Key topKey = iter.getTopKey();
        
        Assert.assertEquals(newKey(SHARD, FIELD_TO_AGGREGATE, "abc"), topKey);
        validateUids(iter.getTopValue(), "abc.0");
        iter.next();
        Assert.assertEquals(newKey(SHARD, FIELD_TO_AGGREGATE, "abc"), topKey);
        validateRemoval(iter.getTopValue(), "abc.0");
        iter.next();
        topKey = iter.getTopKey();
        Assert.assertEquals(newKey(SHARD, FIELD_TO_AGGREGATE, "abd"), topKey);
        
    }
    
    @Test
    public void testDeleteAggregateFour() throws IOException {
        SortedMultiMapIterator data = createSourceWithTestData();
        
        PropogatingIterator iter = new PropogatingIterator();
        Map<String,String> options = Maps.newHashMap();
        
        options.put(PropogatingIterator.AGGREGATOR_DEFAULT, GlobalIndexUidAggregator.class.getCanonicalName());
        
        IteratorEnvironment env = new MockIteratorEnvironment(false);
        
        iter.init(data, options, env);
        
        iter.seek(new Range(), Collections.emptyList(), false);
        
        assertTrue(iter.hasTop());
        
        Key topKey = iter.getTopKey();
        
        Assert.assertEquals(newKey(SHARD, FIELD_TO_AGGREGATE, "abc", true), topKey);
        validateUids(iter.getTopValue(), "abc.0", "abc.1", "abc.2", "abc.3", "abc.4");
        iter.next();
        topKey = iter.getTopKey();
        Assert.assertEquals(newKey(SHARD, FIELD_TO_AGGREGATE, "abd"), topKey);
        
    }
    
    @Test
    public void testDeleteFullMajc() throws IOException {
        
        PropogatingIterator iter = new PropogatingIterator();
        Map<String,String> options = Maps.newHashMap();
        
        options.put(PropogatingIterator.AGGREGATOR_DEFAULT, GlobalIndexUidAggregator.class.getCanonicalName());
        
        IteratorEnvironment env = new MockIteratorEnvironment(true);
        
        iter.init(createSourceWithTestData(), options, env);
        
        iter.seek(new Range(), Collections.emptyList(), false);
        
        assertTrue(iter.hasTop());
        
        Key topKey = iter.getTopKey();
        
        Assert.assertEquals(newKey(SHARD, FIELD_TO_AGGREGATE, "abc", true), topKey);
        validateUids(iter.getTopValue(), "abc.0", "abc.1", "abc.2", "abc.3", "abc.4");
        iter.next();
        topKey = iter.getTopKey();
        Assert.assertEquals(newKey(SHARD, FIELD_TO_AGGREGATE, "abd"), topKey);
        
    }
    
    @Test(expected = NullPointerException.class)
    public void testDeepCopyCannotCopyUninitialized() {
        PropogatingIterator uut = new PropogatingIterator();
        uut.deepCopy(new MockIteratorEnvironment(false));
    }
    
    @Test
    public void testAggregateThreeWithDeepCopy() throws IOException {
        TreeMultimap<Key,Value> map = TreeMultimap.create();
        
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abc"), new Value(createValueWithUid("abc.1").build().toByteArray()));
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abc"), new Value(createValueWithUid("abc.2").build().toByteArray()));
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abc"), new Value(createValueWithUid("abc.3").build().toByteArray()));
        
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abd"), new Value(createValueWithUid("abc.3").build().toByteArray()));
        
        SortedMultiMapIterator data = new SortedMultiMapIterator(map);
        
        PropogatingIterator iter = new PropogatingIterator();
        Map<String,String> options = Maps.newHashMap();
        
        options.put(PropogatingIterator.AGGREGATOR_DEFAULT, GlobalIndexUidAggregator.class.getCanonicalName());
        
        IteratorEnvironment env = new MockIteratorEnvironment(false);
        
        iter.init(data, options, env);
        iter = iter.deepCopy(env);
        
        iter.seek(new Range(), Collections.emptyList(), false);
        
        assertTrue(iter.hasTop());
        
        Key topKey = iter.getTopKey();
        
        Assert.assertEquals(newKey(SHARD, FIELD_TO_AGGREGATE, "abc"), topKey);
        validateUids(iter.getTopValue(), "abc.1", "abc.2", "abc.3");
        iter.next();
        topKey = iter.getTopKey();
        Assert.assertEquals(newKey(SHARD, FIELD_TO_AGGREGATE, "abd"), topKey);
        
    }
    
    @Test
    public void testAggregateFourWithDeepCopy() throws IOException {
        TreeMultimap<Key,Value> map = TreeMultimap.create();
        
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abc"), new Value(createValueWithUid("abc.1").build().toByteArray()));
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abc"), new Value(createValueWithUid("abc.2").build().toByteArray()));
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abc"), new Value(createValueWithUid("abc.3").build().toByteArray()));
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abc"), new Value(createValueWithUid("abc.4").build().toByteArray()));
        
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abd"), new Value(createValueWithUid("abc.3").build().toByteArray()));
        
        SortedMultiMapIterator data = new SortedMultiMapIterator(map);
        
        PropogatingIterator iter = new PropogatingIterator();
        Map<String,String> options = Maps.newHashMap();
        
        options.put(PropogatingIterator.AGGREGATOR_DEFAULT, GlobalIndexUidAggregator.class.getCanonicalName());
        
        IteratorEnvironment env = new MockIteratorEnvironment(false);
        
        iter.init(data, options, env);
        iter = iter.deepCopy(env);
        
        iter.seek(new Range(), Collections.emptyList(), false);
        
        assertTrue(iter.hasTop());
        
        Key topKey = iter.getTopKey();
        
        Assert.assertEquals(newKey(SHARD, FIELD_TO_AGGREGATE, "abc"), topKey);
        validateUids(iter.getTopValue(), "abc.1", "abc.2", "abc.3", "abc.4");
        iter.next();
        topKey = iter.getTopKey();
        Assert.assertEquals(newKey(SHARD, FIELD_TO_AGGREGATE, "abd"), topKey);
        validateUids(iter.getTopValue(), "abc.3");
        
    }
    
    @Test(expected = NullPointerException.class)
    public void testNullOptionsWithInit() throws IOException {
        IteratorEnvironment env = new MockIteratorEnvironment(false);
        new PropogatingIterator().init(createSourceWithTestData(), null, env);
    }
    
    @Test(expected = NullPointerException.class)
    public void testNullEnvironmentWithInit() throws IOException {
        Map<String,String> options = Maps.newHashMap();
        options.put(PropogatingIterator.AGGREGATOR_DEFAULT, GlobalIndexUidAggregator.class.getCanonicalName());
        new PropogatingIterator().init(createSourceWithTestData(), options, null);
    }
    
    @Test
    public void testForceNoPropogateWithDeepCopy() throws IOException {
        TreeMultimap<Key,Value> map = TreeMultimap.create();
        
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abc"), new Value(createValueWithUid("abc.0").build().toByteArray()));
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abc"), new Value(createValueWithRemoveUid("abc.0").build().toByteArray()));
        
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abd"), new Value(createValueWithUid("abc.3").build().toByteArray()));
        
        SortedMultiMapIterator data = new SortedMultiMapIterator(map);
        
        PropogatingIterator iter = new PropogatingIterator();
        Map<String,String> options = Maps.newHashMap();
        
        options.put(PropogatingIterator.AGGREGATOR_DEFAULT, GlobalIndexUidAggregator.class.getCanonicalName());
        
        IteratorEnvironment env = new MockIteratorEnvironment(true);
        
        iter.init(data, options, env);
        iter = iter.deepCopy(env);
        
        iter.seek(new Range(), Collections.emptyList(), false);
        
        assertTrue(iter.hasTop());
        
        Key topKey = iter.getTopKey();
        Assert.assertEquals(newKey(SHARD, FIELD_TO_AGGREGATE, "abd"), topKey);
        
    }
    
    @Test
    public void testNoAggregatorWithDeepCopy() throws IOException {
        TreeMultimap<Key,Value> map = TreeMultimap.create();
        
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abc"), new Value(createValueWithUid("abc.0").build().toByteArray()));
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abc"), new Value(createValueWithRemoveUid("abc.0").build().toByteArray()));
        
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abd"), new Value(createValueWithUid("abc.3").build().toByteArray()));
        
        SortedMultiMapIterator data = new SortedMultiMapIterator(map);
        
        PropogatingIterator iter = new PropogatingIterator();
        Map<String,String> options = Maps.newHashMap();
        
        IteratorEnvironment env = new MockIteratorEnvironment(false);
        
        iter.init(data, options, env);
        iter = iter.deepCopy(env);
        
        iter.seek(new Range(), Collections.emptyList(), false);
        
        assertTrue(iter.hasTop());
        
        Key topKey = iter.getTopKey();
        
        Assert.assertEquals(newKey(SHARD, FIELD_TO_AGGREGATE, "abc"), topKey);
        validateUids(iter.getTopValue(), "abc.0");
        iter.next();
        Assert.assertEquals(newKey(SHARD, FIELD_TO_AGGREGATE, "abc"), topKey);
        validateRemoval(iter.getTopValue(), "abc.0");
        iter.next();
        topKey = iter.getTopKey();
        Assert.assertEquals(newKey(SHARD, FIELD_TO_AGGREGATE, "abd"), topKey);
        
    }
    
    @Test
    public void testDeleteAggregateFourWithDeepCopy() throws IOException {
        PropogatingIterator iter = new PropogatingIterator();
        Map<String,String> options = Maps.newHashMap();
        
        options.put(PropogatingIterator.AGGREGATOR_DEFAULT, GlobalIndexUidAggregator.class.getCanonicalName());
        
        IteratorEnvironment env = new MockIteratorEnvironment(false);
        
        iter.init(createSourceWithTestData(), options, env);
        iter = iter.deepCopy(env);
        
        iter.seek(new Range(), Collections.emptyList(), false);
        
        assertTrue(iter.hasTop());
        
        Key topKey = iter.getTopKey();
        
        Assert.assertEquals(newKey(SHARD, FIELD_TO_AGGREGATE, "abc", true), topKey);
        validateUids(iter.getTopValue(), "abc.0", "abc.1", "abc.2", "abc.3", "abc.4");
        iter.next();
        topKey = iter.getTopKey();
        Assert.assertEquals(newKey(SHARD, FIELD_TO_AGGREGATE, "abd"), topKey);
        
    }
    
    @Test
    public void testDeleteFullMajcWithDeepCopy() throws IOException {
        PropogatingIterator iter = new PropogatingIterator();
        Map<String,String> options = Maps.newHashMap();
        
        options.put(PropogatingIterator.AGGREGATOR_DEFAULT, GlobalIndexUidAggregator.class.getCanonicalName());
        
        IteratorEnvironment env = new MockIteratorEnvironment(true);
        
        iter.init(createSourceWithTestData(), options, env);
        iter = iter.deepCopy(env);
        
        iter.seek(new Range(), Collections.emptyList(), false);
        
        assertTrue(iter.hasTop());
        
        Key topKey = iter.getTopKey();
        
        Assert.assertEquals(newKey(SHARD, FIELD_TO_AGGREGATE, "abc", true), topKey);
        validateUids(iter.getTopValue(), "abc.0", "abc.1", "abc.2", "abc.3", "abc.4");
        iter.next();
        topKey = iter.getTopKey();
        Assert.assertEquals(newKey(SHARD, FIELD_TO_AGGREGATE, "abd"), topKey);
        
    }
    
    @Test
    public void testDeleteFullMajcWithDeepCopyThreaded() throws IOException, InterruptedException, ExecutionException {
        SortedMultiMapIterator data = createSourceWithTestData();
        
        final PropogatingIterator iter = new PropogatingIterator();
        Map<String,String> options = Maps.newHashMap();
        
        options.put(PropogatingIterator.AGGREGATOR_DEFAULT, GlobalIndexUidAggregator.class.getCanonicalName());
        
        final IteratorEnvironment env = new MockIteratorEnvironment(true);
        
        iter.init(data, options, env);
        ExecutorService exec = Executors.newFixedThreadPool(10);
        
        List<Future<List<Entry<Key,Value>>>> futures = Lists.newArrayList();
        for (int i = 0; i < 25; i++) {
            futures.add(exec.submit(() -> {
                SortedKeyValueIterator<Key,Value> myitr = iter.deepCopy(env);
                Random rand = new Random();
                LockSupport.parkNanos(rand.nextInt(10));
                
                myitr.seek(new Range(), Collections.emptyList(), false);
                
                List<Entry<Key,Value>> resultList = Lists.newArrayList();
                
                assertTrue(myitr.hasTop());
                
                Key topKey = myitr.getTopKey();
                resultList.add(Maps.immutableEntry(topKey, myitr.getTopValue()));
                myitr.next();
                resultList.add(Maps.immutableEntry(myitr.getTopKey(), myitr.getTopValue()));
                
                return resultList;
            }));
        }
        
        for (Future<List<Entry<Key,Value>>> future : futures) {
            List<Entry<Key,Value>> kvList = future.get();
            
            Assert.assertEquals(2, kvList.size());
            
            Entry<Key,Value> kv1 = kvList.get(0);
            Entry<Key,Value> kv2 = kvList.get(1);
            
            Assert.assertEquals(newKey(SHARD, FIELD_TO_AGGREGATE, "abc", true), kv1.getKey());
            validateUids(kv1.getValue(), "abc.0", "abc.1", "abc.2", "abc.3", "abc.4");
            Assert.assertEquals(newKey(SHARD, FIELD_TO_AGGREGATE, "abd"), kv2.getKey());
        }
        exec.shutdownNow();
        
    }
    
    @Test
    public void testDeepCopyOnDefaultConstructor() throws IOException {
        SortedKeyValueIterator<Key,Value> source = createSourceWithTestData();
        HashMap<String,String> emptyOptions = new HashMap<>();
        MockIteratorEnvironment env = new MockIteratorEnvironment(true);
        
        PropogatingIterator propogatingIterator = new PropogatingIterator();
        propogatingIterator.init(source, emptyOptions, env);
        PropogatingIterator deepCopiedPropogatingIterator = propogatingIterator.deepCopy(env);
        Assert.assertNotNull("PropogatingIterator default constructor failed to create a valid instance.", deepCopiedPropogatingIterator);
    }
    
    @Test
    public void testDeepCopyOnConstructor() throws IOException {
        SortedKeyValueIterator<Key,Value> source = createSourceWithTestData();
        HashMap<String,String> emptyOptions = new HashMap<>();
        MockIteratorEnvironment env = new MockIteratorEnvironment(true);
        
        PropogatingIterator propogatingIterator = new PropogatingIterator(source, null);
        propogatingIterator.init(source, emptyOptions, env);
        PropogatingIterator deepCopiedPropogatingIterator = propogatingIterator.deepCopy(env);
        Assert.assertNotNull("PropogatingIterator constructor failed to create a valid instance.", deepCopiedPropogatingIterator);
    }
    
    @Test
    public void testDefaultConstructor() {
        PropogatingIterator uut = new PropogatingIterator();
        Assert.assertNotNull("PropogatingIterator constructor failed to create a valid instance.", uut);
    }
    
    @Test
    public void testConstructor() throws IOException {
        PropogatingIterator uut = new PropogatingIterator(createSourceWithTestData(), null);
        Assert.assertNotNull("PropogatingIterator constructor failed to create a valid instance.", uut);
    }
}
