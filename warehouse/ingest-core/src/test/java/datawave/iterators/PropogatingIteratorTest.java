package datawave.iterators;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.LockSupport;

import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.conf.ColumnToClassMapping;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.TreeMultimap;
import com.google.protobuf.InvalidProtocolBufferException;

import datawave.ingest.protobuf.Uid;
import datawave.ingest.table.aggregator.GlobalIndexUidAggregator;

/**
 * 
 */
@SuppressWarnings("deprecation")
public class PropogatingIteratorTest {
    protected IteratorScope getIteratorScopeResults = IteratorScope.majc;
    protected boolean isFullMajorCompactionResults = false;
    
    /**
     * @param topValue
     * @param uids
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
     * @param uids
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
    
    protected IteratorEnvironment createMockIteratorEnvironment(boolean activateIteratorScope, int iteratorScopeCallCount, boolean activateIsFullCompaction,
                    int compactionCallCount) {
        IteratorEnvironment mock = PowerMock.createMock(IteratorEnvironment.class);
        EasyMock.expect(mock.getConfig()).andReturn(null);
        if (activateIteratorScope) {
            
            mock.getIteratorScope();
            EasyMock.expectLastCall().andAnswer(() -> getIteratorScopeResults).times(iteratorScopeCallCount);
        }
        
        if (activateIsFullCompaction) {
            
            mock.isFullMajorCompaction();
            EasyMock.expectLastCall().andAnswer(() -> isFullMajorCompactionResults).times(compactionCallCount);
        }
        
        PowerMock.replay(mock);
        
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
        
        iter.seek(new Range(), Collections.emptyList(), false);
        
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
        
        iter.seek(new Range(), Collections.emptyList(), false);
        
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
        
        iter.seek(new Range(), Collections.emptyList(), false);
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
        
        iter.seek(new Range(), Collections.emptyList(), false);
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
        
        iter.seek(new Range(), Collections.emptyList(), false);
        
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
        
        iter.seek(new Range(), Collections.emptyList(), false);
        
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
        
        iter.seek(new Range(), Collections.emptyList(), false);
        
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
        
        iter.seek(new Range(), Collections.emptyList(), false);
        
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
        
        iter.seek(new Range(), Collections.emptyList(), false);
        
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
        
        iter.seek(new Range(), Collections.emptyList(), false);
        
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
        
        iter.seek(new Range(), Collections.emptyList(), false);
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
        
        iter.seek(new Range(), Collections.emptyList(), false);
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
        
        iter.seek(new Range(), Collections.emptyList(), false);
        
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
        
        iter.seek(new Range(), Collections.emptyList(), false);
        
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
        
        iter.seek(new Range(), Collections.emptyList(), false);
        
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
        
        iter.seek(new Range(), Collections.emptyList(), false);
        
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
            futures.add(exec.submit(() -> {
                SortedKeyValueIterator<Key,Value> myitr = iter.deepCopy(env);
                Random rand = new Random();
                LockSupport.parkNanos(rand.nextInt(10));
                
                myitr.seek(new Range(), Collections.emptyList(), false);
                
                List<Entry<Key,Value>> resultList = Lists.newArrayList();
                
                Assert.assertTrue(myitr.hasTop());
                
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
