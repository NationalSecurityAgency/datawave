package datawave.iterators;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

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

import com.google.common.collect.Maps;
import com.google.common.collect.TreeMultimap;
import com.google.protobuf.InvalidProtocolBufferException;

import datawave.ingest.protobuf.Uid;

public class ZeroUIDCountMappingIteratorTest {
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

    private Uid.List.Builder createValueWithUid(String uid, int count) {
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.setIGNORE(false);
        builder.setCOUNT(count);
        builder.addUID(uid);

        return builder;
    }

    private Uid.List.Builder createValueWithNoUid(int count) {
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.setIGNORE(true);
        builder.setCOUNT(count);

        return builder;
    }

    private Uid.List.Builder createValueWithRemoveUid(String uid, int count) {
        Uid.List.Builder builder = Uid.List.newBuilder();
        builder.setIGNORE(false);
        builder.setCOUNT(count);
        builder.addREMOVEDUID(uid);

        return builder;
    }

    public static class MockIteratorEnvironment implements IteratorEnvironment {
        AccumuloConfiguration conf;
        private final boolean major;
        private final boolean isUser;

        public MockIteratorEnvironment(AccumuloConfiguration conf) {
            this.conf = conf;
            this.isUser = false;
            this.major = false;
        }

        public MockIteratorEnvironment(boolean major) {
            this(major, false);
        }

        public MockIteratorEnvironment(boolean major, boolean isUser) {
            this.conf = DefaultConfiguration.getInstance();
            this.major = major;
            this.isUser = isUser;
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
        public boolean isUserCompaction() {
            return isUser;
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

    protected Key newKey(String row, String field, String uid, boolean delete, int index) {
        Key key = new Key(uid, field, "dataType\0" + row, new ColumnVisibility("PUBLIC"), TIMESTAMP + index);
        key.setDeleted(delete);
        return key;
    }

    protected Key newKey(String row, String field, String uid, int index) {
        return newKey(row, field, uid, false, index);
    }

    @Test
    public void testMapZero() throws IOException {

        TreeMultimap<Key,Value> map = TreeMultimap.create();

        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abc", 3), new Value(createValueWithUid("abc.1", 0).build().toByteArray()));
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abc", 2), new Value(createValueWithRemoveUid("abc.2", 0).build().toByteArray()));
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abc", 1), new Value(createValueWithNoUid(0).build().toByteArray()));

        SortedMultiMapIterator data = new SortedMultiMapIterator(map);

        ZeroUIDCountMappingIterator iter = new ZeroUIDCountMappingIterator();
        Map<String,String> options = Maps.newHashMap();

        IteratorEnvironment env = new MockIteratorEnvironment(false);

        iter.init(data, options, env);

        iter.seek(new Range(), Collections.emptyList(), false);

        assertTrue(iter.hasTop());
        Key topKey = iter.getTopKey();
        Value topValue = iter.getTopValue();
        Assert.assertEquals(newKey(SHARD, FIELD_TO_AGGREGATE, "abc", 3), topKey);
        Assert.assertEquals(new Value(createValueWithUid("abc.1", 1).build().toByteArray()), topValue);

        iter.next();
        assertTrue(iter.hasTop());
        topKey = iter.getTopKey();
        topValue = iter.getTopValue();
        Assert.assertEquals(newKey(SHARD, FIELD_TO_AGGREGATE, "abc", 2), topKey);
        Assert.assertEquals(new Value(createValueWithRemoveUid("abc.2", 1).build().toByteArray()), topValue);

        iter.next();
        assertTrue(iter.hasTop());
        topKey = iter.getTopKey();
        topValue = iter.getTopValue();
        Assert.assertEquals(newKey(SHARD, FIELD_TO_AGGREGATE, "abc", 1), topKey);
        Assert.assertEquals(new Value(createValueWithNoUid(1).build().toByteArray()), topValue);

        iter.next();
        assertFalse(iter.hasTop());
    }

    @Test
    public void testNoChange() throws IOException {

        TreeMultimap<Key,Value> map = TreeMultimap.create();

        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abc", 3), new Value(createValueWithUid("abc.1", 1).build().toByteArray()));
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abc", 2), new Value(createValueWithRemoveUid("abc.2", 2).build().toByteArray()));
        map.put(newKey(SHARD, FIELD_TO_AGGREGATE, "abc", 1), new Value(createValueWithNoUid(3).build().toByteArray()));

        SortedMultiMapIterator data = new SortedMultiMapIterator(map);

        ZeroUIDCountMappingIterator iter = new ZeroUIDCountMappingIterator();
        Map<String,String> options = Maps.newHashMap();

        IteratorEnvironment env = new MockIteratorEnvironment(false);

        iter.init(data, options, env);

        iter.seek(new Range(), Collections.emptyList(), false);

        assertTrue(iter.hasTop());
        Key topKey = iter.getTopKey();
        Value topValue = iter.getTopValue();
        Assert.assertEquals(newKey(SHARD, FIELD_TO_AGGREGATE, "abc", 3), topKey);
        Assert.assertEquals(new Value(createValueWithUid("abc.1", 1).build().toByteArray()), topValue);

        iter.next();
        assertTrue(iter.hasTop());
        topKey = iter.getTopKey();
        topValue = iter.getTopValue();
        Assert.assertEquals(newKey(SHARD, FIELD_TO_AGGREGATE, "abc", 2), topKey);
        Assert.assertEquals(new Value(createValueWithRemoveUid("abc.2", 2).build().toByteArray()), topValue);

        iter.next();
        assertTrue(iter.hasTop());
        topKey = iter.getTopKey();
        topValue = iter.getTopValue();
        Assert.assertEquals(newKey(SHARD, FIELD_TO_AGGREGATE, "abc", 1), topKey);
        Assert.assertEquals(new Value(createValueWithNoUid(3).build().toByteArray()), topValue);

        iter.next();
        assertFalse(iter.hasTop());
    }

}
