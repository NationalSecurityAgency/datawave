package datawave.ingest.table.bloomfilter;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;

public class ShardIndexKeyFunctorTest {
    
    public static org.apache.hadoop.util.bloom.Key EMPTY_BF_KEY = new org.apache.hadoop.util.bloom.Key(new byte[0], 1.0);
    protected ShardIndexKeyFunctor functor = null;
    
    @Before
    public void setUp() throws Exception {
        functor = new ShardIndexKeyFunctor();
    }
    
    @After
    public void tearDown() throws Exception {
        functor = null;
    }
    
    @Test
    public void testIsKeyInBloomFilter() {
        // key should only be in bloom filter if it contains both the field name (cf) and field value (row)
        org.apache.accumulo.core.data.Key cbKey = new org.apache.accumulo.core.data.Key();
        Assert.assertFalse("empty key should not be in bloom filter", ShardIndexKeyFunctor.isKeyInBloomFilter(cbKey));
        
        cbKey = new org.apache.accumulo.core.data.Key("row only");
        Assert.assertFalse("row only key should not be in bloom filter", ShardIndexKeyFunctor.isKeyInBloomFilter(cbKey));
        
        cbKey = new org.apache.accumulo.core.data.Key("", "cf only");
        Assert.assertFalse("cf only key should not be in bloom filter", ShardIndexKeyFunctor.isKeyInBloomFilter(cbKey));
        
        cbKey = new org.apache.accumulo.core.data.Key("", "", "cq only");
        Assert.assertFalse("cq only key should not be in bloom filter", ShardIndexKeyFunctor.isKeyInBloomFilter(cbKey));
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "", "and cq");
        Assert.assertFalse("row and cq only key should not be in bloom filter", ShardIndexKeyFunctor.isKeyInBloomFilter(cbKey));
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "and cf");
        Assert.assertTrue("row and cf only key should be in bloom filter", ShardIndexKeyFunctor.isKeyInBloomFilter(cbKey));
    }
    
    @Test
    public void testIsRangeInBloomFilter() {
        // key should only be in bloom filter if it contains both the field name (cf) and field value (row)
        org.apache.accumulo.core.data.Key cbKey = new org.apache.accumulo.core.data.Key();
        Assert.assertFalse("empty key should not be in bloom filter", ShardIndexKeyFunctor.isRangeInBloomFilter(new Range(cbKey, cbKey)));
        
        cbKey = new org.apache.accumulo.core.data.Key("row only");
        Assert.assertFalse("row only key should not be in bloom filter", ShardIndexKeyFunctor.isRangeInBloomFilter(new Range(cbKey, cbKey)));
        
        cbKey = new org.apache.accumulo.core.data.Key("", "cf only");
        Assert.assertFalse("cf only key should not be in bloom filter", ShardIndexKeyFunctor.isRangeInBloomFilter(new Range(cbKey, cbKey)));
        
        cbKey = new org.apache.accumulo.core.data.Key("", "", "cq only");
        Assert.assertFalse("cq only key should not be in bloom filter", ShardIndexKeyFunctor.isRangeInBloomFilter(new Range(cbKey, cbKey)));
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "", "and cq");
        Assert.assertFalse("row and cq only key should not be in bloom filter", ShardIndexKeyFunctor.isRangeInBloomFilter(new Range(cbKey, cbKey)));
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "and a cf");
        Assert.assertTrue("row and cf only key should be in bloom filter", ShardIndexKeyFunctor.isRangeInBloomFilter(new Range(cbKey, cbKey)));
        
        org.apache.accumulo.core.data.Key cbKey1 = new org.apache.accumulo.core.data.Key("row", "and a cf");
        org.apache.accumulo.core.data.Key cbKey2 = new org.apache.accumulo.core.data.Key("row", "and another cf");
        Assert.assertFalse("different keys should not be in bloom filter", ShardIndexKeyFunctor.isRangeInBloomFilter(new Range(cbKey1, cbKey2)));
        
        cbKey1 = new org.apache.accumulo.core.data.Key("row", "and a cf", "and a cq");
        cbKey2 = cbKey1.followingKey(PartialKey.ROW_COLFAM);
        Assert.assertFalse("consecutive keys should not be in bloom filter with end inclusive",
                        ShardIndexKeyFunctor.isRangeInBloomFilter(new Range(cbKey1, cbKey2)));
        Assert.assertTrue("consecutive keys should be in bloom filter with end exclusive",
                        ShardIndexKeyFunctor.isRangeInBloomFilter(new Range(cbKey1, true, cbKey2, false)));
    }
    
    @Test
    public void testTransformRange() {
        // key should only be in bloom filter if it contains both the field name (cf) and field value (row)
        org.apache.accumulo.core.data.Key cbKey = new org.apache.accumulo.core.data.Key();
        Assert.assertNull("empty key should not be in bloom filter", functor.transform(new Range(cbKey, cbKey)));
        
        cbKey = new org.apache.accumulo.core.data.Key("row only");
        Assert.assertNull("row only key should not be in bloom filter", functor.transform(new Range(cbKey, cbKey)));
        
        cbKey = new org.apache.accumulo.core.data.Key("", "cf only");
        Assert.assertNull("cf only key should not be in bloom filter", functor.transform(new Range(cbKey, cbKey)));
        
        cbKey = new org.apache.accumulo.core.data.Key("", "", "cq only");
        Assert.assertNull("cq only key should not be in bloom filter", functor.transform(new Range(cbKey, cbKey)));
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "", "and cq");
        Assert.assertNull("row and cq only key should not be in bloom filter", functor.transform(new Range(cbKey, cbKey)));
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "and a cf");
        org.apache.hadoop.util.bloom.Key bfKey = new org.apache.hadoop.util.bloom.Key(new byte[] {'a', 'n', 'd', ' ', 'a', ' ', 'c', 'f', 'r', 'o', 'w'}, 1.0);
        Assert.assertEquals("row and cf only key should be in bloom filter", bfKey, functor.transform(new Range(cbKey, cbKey)));
        
        org.apache.accumulo.core.data.Key cbKey1 = new org.apache.accumulo.core.data.Key("row", "and a cf");
        org.apache.accumulo.core.data.Key cbKey2 = new org.apache.accumulo.core.data.Key("row", "and another cf");
        Assert.assertNull("different keys should not be in bloom filter", functor.transform(new Range(cbKey1, cbKey2)));
        
        cbKey1 = new org.apache.accumulo.core.data.Key("row", "and a cf", "and a cq");
        cbKey2 = cbKey1.followingKey(PartialKey.ROW_COLFAM);
        Assert.assertNull("consecutive keys should not be in bloom filter with end inclusive", functor.transform(new Range(cbKey1, cbKey2)));
        Assert.assertEquals("consecutive keys should be in bloom filter with end exclusive", bfKey, functor.transform(new Range(cbKey1, true, cbKey2, false)));
    }
    
    @Test
    public void testTransformKey() {
        // key should only be in bloom filter if it contains both the field name (cf) and field value (row)
        org.apache.accumulo.core.data.Key cbKey = new org.apache.accumulo.core.data.Key();
        Assert.assertEquals("empty key should not be in bloom filter", EMPTY_BF_KEY, functor.transform(cbKey));
        
        cbKey = new org.apache.accumulo.core.data.Key("row only");
        Assert.assertEquals("row only key should not be in bloom filter", EMPTY_BF_KEY, functor.transform(cbKey));
        
        cbKey = new org.apache.accumulo.core.data.Key("", "cf only");
        Assert.assertEquals("cf only key should not be in bloom filter", EMPTY_BF_KEY, functor.transform(cbKey));
        
        cbKey = new org.apache.accumulo.core.data.Key("", "", "cq only");
        Assert.assertEquals("cq only key should not be in bloom filter", EMPTY_BF_KEY, functor.transform(cbKey));
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "", "and cq");
        Assert.assertEquals("row and cq only key should not be in bloom filter", EMPTY_BF_KEY, functor.transform(cbKey));
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "and cf");
        org.apache.hadoop.util.bloom.Key bfKey = new org.apache.hadoop.util.bloom.Key(new byte[] {'a', 'n', 'd', ' ', 'c', 'f', 'r', 'o', 'w'}, 1.0);
        Assert.assertEquals("row and cf only key should be in bloom filter", bfKey, functor.transform(cbKey));
    }
}
