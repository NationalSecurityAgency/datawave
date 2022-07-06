package datawave.ingest.table.bloomfilter;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;

public class ShardKeyFunctorTest {
    
    public static org.apache.hadoop.util.bloom.Key EMPTY_BF_KEY = new org.apache.hadoop.util.bloom.Key(new byte[0], 1.0);
    protected ShardKeyFunctor functor = null;
    
    @Before
    public void setUp() throws Exception {
        functor = new ShardKeyFunctor();
    }
    
    @After
    public void tearDown() throws Exception {
        functor = null;
    }
    
    @Test
    public void testIsKeyInBloomFilter() {
        // key should only be in bloom filter if it is a field index column (cf = 'fi\x00'...) and
        // contains both the field name (cf) and field value (cq)
        org.apache.accumulo.core.data.Key cbKey = new org.apache.accumulo.core.data.Key();
        Assert.assertFalse("empty key should not be in bloom filter", ShardKeyFunctor.isKeyInBloomFilter(cbKey));
        
        cbKey = new org.apache.accumulo.core.data.Key("row only", "fi\0");
        Assert.assertFalse("row only key should not be in bloom filter", ShardKeyFunctor.isKeyInBloomFilter(cbKey));
        
        cbKey = new org.apache.accumulo.core.data.Key("", "fi\0cf only");
        Assert.assertFalse("cf only key should not be in bloom filter", ShardKeyFunctor.isKeyInBloomFilter(cbKey));
        
        cbKey = new org.apache.accumulo.core.data.Key("", "fi\0", "cq only");
        Assert.assertFalse("cq only key should not be in bloom filter", ShardKeyFunctor.isKeyInBloomFilter(cbKey));
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "fi\0", "and cq");
        Assert.assertFalse("row and cq only key should not be in bloom filter", ShardKeyFunctor.isKeyInBloomFilter(cbKey));
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "fi\0and cf");
        Assert.assertFalse("row and cf only key should not be in bloom filter", ShardKeyFunctor.isKeyInBloomFilter(cbKey));
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "and cf", "and a cq");
        Assert.assertFalse("row and cf (non-fi) and cq key should not be in bloom filter", ShardKeyFunctor.isKeyInBloomFilter(cbKey));
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "fi\0and cf", "and a cq");
        Assert.assertTrue("row and cf and cq key should be in bloom filter", ShardKeyFunctor.isKeyInBloomFilter(cbKey));
        
        cbKey = new org.apache.accumulo.core.data.Key("", "fi\0and cf", "and a cq");
        Assert.assertTrue("cf and cq key should be in bloom filter", ShardKeyFunctor.isKeyInBloomFilter(cbKey));
    }
    
    @Test
    public void testIsRangeInBloomFilter() {
        // key should only be in bloom filter if it is a field index column (cf = 'fi\x00'...) and
        // contains both the field name (cf) and field value (cq)
        org.apache.accumulo.core.data.Key cbKey = new org.apache.accumulo.core.data.Key();
        Assert.assertFalse("empty key should not be in bloom filter", ShardKeyFunctor.isRangeInBloomFilter(new Range(cbKey, cbKey)));
        
        cbKey = new org.apache.accumulo.core.data.Key("row only", "fi\0");
        Assert.assertFalse("row only key should not be in bloom filter", ShardKeyFunctor.isRangeInBloomFilter(new Range(cbKey, cbKey)));
        
        cbKey = new org.apache.accumulo.core.data.Key("", "fi\0cf only");
        Assert.assertFalse("cf only key should not be in bloom filter", ShardKeyFunctor.isRangeInBloomFilter(new Range(cbKey, cbKey)));
        
        cbKey = new org.apache.accumulo.core.data.Key("", "fi\0", "cq only");
        Assert.assertFalse("cq only key should not be in bloom filter", ShardKeyFunctor.isRangeInBloomFilter(new Range(cbKey, cbKey)));
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "fi\0", "and cq");
        Assert.assertFalse("row and cq only key should not be in bloom filter", ShardKeyFunctor.isRangeInBloomFilter(new Range(cbKey, cbKey)));
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "fi\0and a cf");
        Assert.assertFalse("row and cf only key should not be in bloom filter", ShardKeyFunctor.isRangeInBloomFilter(new Range(cbKey, cbKey)));
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "and cf", "and a cq");
        Assert.assertFalse("row and cf (non-fi) and cq key should not be in bloom filter", ShardKeyFunctor.isRangeInBloomFilter(new Range(cbKey, cbKey)));
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "fi\0and cf", "and a cq");
        Assert.assertTrue("row and cf and cq key should be in bloom filter", ShardKeyFunctor.isRangeInBloomFilter(new Range(cbKey, cbKey)));
        
        cbKey = new org.apache.accumulo.core.data.Key("", "fi\0and cf", "and a cq");
        Assert.assertTrue("cf and cq key should be in bloom filter", ShardKeyFunctor.isRangeInBloomFilter(new Range(cbKey, cbKey)));
        
        org.apache.accumulo.core.data.Key cbKey1 = new org.apache.accumulo.core.data.Key("row", "fi\0and a cf", "and a cq");
        org.apache.accumulo.core.data.Key cbKey2 = new org.apache.accumulo.core.data.Key("row", "fi\0and another cf", "and a cq");
        Assert.assertFalse("different keys should not be in bloom filter", ShardKeyFunctor.isRangeInBloomFilter(new Range(cbKey1, cbKey2)));
        
        cbKey1 = new org.apache.accumulo.core.data.Key("row", "fi\0and a cf", "and a cq");
        cbKey2 = cbKey1.followingKey(PartialKey.ROW_COLFAM_COLQUAL);
        Assert.assertFalse("consecutive keys should not be in bloom filter with end inclusive", ShardKeyFunctor.isRangeInBloomFilter(new Range(cbKey1, cbKey2)));
        Assert.assertTrue("consecutive keys should be in bloom filter with end exclusive",
                        ShardKeyFunctor.isRangeInBloomFilter(new Range(cbKey1, true, cbKey2, false)));
    }
    
    @Test
    public void testTransformRange() {
        // key should only be in bloom filter if it is a field index column (cf = 'fi\x00'...) and
        // contains both the field name (cf) and field value (cq)
        org.apache.accumulo.core.data.Key cbKey = new org.apache.accumulo.core.data.Key();
        Assert.assertNull("empty key should not be in bloom filter", functor.transform(new Range(cbKey, cbKey)));
        
        cbKey = new org.apache.accumulo.core.data.Key("row only", "fi\0");
        Assert.assertNull("row only key should not be in bloom filter", functor.transform(new Range(cbKey, cbKey)));
        
        cbKey = new org.apache.accumulo.core.data.Key("", "fi\0cf only");
        Assert.assertNull("cf only key should not be in bloom filter", functor.transform(new Range(cbKey, cbKey)));
        
        cbKey = new org.apache.accumulo.core.data.Key("", "fi\0", "cq only");
        Assert.assertNull("cq only key should not be in bloom filter", functor.transform(new Range(cbKey, cbKey)));
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "fi\0", "and cq");
        Assert.assertNull("row and cq only key should not be in bloom filter", functor.transform(new Range(cbKey, cbKey)));
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "fi\0and a cf");
        Assert.assertNull("row and cf only key should not be in bloom filter", functor.transform(new Range(cbKey, cbKey)));
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "and cf", "and a cq");
        Assert.assertNull("row and cf (non-fi) and cq key should not be in bloom filter", functor.transform(new Range(cbKey, cbKey)));
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "fi\0and cf", "and a cq");
        org.apache.hadoop.util.bloom.Key bfKey = new org.apache.hadoop.util.bloom.Key(new byte[] {'a', 'n', 'd', ' ', 'c', 'f', 'a', 'n', 'd', ' ', 'a', ' ',
                'c', 'q'}, 1.0);
        Assert.assertEquals("row and cf and cq key should be in bloom filter", bfKey, functor.transform(new Range(cbKey, cbKey)));
        
        cbKey = new org.apache.accumulo.core.data.Key("", "fi\0and cf", "and a cq");
        Assert.assertEquals("cf and cq key should be in bloom filter", bfKey, functor.transform(new Range(cbKey, cbKey)));
        
        org.apache.accumulo.core.data.Key cbKey1 = new org.apache.accumulo.core.data.Key("row", "fi\0and a cf", "and a cq");
        org.apache.accumulo.core.data.Key cbKey2 = new org.apache.accumulo.core.data.Key("row", "fi\0and another cf", "and a cq");
        Assert.assertNull("different keys should not be in bloom filter", functor.transform(new Range(cbKey1, cbKey2)));
        
        cbKey1 = new org.apache.accumulo.core.data.Key("row", "fi\0and cf", "and a cq");
        cbKey2 = cbKey1.followingKey(PartialKey.ROW_COLFAM_COLQUAL);
        Assert.assertNull("consecutive keys should not be in bloom filter with end inclusive", functor.transform(new Range(cbKey1, cbKey2)));
        Assert.assertEquals("consecutive keys should be in bloom filter with end exclusive", bfKey, functor.transform(new Range(cbKey1, true, cbKey2, false)));
    }
    
    @Test
    public void testTransformKey() {
        // key should only be in bloom filter if it is a field index column (cf = 'fi\x00'...) and
        // contains both the field name (cf) and field value (cq)
        org.apache.accumulo.core.data.Key cbKey = new org.apache.accumulo.core.data.Key();
        Assert.assertEquals("empty key should not be in bloom filter", EMPTY_BF_KEY, functor.transform(cbKey));
        
        cbKey = new org.apache.accumulo.core.data.Key("row only", "fi\0");
        Assert.assertEquals("row only key should not be in bloom filter", EMPTY_BF_KEY, functor.transform(cbKey));
        
        cbKey = new org.apache.accumulo.core.data.Key("", "fi\0cf only");
        Assert.assertEquals("cf only key should not be in bloom filter", EMPTY_BF_KEY, functor.transform(cbKey));
        
        cbKey = new org.apache.accumulo.core.data.Key("", "fi\0", "cq only");
        Assert.assertEquals("cq only key should not be in bloom filter", EMPTY_BF_KEY, functor.transform(cbKey));
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "fi\0", "and cq");
        Assert.assertEquals("row and cq only key should not be in bloom filter", EMPTY_BF_KEY, functor.transform(cbKey));
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "fi\0and cf");
        Assert.assertEquals("row and cf only key should not be in bloom filter", EMPTY_BF_KEY, functor.transform(cbKey));
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "and cf", "and a cq");
        Assert.assertEquals("row and cf (non-fi) and cq key should not be in bloom filter", EMPTY_BF_KEY, functor.transform(cbKey));
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "fi\0and cf", "and a cq");
        org.apache.hadoop.util.bloom.Key bfKey = new org.apache.hadoop.util.bloom.Key(new byte[] {'a', 'n', 'd', ' ', 'c', 'f', 'a', 'n', 'd', ' ', 'a', ' ',
                'c', 'q'}, 1.0);
        Assert.assertEquals("row and cf and cq key should be in bloom filter", bfKey, functor.transform(cbKey));
        
        cbKey = new org.apache.accumulo.core.data.Key("", "fi\0and cf", "and a cq");
        Assert.assertEquals("cf and cq key should be in bloom filter", bfKey, functor.transform(cbKey));
    }
}
