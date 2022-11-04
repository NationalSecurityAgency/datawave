package datawave.ingest.table.bloomfilter;

import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ShardKeyFunctorTest {
    
    public static org.apache.hadoop.util.bloom.Key EMPTY_BF_KEY = new org.apache.hadoop.util.bloom.Key(new byte[0], 1.0);
    protected ShardKeyFunctor functor;
    
    @BeforeEach
    public void setUp() throws Exception {
        functor = new ShardKeyFunctor();
    }
    
    @AfterEach
    public void tearDown() throws Exception {
        functor = null;
    }
    
    @Test
    public void testIsKeyInBloomFilter() {
        // key should only be in bloom filter if it is a field index column (cf = 'fi\x00'...) and
        // contains both the field name (cf) and field value (cq)
        org.apache.accumulo.core.data.Key cbKey = new org.apache.accumulo.core.data.Key();
        Assertions.assertFalse(ShardKeyFunctor.isKeyInBloomFilter(cbKey), "empty key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("row only", "fi\0");
        Assertions.assertFalse(ShardKeyFunctor.isKeyInBloomFilter(cbKey), "row only key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("", "fi\0cf only");
        Assertions.assertFalse(ShardKeyFunctor.isKeyInBloomFilter(cbKey), "cf only key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("", "fi\0", "cq only");
        Assertions.assertFalse(ShardKeyFunctor.isKeyInBloomFilter(cbKey), "cq only key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "fi\0", "and cq");
        Assertions.assertFalse(ShardKeyFunctor.isKeyInBloomFilter(cbKey), "row and cq only key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "fi\0and cf");
        Assertions.assertFalse(ShardKeyFunctor.isKeyInBloomFilter(cbKey), "row and cf only key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "and cf", "and a cq");
        Assertions.assertFalse(ShardKeyFunctor.isKeyInBloomFilter(cbKey), "row and cf (non-fi) and cq key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "fi\0and cf", "and a cq");
        Assertions.assertTrue(ShardKeyFunctor.isKeyInBloomFilter(cbKey), "row and cf and cq key should be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("", "fi\0and cf", "and a cq");
        Assertions.assertTrue(ShardKeyFunctor.isKeyInBloomFilter(cbKey), "cf and cq key should be in bloom filter");
    }
    
    @Test
    public void testIsRangeInBloomFilter() {
        // key should only be in bloom filter if it is a field index column (cf = 'fi\x00'...) and
        // contains both the field name (cf) and field value (cq)
        org.apache.accumulo.core.data.Key cbKey = new org.apache.accumulo.core.data.Key();
        Assertions.assertFalse(ShardKeyFunctor.isRangeInBloomFilter(new Range(cbKey, cbKey)), "empty key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("row only", "fi\0");
        Assertions.assertFalse(ShardKeyFunctor.isRangeInBloomFilter(new Range(cbKey, cbKey)), "row only key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("", "fi\0cf only");
        Assertions.assertFalse(ShardKeyFunctor.isRangeInBloomFilter(new Range(cbKey, cbKey)), "cf only key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("", "fi\0", "cq only");
        Assertions.assertFalse(ShardKeyFunctor.isRangeInBloomFilter(new Range(cbKey, cbKey)), "cq only key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "fi\0", "and cq");
        Assertions.assertFalse(ShardKeyFunctor.isRangeInBloomFilter(new Range(cbKey, cbKey)), "row and cq only key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "fi\0and a cf");
        Assertions.assertFalse(ShardKeyFunctor.isRangeInBloomFilter(new Range(cbKey, cbKey)), "row and cf only key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "and cf", "and a cq");
        Assertions.assertFalse(ShardKeyFunctor.isRangeInBloomFilter(new Range(cbKey, cbKey)), "row and cf (non-fi) and cq key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "fi\0and cf", "and a cq");
        Assertions.assertTrue(ShardKeyFunctor.isRangeInBloomFilter(new Range(cbKey, cbKey)), "row and cf and cq key should be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("", "fi\0and cf", "and a cq");
        Assertions.assertTrue(ShardKeyFunctor.isRangeInBloomFilter(new Range(cbKey, cbKey)), "cf and cq key should be in bloom filter");
        
        org.apache.accumulo.core.data.Key cbKey1 = new org.apache.accumulo.core.data.Key("row", "fi\0and a cf", "and a cq");
        org.apache.accumulo.core.data.Key cbKey2 = new org.apache.accumulo.core.data.Key("row", "fi\0and another cf", "and a cq");
        Assertions.assertFalse(ShardKeyFunctor.isRangeInBloomFilter(new Range(cbKey1, cbKey2)), "different keys should not be in bloom filter");
        
        cbKey1 = new org.apache.accumulo.core.data.Key("row", "fi\0and a cf", "and a cq");
        cbKey2 = cbKey1.followingKey(PartialKey.ROW_COLFAM_COLQUAL);
        Assertions.assertFalse(ShardKeyFunctor.isRangeInBloomFilter(new Range(cbKey1, cbKey2)),
                        "consecutive keys should not be in bloom filter with end inclusive");
        Assertions.assertTrue(ShardKeyFunctor.isRangeInBloomFilter(new Range(cbKey1, true, cbKey2, false)),
                        "consecutive keys should be in bloom filter with end exclusive");
    }
    
    @Test
    public void testTransformRange() {
        // key should only be in bloom filter if it is a field index column (cf = 'fi\x00'...) and
        // contains both the field name (cf) and field value (cq)
        org.apache.accumulo.core.data.Key cbKey = new org.apache.accumulo.core.data.Key();
        Assertions.assertNull(functor.transform(new Range(cbKey, cbKey)), "empty key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("row only", "fi\0");
        Assertions.assertNull(functor.transform(new Range(cbKey, cbKey)), "row only key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("", "fi\0cf only");
        Assertions.assertNull(functor.transform(new Range(cbKey, cbKey)), "cf only key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("", "fi\0", "cq only");
        Assertions.assertNull(functor.transform(new Range(cbKey, cbKey)), "cq only key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "fi\0", "and cq");
        Assertions.assertNull(functor.transform(new Range(cbKey, cbKey)), "row and cq only key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "fi\0and a cf");
        Assertions.assertNull(functor.transform(new Range(cbKey, cbKey)), "row and cf only key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "and cf", "and a cq");
        Assertions.assertNull(functor.transform(new Range(cbKey, cbKey)), "row and cf (non-fi) and cq key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "fi\0and cf", "and a cq");
        org.apache.hadoop.util.bloom.Key bfKey = new org.apache.hadoop.util.bloom.Key(new byte[] {'a', 'n', 'd', ' ', 'c', 'f', 'a', 'n', 'd', ' ', 'a', ' ',
                'c', 'q'}, 1.0);
        Assertions.assertEquals(bfKey, functor.transform(new Range(cbKey, cbKey)), "row and cf and cq key should be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("", "fi\0and cf", "and a cq");
        Assertions.assertEquals(bfKey, functor.transform(new Range(cbKey, cbKey)), "cf and cq key should be in bloom filter");
        
        org.apache.accumulo.core.data.Key cbKey1 = new org.apache.accumulo.core.data.Key("row", "fi\0and a cf", "and a cq");
        org.apache.accumulo.core.data.Key cbKey2 = new org.apache.accumulo.core.data.Key("row", "fi\0and another cf", "and a cq");
        Assertions.assertNull(functor.transform(new Range(cbKey1, cbKey2)), "different keys should not be in bloom filter");
        
        cbKey1 = new org.apache.accumulo.core.data.Key("row", "fi\0and cf", "and a cq");
        cbKey2 = cbKey1.followingKey(PartialKey.ROW_COLFAM_COLQUAL);
        Assertions.assertNull(functor.transform(new Range(cbKey1, cbKey2)), "consecutive keys should not be in bloom filter with end inclusive");
        Assertions.assertEquals(bfKey, functor.transform(new Range(cbKey1, true, cbKey2, false)),
                        "consecutive keys should be in bloom filter with end exclusive");
    }
    
    @Test
    public void testTransformKey() {
        // key should only be in bloom filter if it is a field index column (cf = 'fi\x00'...) and
        // contains both the field name (cf) and field value (cq)
        org.apache.accumulo.core.data.Key cbKey = new org.apache.accumulo.core.data.Key();
        Assertions.assertEquals(EMPTY_BF_KEY, functor.transform(cbKey), "empty key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("row only", "fi\0");
        Assertions.assertEquals(EMPTY_BF_KEY, functor.transform(cbKey), "row only key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("", "fi\0cf only");
        Assertions.assertEquals(EMPTY_BF_KEY, functor.transform(cbKey), "cf only key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("", "fi\0", "cq only");
        Assertions.assertEquals(EMPTY_BF_KEY, functor.transform(cbKey), "cq only key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "fi\0", "and cq");
        Assertions.assertEquals(EMPTY_BF_KEY, functor.transform(cbKey), "row and cq only key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "fi\0and cf");
        Assertions.assertEquals(EMPTY_BF_KEY, functor.transform(cbKey), "row and cf only key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "and cf", "and a cq");
        Assertions.assertEquals(EMPTY_BF_KEY, functor.transform(cbKey), "row and cf (non-fi) and cq key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "fi\0and cf", "and a cq");
        org.apache.hadoop.util.bloom.Key bfKey = new org.apache.hadoop.util.bloom.Key(new byte[] {'a', 'n', 'd', ' ', 'c', 'f', 'a', 'n', 'd', ' ', 'a', ' ',
                'c', 'q'}, 1.0);
        Assertions.assertEquals(bfKey, functor.transform(cbKey), "row and cf and cq key should be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("", "fi\0and cf", "and a cq");
        Assertions.assertEquals(bfKey, functor.transform(cbKey), "cf and cq key should be in bloom filter");
    }
}
