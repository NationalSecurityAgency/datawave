package datawave.ingest.table.bloomfilter;

import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ShardIndexKeyFunctorTest {
    
    public static org.apache.hadoop.util.bloom.Key EMPTY_BF_KEY = new org.apache.hadoop.util.bloom.Key(new byte[0], 1.0);
    protected ShardIndexKeyFunctor functor = null;
    
    @BeforeEach
    public void setUp() throws Exception {
        functor = new ShardIndexKeyFunctor();
    }
    
    @AfterEach
    public void tearDown() throws Exception {
        functor = null;
    }
    
    @Test
    public void testIsKeyInBloomFilter() {
        // key should only be in bloom filter if it contains both the field name (cf) and field value (row)
        org.apache.accumulo.core.data.Key cbKey = new org.apache.accumulo.core.data.Key();
        Assertions.assertFalse(ShardIndexKeyFunctor.isKeyInBloomFilter(cbKey), "empty key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("row only");
        Assertions.assertFalse(ShardIndexKeyFunctor.isKeyInBloomFilter(cbKey), "row only key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("", "cf only");
        Assertions.assertFalse(ShardIndexKeyFunctor.isKeyInBloomFilter(cbKey), "cf only key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("", "", "cq only");
        Assertions.assertFalse(ShardIndexKeyFunctor.isKeyInBloomFilter(cbKey), "cq only key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "", "and cq");
        Assertions.assertFalse(ShardIndexKeyFunctor.isKeyInBloomFilter(cbKey), "row and cq only key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "and cf");
        Assertions.assertTrue(ShardIndexKeyFunctor.isKeyInBloomFilter(cbKey), "row and cf only key should be in bloom filter");
    }
    
    @Test
    public void testIsRangeInBloomFilter() {
        // key should only be in bloom filter if it contains both the field name (cf) and field value (row)
        org.apache.accumulo.core.data.Key cbKey = new org.apache.accumulo.core.data.Key();
        Assertions.assertFalse(ShardIndexKeyFunctor.isRangeInBloomFilter(new Range(cbKey, cbKey)), "empty key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("row only");
        Assertions.assertFalse(ShardIndexKeyFunctor.isRangeInBloomFilter(new Range(cbKey, cbKey)), "row only key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("", "cf only");
        Assertions.assertFalse(ShardIndexKeyFunctor.isRangeInBloomFilter(new Range(cbKey, cbKey)), "cf only key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("", "", "cq only");
        Assertions.assertFalse(ShardIndexKeyFunctor.isRangeInBloomFilter(new Range(cbKey, cbKey)), "cq only key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "", "and cq");
        Assertions.assertFalse(ShardIndexKeyFunctor.isRangeInBloomFilter(new Range(cbKey, cbKey)), "row and cq only key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "and a cf");
        Assertions.assertTrue(ShardIndexKeyFunctor.isRangeInBloomFilter(new Range(cbKey, cbKey)), "row and cf only key should be in bloom filter");
        
        org.apache.accumulo.core.data.Key cbKey1 = new org.apache.accumulo.core.data.Key("row", "and a cf");
        org.apache.accumulo.core.data.Key cbKey2 = new org.apache.accumulo.core.data.Key("row", "and another cf");
        Assertions.assertFalse(ShardIndexKeyFunctor.isRangeInBloomFilter(new Range(cbKey1, cbKey2)), "different keys should not be in bloom filter");
        
        cbKey1 = new org.apache.accumulo.core.data.Key("row", "and a cf", "and a cq");
        cbKey2 = cbKey1.followingKey(PartialKey.ROW_COLFAM);
        Assertions.assertFalse(ShardIndexKeyFunctor.isRangeInBloomFilter(new Range(cbKey1, cbKey2)),
                        "consecutive keys should not be in bloom filter with end inclusive");
        Assertions.assertTrue(ShardIndexKeyFunctor.isRangeInBloomFilter(new Range(cbKey1, true, cbKey2, false)),
                        "consecutive keys should be in bloom filter with end exclusive");
    }
    
    @Test
    public void testTransformRange() {
        // key should only be in bloom filter if it contains both the field name (cf) and field value (row)
        org.apache.accumulo.core.data.Key cbKey = new org.apache.accumulo.core.data.Key();
        Assertions.assertNull(functor.transform(new Range(cbKey, cbKey)), "empty key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("row only");
        Assertions.assertNull(functor.transform(new Range(cbKey, cbKey)), "row only key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("", "cf only");
        Assertions.assertNull(functor.transform(new Range(cbKey, cbKey)), "cf only key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("", "", "cq only");
        Assertions.assertNull(functor.transform(new Range(cbKey, cbKey)), "cq only key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "", "and cq");
        Assertions.assertNull(functor.transform(new Range(cbKey, cbKey)), "row and cq only key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "and a cf");
        org.apache.hadoop.util.bloom.Key bfKey = new org.apache.hadoop.util.bloom.Key(new byte[] {'a', 'n', 'd', ' ', 'a', ' ', 'c', 'f', 'r', 'o', 'w'}, 1.0);
        Assertions.assertEquals(bfKey, functor.transform(new Range(cbKey, cbKey)), "row and cf only key should be in bloom filter");
        
        org.apache.accumulo.core.data.Key cbKey1 = new org.apache.accumulo.core.data.Key("row", "and a cf");
        org.apache.accumulo.core.data.Key cbKey2 = new org.apache.accumulo.core.data.Key("row", "and another cf");
        Assertions.assertNull(functor.transform(new Range(cbKey1, cbKey2)), "different keys should not be in bloom filter");
        
        cbKey1 = new org.apache.accumulo.core.data.Key("row", "and a cf", "and a cq");
        cbKey2 = cbKey1.followingKey(PartialKey.ROW_COLFAM);
        Assertions.assertNull(functor.transform(new Range(cbKey1, cbKey2)), "consecutive keys should not be in bloom filter with end inclusive");
        Assertions.assertEquals(bfKey, functor.transform(new Range(cbKey1, true, cbKey2, false)),
                        "consecutive keys should be in bloom filter with end exclusive");
    }
    
    @Test
    public void testTransformKey() {
        // key should only be in bloom filter if it contains both the field name (cf) and field value (row)
        org.apache.accumulo.core.data.Key cbKey = new org.apache.accumulo.core.data.Key();
        Assertions.assertEquals(EMPTY_BF_KEY, functor.transform(cbKey), "empty key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("row only");
        Assertions.assertEquals(EMPTY_BF_KEY, functor.transform(cbKey), "row only key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("", "cf only");
        Assertions.assertEquals(EMPTY_BF_KEY, functor.transform(cbKey), "cf only key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("", "", "cq only");
        Assertions.assertEquals(EMPTY_BF_KEY, functor.transform(cbKey), "cq only key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "", "and cq");
        Assertions.assertEquals(EMPTY_BF_KEY, functor.transform(cbKey), "row and cq only key should not be in bloom filter");
        
        cbKey = new org.apache.accumulo.core.data.Key("row", "and cf");
        org.apache.hadoop.util.bloom.Key bfKey = new org.apache.hadoop.util.bloom.Key(new byte[] {'a', 'n', 'd', ' ', 'c', 'f', 'r', 'o', 'w'}, 1.0);
        Assertions.assertEquals(bfKey, functor.transform(cbKey), "row and cf only key should be in bloom filter");
    }
}
