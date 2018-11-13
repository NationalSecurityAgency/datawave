package datawave.core.iterators;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 */
public class RangeSplitterTest {
    
    Logger log = Logger.getLogger(RangeSplitterTest.class);
    
    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {}
    
    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {}
    
    /**
     * Test method for {@link datawave.core.iterators.RangeSplitter#RangeSplitter(org.apache.accumulo.core.data.Range, int)}.
     */
    @Test
    public void testRangeSplitterCq1() {
        Key start = new Key("row", "cf", "start");
        Key end = new Key("row", "cf", "zend of the sequence");
        Range r = new Range(start, true, end, false);
        RangeSplitter splitter = new RangeSplitter(r, 10);
        Range lastRange = null;
        int count = 0;
        for (Range range : splitter) {
            count++;
            Assert.assertTrue(range.isStartKeyInclusive());
            Assert.assertFalse(range.isEndKeyInclusive());
            if (lastRange != null) {
                Assert.assertEquals(lastRange.getEndKey(), range.getStartKey());
            } else {
                Assert.assertEquals(start, range.getStartKey());
            }
            lastRange = range;
        }
        Assert.assertEquals(end, lastRange.getEndKey());
        Assert.assertEquals(10, count);
    }
    
    /**
     * Test method for {@link datawave.core.iterators.RangeSplitter#RangeSplitter(org.apache.accumulo.core.data.Range, int)}.
     */
    @Test
    public void testRangeSplitterCq2() {
        Key start = new Key("row", "cf", "start of the sequence");
        Key end = new Key("row", "cf", "zend");
        Range r = new Range(start, true, end, false);
        RangeSplitter splitter = new RangeSplitter(r, 10);
        Range lastRange = null;
        int count = 0;
        log.trace("Splitting " + r);
        for (Range range : splitter) {
            log.trace(range);
            count++;
            Assert.assertTrue(range.isStartKeyInclusive());
            Assert.assertFalse(range.isEndKeyInclusive());
            if (lastRange != null) {
                Assert.assertEquals(lastRange.getEndKey(), range.getStartKey());
            } else {
                Assert.assertEquals(start, range.getStartKey());
            }
            lastRange = range;
        }
        Assert.assertEquals(end, lastRange.getEndKey());
        Assert.assertEquals(10, count);
    }
    
    /**
     * Test method for {@link datawave.core.iterators.RangeSplitter#RangeSplitter(org.apache.accumulo.core.data.Range, int)}.
     */
    @Test
    public void testRangeSplitterCf() {
        Key start = new Key("row", "start", "cq");
        Key end = new Key("row", "zend", "cq");
        Range r = new Range(start, true, end, false);
        RangeSplitter splitter = new RangeSplitter(r, 10);
        Range lastRange = null;
        int count = 0;
        log.trace("Splitting " + r);
        for (Range range : splitter) {
            log.trace(range);
            count++;
            Assert.assertTrue(range.isStartKeyInclusive());
            Assert.assertFalse(range.isEndKeyInclusive());
            if (lastRange != null) {
                Assert.assertEquals(lastRange.getEndKey(), range.getStartKey());
            } else {
                Assert.assertEquals(start, range.getStartKey());
            }
            lastRange = range;
        }
        Assert.assertEquals(end, lastRange.getEndKey());
        Assert.assertEquals(10, count);
    }
    
    /**
     * Test method for {@link datawave.core.iterators.RangeSplitter#RangeSplitter(org.apache.accumulo.core.data.Range, int)}.
     */
    @Test
    public void testRangeSplitterRow() {
        Key start = new Key("start", "cf", "cq");
        Key end = new Key("zend", "cf", "cq");
        Range r = new Range(start, true, end, false);
        RangeSplitter splitter = new RangeSplitter(r, 10);
        Range lastRange = null;
        int count = 0;
        log.trace("Splitting " + r);
        for (Range range : splitter) {
            log.trace(range);
            count++;
            Assert.assertTrue(range.isStartKeyInclusive());
            Assert.assertFalse(range.isEndKeyInclusive());
            if (lastRange != null) {
                Assert.assertEquals(lastRange.getEndKey(), range.getStartKey());
            } else {
                Assert.assertEquals(start, range.getStartKey());
            }
            lastRange = range;
        }
        Assert.assertEquals(end, lastRange.getEndKey());
        Assert.assertEquals(10, count);
    }
    
    /**
     * Test method for {@link datawave.core.iterators.RangeSplitter#RangeSplitter(org.apache.accumulo.core.data.Range, int)}.
     */
    @Test
    public void testRangeSplitterAll() {
        Key start = new Key("start", "start", "start");
        Key end = new Key("zend", "end", "end");
        Range r = new Range(start, true, end, false);
        RangeSplitter splitter = new RangeSplitter(r, 10);
        Range lastRange = null;
        int count = 0;
        log.trace("Splitting " + r);
        for (Range range : splitter) {
            log.trace(range);
            count++;
            Assert.assertTrue(range.isStartKeyInclusive());
            Assert.assertFalse(range.isEndKeyInclusive());
            if (lastRange != null) {
                Assert.assertEquals(lastRange.getEndKey(), range.getStartKey());
            } else {
                Assert.assertEquals(start, range.getStartKey());
            }
            lastRange = range;
        }
        Assert.assertEquals(end, lastRange.getEndKey());
        Assert.assertEquals(10, count);
    }
    
    /**
     * Test method for {@link datawave.core.iterators.RangeSplitter#RangeSplitter(org.apache.accumulo.core.data.Range, int)}.
     */
    @Test
    public void testRangeSplitterNegativeBytes() {
        Key start = new Key("row", "cf", "¢start");
        Key end = new Key("row", "cf", "¶zend of the sequence");
        Range r = new Range(start, true, end, false);
        RangeSplitter splitter = new RangeSplitter(r, 10);
        Range lastRange = null;
        int count = 0;
        log.trace("Splitting " + r);
        for (Range range : splitter) {
            log.trace(range);
            count++;
            Assert.assertTrue(range.isStartKeyInclusive());
            Assert.assertFalse(range.isEndKeyInclusive());
            if (lastRange != null) {
                Assert.assertEquals(lastRange.getEndKey(), range.getStartKey());
            } else {
                Assert.assertEquals(start, range.getStartKey());
            }
            lastRange = range;
        }
        Assert.assertEquals(end, lastRange.getEndKey());
        Assert.assertEquals(10, count);
    }
    
    /**
     * Test method for {@link datawave.core.iterators.RangeSplitter#RangeSplitter(org.apache.accumulo.core.data.Range, int)}.
     */
    @Test
    public void testRangeSplitterSmall() {
        Key start = new Key("row", "cf", "a");
        Key end = new Key("row", "cf", "b");
        Range r = new Range(start, true, end, false);
        RangeSplitter splitter = new RangeSplitter(r, 10);
        Range lastRange = null;
        int count = 0;
        log.trace("Splitting " + r);
        for (Range range : splitter) {
            log.trace(range);
            count++;
            Assert.assertTrue(range.isStartKeyInclusive());
            Assert.assertFalse(range.isEndKeyInclusive());
            if (lastRange != null) {
                Assert.assertEquals(lastRange.getEndKey(), range.getStartKey());
            } else {
                Assert.assertEquals(start, range.getStartKey());
            }
            lastRange = range;
        }
        Assert.assertEquals(end, lastRange.getEndKey());
        Assert.assertEquals(10, count);
    }
    
    /**
     * Test method for {@link datawave.core.iterators.RangeSplitter#RangeSplitter(org.apache.accumulo.core.data.Range, int)}.
     */
    @Test
    public void testRangeSplitterNone() {
        Key start = new Key("row", "cf", "a");
        Key end = new Key("row", "cf", "a");
        Range r = new Range(start, true, end, false);
        RangeSplitter splitter = new RangeSplitter(r, 10);
        Range lastRange = null;
        int count = 0;
        log.trace("Splitting " + r);
        for (Range range : splitter) {
            log.trace(range);
            count++;
            Assert.assertTrue(range.isStartKeyInclusive());
            Assert.assertFalse(range.isEndKeyInclusive());
            if (lastRange != null) {
                Assert.assertEquals(lastRange.getEndKey(), range.getStartKey());
            } else {
                Assert.assertEquals(start, range.getStartKey());
            }
            lastRange = range;
        }
        Assert.assertEquals(end, lastRange.getEndKey());
        Assert.assertEquals(1, count);
    }
    
    /**
     * Test method for {@link datawave.core.iterators.RangeSplitter#RangeSplitter(org.apache.accumulo.core.data.Range, int)}.
     */
    @Test
    public void testRangeSplitterTrailingNullBytes() {
        Key start = new Key("row", "cf\u0000\u0000", "a");
        Key end = new Key("row\u0000", "cf", "a\u0000\u0000\u0000");
        Range r = new Range(start, true, end, false);
        RangeSplitter splitter = new RangeSplitter(r, 10);
        Range lastRange = null;
        int count = 0;
        log.trace("Splitting " + r);
        for (Range range : splitter) {
            log.trace(range);
            count++;
            Assert.assertTrue(range.isStartKeyInclusive());
            Assert.assertFalse(range.isEndKeyInclusive());
            if (lastRange != null) {
                Assert.assertEquals(lastRange.getEndKey(), range.getStartKey());
            } else {
                Assert.assertEquals(start, range.getStartKey());
            }
            lastRange = range;
        }
        Assert.assertEquals(end, lastRange.getEndKey());
        Assert.assertEquals(1, count);
    }
    
    /**
     * Test method for {@link datawave.core.iterators.RangeSplitter#RangeSplitter(org.apache.accumulo.core.data.Range, int)}.
     */
    @Test
    public void testRangeSplitterNullEndKey() {
        Key start = new Key("row", "cf", "a");
        Key end = null;
        Range r = new Range(start, true, end, false);
        RangeSplitter splitter = new RangeSplitter(r, 10);
        Range lastRange = null;
        int count = 0;
        log.trace("Splitting " + r);
        for (Range range : splitter) {
            log.trace(range);
            count++;
            Assert.assertTrue(range.isStartKeyInclusive());
            Assert.assertFalse(range.isEndKeyInclusive());
            if (lastRange != null) {
                Assert.assertEquals(lastRange.getEndKey(), range.getStartKey());
            } else {
                Assert.assertEquals(start, range.getStartKey());
            }
            lastRange = range;
        }
        Assert.assertNull(lastRange.getEndKey());
        Assert.assertEquals(10, count);
    }
    
    /**
     * Test method for {@link datawave.core.iterators.RangeSplitter#RangeSplitter(org.apache.accumulo.core.data.Range, int)}.
     */
    @Test
    public void testRangeSplitterNullStartKey() {
        Key start = null;
        Key end = new Key("row", "cf", "a");
        Range r = new Range(start, true, end, false);
        RangeSplitter splitter = new RangeSplitter(r, 10);
        Range lastRange = null;
        int count = 0;
        log.trace("Splitting " + r);
        for (Range range : splitter) {
            log.trace(range);
            count++;
            Assert.assertTrue(range.isStartKeyInclusive());
            Assert.assertFalse(range.isEndKeyInclusive());
            if (lastRange != null) {
                Assert.assertEquals(lastRange.getEndKey(), range.getStartKey());
            } else {
                Assert.assertNull(range.getStartKey());
            }
            lastRange = range;
        }
        Assert.assertEquals(end, lastRange.getEndKey());
        Assert.assertEquals(10, count);
    }
    
    /**
     * Test method for {@link datawave.core.iterators.RangeSplitter#RangeSplitter(org.apache.accumulo.core.data.Range, int)}.
     */
    @Test
    public void testRangeSplitterEmptyStartElements() {
        Key start = new Key("row", "", "");
        Key end = new Key("row", "cf", "a");
        Range r = new Range(start, true, end, false);
        RangeSplitter splitter = new RangeSplitter(r, 10);
        Range lastRange = null;
        int count = 0;
        log.trace("Splitting " + r);
        for (Range range : splitter) {
            log.trace(range);
            count++;
            Assert.assertTrue(range.isStartKeyInclusive());
            Assert.assertFalse(range.isEndKeyInclusive());
            if (lastRange != null) {
                Assert.assertEquals(lastRange.getEndKey(), range.getStartKey());
            } else {
                Assert.assertEquals(start, range.getStartKey());
            }
            lastRange = range;
        }
        Assert.assertEquals(end, lastRange.getEndKey());
        Assert.assertEquals(10, count);
    }
    
    /**
     * Test method for {@link datawave.core.iterators.RangeSplitter#RangeSplitter(org.apache.accumulo.core.data.Range, int)}.
     */
    @Test
    public void testRangeSplitterEmptyEndElements() {
        Key start = new Key("row", "cf", "a");
        Key end = start.followingKey(PartialKey.ROW);
        Range r = new Range(start, true, end, false);
        RangeSplitter splitter = new RangeSplitter(r, 10);
        Range lastRange = null;
        int count = 0;
        log.trace("Splitting " + r);
        for (Range range : splitter) {
            log.trace(range);
            count++;
            Assert.assertTrue(range.isStartKeyInclusive());
            Assert.assertFalse(range.isEndKeyInclusive());
            if (lastRange != null) {
                Assert.assertEquals(lastRange.getEndKey(), range.getStartKey());
            } else {
                Assert.assertEquals(start, range.getStartKey());
            }
            lastRange = range;
        }
        Assert.assertEquals(end, lastRange.getEndKey());
        Assert.assertEquals(10, count);
    }
    
}
