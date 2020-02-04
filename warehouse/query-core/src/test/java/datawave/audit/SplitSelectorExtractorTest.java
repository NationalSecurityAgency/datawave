package datawave.audit;

import com.google.common.collect.Lists;
import java.util.List;

import datawave.webservice.query.QueryImpl;
import org.apache.commons.lang.math.IntRange;
import org.junit.Assert;
import org.junit.Test;

public class SplitSelectorExtractorTest {
    
    @Test
    public void extractSelectorsLuceneQuery1() {
        
        SplitSelectorExtractor extractor = new SplitSelectorExtractor();
        QueryImpl q = new QueryImpl();
        q.setQuery("selector1");
        List<String> selectorList = extractor.extractSelectors(q);
        List<String> expected = Lists.newArrayList("selector1");
        Assert.assertEquals(expected, selectorList);
    }
    
    @Test
    public void extractSelectorsLuceneQuery2() {
        
        SplitSelectorExtractor extractor = new SplitSelectorExtractor();
        extractor.setSeparatorCharacter(";");
        QueryImpl q = new QueryImpl();
        q.setQuery("selector1;selector2;selector3");
        List<String> selectorList = extractor.extractSelectors(q);
        List<String> expected = Lists.newArrayList("selector1", "selector2", "selector3");
        Assert.assertEquals(expected, selectorList);
    }
    
    @Test
    public void extractSelectorsLuceneQuery3() {
        
        SplitSelectorExtractor extractor = new SplitSelectorExtractor();
        extractor.setSeparatorCharacter("\0");
        QueryImpl q = new QueryImpl();
        q.setQuery("selector1\0selector2\0selector3");
        List<String> selectorList = extractor.extractSelectors(q);
        List<String> expected = Lists.newArrayList("selector1", "selector2", "selector3");
        Assert.assertEquals(expected, selectorList);
    }
    
    @Test
    public void extractSelectorsLuceneQuery4() {
        
        SplitSelectorExtractor extractor = new SplitSelectorExtractor();
        extractor.setSeparatorParameter("delimiter");
        QueryImpl q = new QueryImpl();
        q.addParameter("delimiter", ",");
        q.setQuery("selector1,selector2,selector3");
        List<String> selectorList = extractor.extractSelectors(q);
        List<String> expected = Lists.newArrayList("selector1", "selector2", "selector3");
        Assert.assertEquals(expected, selectorList);
    }
    
    @Test
    public void rangeTest1() {
        SplitSelectorExtractor extractor = new SplitSelectorExtractor();
        List<IntRange> useSplitRanges = extractor.parseUseSplitsRanges("0-2");
        Assert.assertTrue(extractor.useSplit(useSplitRanges, 0));
        Assert.assertTrue(extractor.useSplit(useSplitRanges, 1));
        Assert.assertTrue(extractor.useSplit(useSplitRanges, 2));
        Assert.assertFalse(extractor.useSplit(useSplitRanges, 3));
    }
    
    @Test
    public void rangeTest2() {
        SplitSelectorExtractor extractor = new SplitSelectorExtractor();
        List<IntRange> useSplitRanges = extractor.parseUseSplitsRanges("0-2,4");
        Assert.assertTrue(extractor.useSplit(useSplitRanges, 2));
        Assert.assertFalse(extractor.useSplit(useSplitRanges, 3));
        Assert.assertTrue(extractor.useSplit(useSplitRanges, 4));
    }
    
    @Test
    public void rangeTest3() {
        SplitSelectorExtractor extractor = new SplitSelectorExtractor();
        List<IntRange> useSplitRanges = extractor.parseUseSplitsRanges("2,4");
        Assert.assertTrue(extractor.useSplit(useSplitRanges, 2));
        Assert.assertFalse(extractor.useSplit(useSplitRanges, 3));
        Assert.assertTrue(extractor.useSplit(useSplitRanges, 4));
    }
    
    @Test
    public void rangeTest4() {
        SplitSelectorExtractor extractor = new SplitSelectorExtractor();
        List<IntRange> useSplitRanges = extractor.parseUseSplitsRanges("2,4,6-");
        Assert.assertTrue(extractor.useSplit(useSplitRanges, 2));
        Assert.assertFalse(extractor.useSplit(useSplitRanges, 3));
        Assert.assertTrue(extractor.useSplit(useSplitRanges, 4));
        Assert.assertFalse(extractor.useSplit(useSplitRanges, 5));
        Assert.assertTrue(extractor.useSplit(useSplitRanges, 6));
        Assert.assertTrue(extractor.useSplit(useSplitRanges, 100));
        Assert.assertTrue(extractor.useSplit(useSplitRanges, 1000));
    }
    
    @Test
    public void rangeTest5() {
        SplitSelectorExtractor extractor = new SplitSelectorExtractor();
        List<IntRange> useSplitRanges = extractor.parseUseSplitsRanges(" 2, 4 , 6- ");
        Assert.assertTrue(extractor.useSplit(useSplitRanges, 2));
        Assert.assertFalse(extractor.useSplit(useSplitRanges, 3));
        Assert.assertTrue(extractor.useSplit(useSplitRanges, 4));
        Assert.assertFalse(extractor.useSplit(useSplitRanges, 5));
        Assert.assertTrue(extractor.useSplit(useSplitRanges, 6));
        Assert.assertTrue(extractor.useSplit(useSplitRanges, 100));
        Assert.assertTrue(extractor.useSplit(useSplitRanges, 1000));
    }
    
    @Test
    public void rangeTest6() {
        SplitSelectorExtractor extractor = new SplitSelectorExtractor();
        List<IntRange> useSplitRanges = extractor.parseUseSplitsRanges("0");
        Assert.assertTrue(extractor.useSplit(useSplitRanges, 0));
        Assert.assertFalse(extractor.useSplit(useSplitRanges, 1));
    }
}
