package datawave.audit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.util.List;

import org.apache.commons.lang.math.IntRange;
import org.junit.Test;

import com.google.common.collect.Lists;

import datawave.microservice.query.QueryImpl;

public class SplitSelectorExtractorTest {

    @Test
    public void extractSelectorsLuceneQuery1() {

        SplitSelectorExtractor extractor = new SplitSelectorExtractor();
        QueryImpl q = new QueryImpl();
        q.setQuery("selector1");
        List<String> selectorList = extractor.extractSelectors(q);
        List<String> expected = Lists.newArrayList("selector1");
        assertEquals(expected, selectorList);
    }

    @Test
    public void extractSelectorsLuceneQuery2() {

        SplitSelectorExtractor extractor = new SplitSelectorExtractor();
        extractor.setSeparatorCharacter(";");
        QueryImpl q = new QueryImpl();
        q.setQuery("selector1;selector2;selector3");
        List<String> selectorList = extractor.extractSelectors(q);
        List<String> expected = Lists.newArrayList("selector1", "selector2", "selector3");
        assertEquals(expected, selectorList);
    }

    @Test
    public void extractSelectorsLuceneQuery3() {

        SplitSelectorExtractor extractor = new SplitSelectorExtractor();
        extractor.setSeparatorCharacter("\0");
        QueryImpl q = new QueryImpl();
        q.setQuery("selector1\0selector2\0selector3");
        List<String> selectorList = extractor.extractSelectors(q);
        List<String> expected = Lists.newArrayList("selector1", "selector2", "selector3");
        assertEquals(expected, selectorList);
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
        assertEquals(expected, selectorList);
    }

    @Test
    public void rangeTest1() {
        SplitSelectorExtractor extractor = new SplitSelectorExtractor();
        List<IntRange> useSplitRanges = extractor.parseUseSplitsRanges("0");
        //  @formatter:off
        assertAll(
                        () -> {assertTrue(extractor.useSplit(useSplitRanges, 0));},
                        () -> {assertFalse(extractor.useSplit(useSplitRanges, 1));}
        );
        //  @formatter:on
    }

    @Test
    public void rangeTest2() {
        SplitSelectorExtractor extractor = new SplitSelectorExtractor();
        List<IntRange> useSplitRanges = extractor.parseUseSplitsRanges("0-2");
        //  @formatter:off
        assertAll(
                () -> {assertTrue(extractor.useSplit(useSplitRanges, 0));},
                () -> {assertTrue(extractor.useSplit(useSplitRanges, 1));},
                () -> {assertTrue(extractor.useSplit(useSplitRanges, 2));},
                () -> {assertFalse(extractor.useSplit(useSplitRanges, 3));}
        );
        //  @formatter:on
    }

    @Test
    public void rangeTest3() {
        SplitSelectorExtractor extractor = new SplitSelectorExtractor();
        List<IntRange> useSplitRanges = extractor.parseUseSplitsRanges("0-2,4");
        //  @formatter:off
        assertAll(
                        () -> {assertTrue(extractor.useSplit(useSplitRanges, 2));},
                        () -> {assertFalse(extractor.useSplit(useSplitRanges, 3));},
                        () -> {assertTrue(extractor.useSplit(useSplitRanges, 4));}
        );
        //  @formatter:on
    }

    @Test
    public void rangeTest4() {
        SplitSelectorExtractor extractor = new SplitSelectorExtractor();
        List<IntRange> useSplitRanges = extractor.parseUseSplitsRanges("2,4");
        //  @formatter:off
        assertAll(
                        () -> {assertTrue(extractor.useSplit(useSplitRanges, 2));},
                        () -> {assertFalse(extractor.useSplit(useSplitRanges, 3));},
                        () -> {assertTrue(extractor.useSplit(useSplitRanges, 4));}
        );
        //  @formatter:on
    }

    @Test
    public void rangeTest5() {
        SplitSelectorExtractor extractor = new SplitSelectorExtractor();
        List<IntRange> useSplitRanges = extractor.parseUseSplitsRanges("2,4,6-");
        //  @formatter:off
        assertAll(//
                        () -> {assertTrue(extractor.useSplit(useSplitRanges, 2));},
                        () -> {assertFalse(extractor.useSplit(useSplitRanges, 3));},
                        () -> {assertTrue(extractor.useSplit(useSplitRanges, 4));},
                        () -> {assertFalse(extractor.useSplit(useSplitRanges, 5));},
                        () -> {assertTrue(extractor.useSplit(useSplitRanges, 6));},
                        () -> {assertTrue(extractor.useSplit(useSplitRanges, 100));},
                        () -> {assertTrue(extractor.useSplit(useSplitRanges, 1000));}
        );
        //  @formatter:on
    }

    @Test
    public void rangeTest6() {
        SplitSelectorExtractor extractor = new SplitSelectorExtractor();
        List<IntRange> useSplitRanges = extractor.parseUseSplitsRanges(" 2, 4 , 6- ");
        //  @formatter:off
        assertAll(//
                () -> {assertTrue(extractor.useSplit(useSplitRanges, 2));},
                () -> {assertFalse(extractor.useSplit(useSplitRanges, 3));},
                () -> {assertTrue(extractor.useSplit(useSplitRanges, 4));},
                () -> {assertFalse(extractor.useSplit(useSplitRanges, 5));},
                () -> {assertTrue(extractor.useSplit(useSplitRanges, 6));},
                () -> {assertTrue(extractor.useSplit(useSplitRanges, 100));},
                () -> {assertTrue(extractor.useSplit(useSplitRanges, 1000));}
        );
        //  @formatter:on
    }
}
