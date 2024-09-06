package datawave.audit;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.Test;

import com.google.common.collect.Lists;

import datawave.microservice.query.QueryImpl;

public class DatawaveSelectorExtractorTest {

    @Test
    public void extractSelectorsLuceneQuery1() {

        DatawaveSelectorExtractor extractor = new DatawaveSelectorExtractor();
        QueryImpl q = new QueryImpl();
        q.setQuery("FIELD1:selector1");
        List<String> selectorList = extractor.extractSelectors(q);
        List<String> expected = Lists.newArrayList("selector1");
        assertEquals(expected, selectorList);
    }

    @Test
    public void extractSelectorsLuceneQuery2() {

        DatawaveSelectorExtractor extractor = new DatawaveSelectorExtractor();
        QueryImpl q = new QueryImpl();
        q.setQuery("FIELD1:selector1 AND selector2 AND selector3");
        List<String> selectorList = extractor.extractSelectors(q);
        List<String> expected = Lists.newArrayList("selector1", "selector2", "selector3");
        assertEquals(expected, selectorList);
    }

    @Test
    public void extractSelectorsLuceneQuery3() {

        DatawaveSelectorExtractor extractor = new DatawaveSelectorExtractor();
        QueryImpl q = new QueryImpl();
        q.setQuery("FIELD1:selector1 OR selector2 OR (selector3 AND selector4)");
        List<String> selectorList = extractor.extractSelectors(q);
        List<String> expected = Lists.newArrayList("selector1", "selector2", "selector3", "selector4");
        assertEquals(expected, selectorList);
    }

    @Test
    public void extractSelectorsNegation() {

        DatawaveSelectorExtractor extractor = new DatawaveSelectorExtractor();
        QueryImpl q = new QueryImpl();
        q.setQuery("FIELD1:selector1 NOT selector2");
        List<String> selectorList = extractor.extractSelectors(q);
        List<String> expected = Lists.newArrayList("selector1");
        assertEquals(expected, selectorList);
    }

    @Test
    public void extractSelectorsDoubleNegation() {

        DatawaveSelectorExtractor extractor = new DatawaveSelectorExtractor();
        QueryImpl q = new QueryImpl();
        q.setQuery("FIELD1:selector1 NOT (selector2 NOT selector3)");
        List<String> selectorList = extractor.extractSelectors(q);
        List<String> expected = Lists.newArrayList("selector1", "selector3");
        assertEquals(expected, selectorList);
    }

    @Test
    public void extractSelectorsTripleNegation() {

        DatawaveSelectorExtractor extractor = new DatawaveSelectorExtractor();
        QueryImpl q = new QueryImpl();
        q.setQuery("FIELD1:selector1 NOT (selector2 NOT (selector3 NOT selector4))");
        List<String> selectorList = extractor.extractSelectors(q);
        List<String> expected = Lists.newArrayList("selector1", "selector3");
        assertEquals(expected, selectorList);
    }

    @Test
    public void extractSelectorsWildcard() {

        DatawaveSelectorExtractor extractor = new DatawaveSelectorExtractor();
        QueryImpl q = new QueryImpl();
        q.setQuery("FIELD1:selector1 AND selector.*");
        List<String> selectorList = extractor.extractSelectors(q);
        List<String> expected = Lists.newArrayList("selector1");
        assertEquals(expected, selectorList);
    }

    @Test
    public void extractSelectorsJEXLQuery1() {

        DatawaveSelectorExtractor extractor = new DatawaveSelectorExtractor();
        QueryImpl q = new QueryImpl();
        q.setQuery("FIELD1 == 'selector1'");
        List<String> selectorList = extractor.extractSelectors(q);
        List<String> expected = Lists.newArrayList("selector1");
        assertEquals(expected, selectorList);
    }

    @Test
    public void extractSelectorsJEXLQuery2() {

        DatawaveSelectorExtractor extractor = new DatawaveSelectorExtractor();
        QueryImpl q = new QueryImpl();
        q.setQuery("FIELD1 == 'selector1' && _ANY_FIELD_ == 'selector2' && _ANY_FIELD_ == 'selector3'");
        List<String> selectorList = extractor.extractSelectors(q);
        List<String> expected = Lists.newArrayList("selector1", "selector2", "selector3");
        assertEquals(expected, selectorList);
    }

    @Test
    public void extractSelectorsJEXLQuery3() {

        DatawaveSelectorExtractor extractor = new DatawaveSelectorExtractor();
        QueryImpl q = new QueryImpl();
        q.setQuery("FIELD1 == 'selector1' || _ANY_FIELD_ == 'selector2' || (_ANY_FIELD_ == 'selector3' && _ANY_FIELD_ == 'selector4')");
        List<String> selectorList = extractor.extractSelectors(q);
        List<String> expected = Lists.newArrayList("selector1", "selector2", "selector3", "selector4");
        assertEquals(expected, selectorList);
    }

    @Test
    public void extract10kSelectors() {

        List<String> uuids = new ArrayList<>();
        for (int i = 0; i < 10000; i++)
            uuids.add("_ANY_FIELD_ == '" + UUID.randomUUID().toString() + "'");

        String query = String.join(" || ", uuids);

        DatawaveSelectorExtractor extractor = new DatawaveSelectorExtractor();
        QueryImpl q = new QueryImpl();
        q.setQuery(query);
        List<String> selectorList = extractor.extractSelectors(q);
        List<String> expected = Lists
                        .newArrayList(uuids.stream().map(x -> x.substring("_ANY_FIELD_ == '".length(), x.length() - 1)).collect(Collectors.toList()));
        assertEquals(expected, selectorList);
    }

    @Test
    public void extractSelectorsUnknownFunction() {

        // expect that the exception will be caught and an empty list returned
        DatawaveSelectorExtractor extractor = new DatawaveSelectorExtractor();
        QueryImpl q = new QueryImpl();
        q.setQuery("FIELD1:selector1 AND #UNKNOWN(parameter)");
        List<String> selectorList = extractor.extractSelectors(q);
        assertEquals(0, selectorList.size());
    }
}
