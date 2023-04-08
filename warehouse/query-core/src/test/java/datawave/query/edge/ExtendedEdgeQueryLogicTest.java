package datawave.query.edge;

import datawave.core.iterators.ColumnRangeIterator;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.QueryLogic;
import datawave.query.tables.edge.EdgeQueryFunctionalTest;
import datawave.query.tables.edge.EdgeQueryLogic;
import datawave.webservice.query.QueryImpl;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

public class ExtendedEdgeQueryLogicTest extends EdgeQueryFunctionalTest {
    
    @Override
    public QueryLogic<?> createLogic() throws Exception {
        return factory.getQueryLogic("ExtendedEdgeQuery");
    }
    
    @Override
    public DefaultExtendedEdgeQueryLogic runLogic(QueryImpl q, Set<Authorizations> auths) throws Exception {
        return runLogic(q, auths, Long.MAX_VALUE);
    }
    
    public DefaultExtendedEdgeQueryLogic runLogic(QueryImpl q, Set<Authorizations> auths, long scanLimit) throws Exception {
        logic.setDateFilterScanLimit(scanLimit);
        GenericQueryConfiguration config = logic.initialize(client, q, auths);
        logic.setupQuery(config);
        return (DefaultExtendedEdgeQueryLogic) logic;
    }
    
    @Test
    public void testEdgeQuerySyntax() throws Exception {
        QueryImpl q = configQuery("(SOURCE =~ 'M.*') && (SINK == 'JUPITER') && (RELATION == 'FROM-TO' || RELATION == 'TO-FROM')", auths);
        q.addParameter("stats", "true");
        DefaultExtendedEdgeQueryLogic logic = runLogic(q, auths);
        
        List<String> expected = new ArrayList<>();
        
        expected.add("mars%00;jupiter AdjacentPlanets/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("mars STATS/ACTIVITY/Planets/TO:20150713/COSMOS_DATA [B]");
        expected.add("mercury STATS/ACTIVITY/Planets/TO:20150713/COSMOS_DATA [B]");
        
        compareResults(logic, factory, expected);
    }
    
    @Test
    public void testEdgeQuerySyntax_WithQueryModel() throws Exception {
        QueryImpl q = configQuery("(VERTEXA =~ 'M.*') && (VERTEXB == 'JUPITER') && (RELATION == 'FROM-TO' || RELATION == 'TO-FROM')", auths);
        q.addParameter("stats", "true");
        q.addParameter("query.syntax", "JEXL");
        DefaultExtendedEdgeQueryLogic logic = runLogic(q, auths);
        
        List<String> expected = new ArrayList<>();
        
        expected.add("mars%00;jupiter AdjacentPlanets/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("mars STATS/ACTIVITY/Planets/TO:20150713/COSMOS_DATA [B]");
        expected.add("mercury STATS/ACTIVITY/Planets/TO:20150713/COSMOS_DATA [B]");
        
        compareResults(logic, factory, expected);
    }
    
    @Test
    public void testEdgeQuerySyntaxLuceneWithQueryModel() throws Exception {
        QueryImpl q = configQuery("(VERTEXA:M*) AND (VERTEXB:JUPITER) AND (RELATION:FROM-TO OR RELATION:TO-FROM)", auths);
        q.addParameter("stats", "true");
        q.addParameter("query.syntax", "LUCENE");
        DefaultExtendedEdgeQueryLogic logic = runLogic(q, auths);
        
        List<String> expected = new ArrayList<>();
        
        expected.add("mars%00;jupiter AdjacentPlanets/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("mars STATS/ACTIVITY/Planets/TO:20150713/COSMOS_DATA [B]");
        expected.add("mercury STATS/ACTIVITY/Planets/TO:20150713/COSMOS_DATA [B]");
        
        compareResults(logic, factory, expected);
    }
    
    @Test(expected = UnsupportedOperationException.class)
    public void testUnknownFunction() throws Exception {
        
        QueryImpl q = configQuery("SOURCE == 'SUN' && (filter:includeregex(SINK, 'earth|mars'))", auths);
        
        EdgeQueryLogic logic = runLogic(q, auths);
        
        List<String> expected = new ArrayList<>();
        
        compareResults(logic, factory, expected);
    }
    
    @Test
    public void testEdgeSummaryQuerySyntax() throws Exception {
        QueryImpl q = configQuery("mars", auths);
        q.addParameter("stats", "true");
        q.addParameter("query.syntax", "LIST");
        DefaultExtendedEdgeQueryLogic logic = runLogic(q, auths);
        
        List<String> expected = new ArrayList<>();
        
        expected.add("mars%00;earth AdjacentPlanets/TO-FROM:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("mars%00;jupiter AdjacentPlanets/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("mars%00;asteroid_belt AdjacentCelestialBodies/TO-FROM:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("mars%00;ceres AdjacentCelestialBodies/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [B]");
        expected.add("mars%00;ceres AdjacentDwarfPlanets/TO-FROM:20150713/COSMOS_DATA-COSMOS_DATA [B]");
        expected.add("mars STATS/ACTIVITY/Planets/TO:20150713/COSMOS_DATA [B]");
        
        compareResults(logic, factory, expected);
    }
    
    @Test
    public void testEdgeSummaryQueryOutput() throws Exception {
        QueryImpl q = configQuery("(SOURCE =~ 'M.*') && (SINK == 'JUPITER') && (RELATION == 'FROM-TO')", auths);
        q.addParameter("stats", "true");
        q.addParameter("summarize", "true");
        DefaultExtendedEdgeQueryLogic logic = runLogic(q, auths);
        
        int counter = 0;
        Iterator<Entry<Key,Value>> ita = logic.iterator();
        while (ita.hasNext()) {
            ita.next();
            counter++;
        }
        Assert.assertTrue(counter > 0);
    }
    
    @Test
    public void testEdgeSummaryQueryOutput_WithQueryModel() throws Exception {
        QueryImpl q = configQuery("(VERTEXA =~ 'M.*') && (VERTEXB == 'JUPITER') && (RELATION == 'FROM-TO')", auths);
        q.addParameter("stats", "true");
        q.addParameter("summarize", "true");
        DefaultExtendedEdgeQueryLogic logic = runLogic(q, auths);
        
        int counter = 0;
        Iterator<Entry<Key,Value>> ita = logic.iterator();
        while (ita.hasNext()) {
            ita.next();
            counter++;
        }
        Assert.assertTrue(counter > 0);
    }
    
    /**
     * Tests to make sure QueryModel is applied properly to a query string
     * 
     * @throws Exception
     *             if there are issues
     */
    @Test
    public void testQueryModelApplied() throws Exception {
        String originalQueryString = "(VERTEXA == 'MARS' && VERTEXB == 'JUPITER' && ATTR1 == 'something' && ((TYPE == 'COSMOS_DATA' && RELATION == 'FROM-TO') || "
                        + "(TYPE == 'COSMOS_DATA' && RELATION == 'TO-FROM')))";
        
        String expectedQueryString = "(SOURCE == 'MARS' && SINK == 'JUPITER' && ATTRIBUTE1 == 'something' && ((TYPE == 'COSMOS_DATA' && RELATION == 'FROM-TO') || "
                        + "(TYPE == 'COSMOS_DATA' && RELATION == 'TO-FROM')))";
        
        QueryImpl q = configQuery(originalQueryString, auths);
        GenericQueryConfiguration config = logic.initialize(client, q, auths);
        logic.setupQuery(config);
        String actualQueryString = config.getQueryString();
        
        Assert.assertEquals(expectedQueryString, actualQueryString);
    }
    
    @Test
    public void testSelectorExtractor() throws Exception {
        QueryImpl q = configQuery("MARS,JUPITER,VENUS", auths);
        q.addParameter("delimiter", ",");
        q.addParameter("query.syntax", "LIST");
        
        List<String> expected = new ArrayList();
        expected.add("MARS");
        expected.add("JUPITER");
        expected.add("VENUS");
        
        List<String> sources = logic.getSelectors(q);
        
        Assert.assertTrue(sources.containsAll(expected));
        
        q = configQuery("SOURCE == 'MARS' OR SOURCE == 'JUPITER' OR SOURCE == 'VENUS'", auths);
        q.addParameter("query.syntax", "JEXL");
        
        sources = logic.getSelectors(q);
        
        Assert.assertTrue(sources.containsAll(expected));
    }
    
    @Test
    public void testEdgeQueryWithScanLimit() throws Exception {
        QueryImpl q = configQuery("(SOURCE =~ 'M.*') && (SINK == 'JUPITER') && (RELATION == 'FROM-TO' || RELATION == 'TO-FROM')", auths);
        q.addParameter("stats", "true");
        DefaultExtendedEdgeQueryLogic logic = runLogic(q, auths, 2);
        
        List<String> expected = new ArrayList<>();
        
        expected.add("mars%00;jupiter AdjacentPlanets/FROM-TO:20150713/COSMOS_DATA-COSMOS_DATA [A]");
        expected.add("mars STATS/ACTIVITY/Planets/TO:20150713/COSMOS_DATA [B]");
        expected.add("mercury STATS/ACTIVITY/Planets/TO:20150713/COSMOS_DATA [B]");
        
        compareResults(logic, factory, expected);
        
        try {
            logic = runLogic(q, auths, 1);
            compareResults(logic, factory, expected);
            Assert.fail("Expected to fail because the scan limit was reached");
        } catch (ColumnRangeIterator.ScanLimitReached e) {
            // expected
        }
    }
    
}
